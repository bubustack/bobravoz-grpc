package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bubustack/bobravoz-grpc/internal/telemetry"
	"github.com/bubustack/bobravoz-grpc/pkg/metrics"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportconnector "github.com/bubustack/core/runtime/transport/connector"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	metaStoryRunName        = "storyrun-name"
	metaStoryRunNS          = "storyrun-namespace"
	metaCurrentStepID       = "current-step-id"
	metaConnectorGeneration = "connector-generation"

	metadataEnvelopeKindKey      = "bubu.envelope.kind"
	metadataEnvelopeMessageIDKey = "bubu.envelope.message_id"
	metadataEnvelopeTimeKey      = "bubu.envelope.timestamp_ms"

	handoffDirectiveDraining    = "handoff.draining"
	handoffDirectiveCutover     = "handoff.cutover"
	handoffDirectiveReady       = "handoff.ready"
	downstreamDeliveryDirective = "downstream.delivered"
	deliveryReceiptStreamIDKey  = "stream_id"
	deliveryReceiptSequenceKey  = "sequence"
	deliveryReceiptPartitionKey = "partition"
	deliveryReceiptSizeBytesKey = "size_bytes"
)

type handoffState struct {
	phase     string
	reason    string
	updatedAt time.Time
}

type runtimeTunables = transportconnector.RuntimeTunables

// Runner hosts the TransportConnectorService gRPC server and bridges frames to the hub.
type Runner struct {
	cfg *Config
	log logr.Logger
}

// NewRunner constructs a Runner.
func NewRunner(cfg *Config, log logr.Logger) *Runner {
	return &Runner{cfg: cfg, log: log}
}

// Run starts the gRPC server and blocks until the context is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	// For P2P support, we need to listen on 0.0.0.0 instead of 127.0.0.1
	// so upstream connectors can reach us through the Kubernetes service.
	// The default LocalEndpoint is "127.0.0.1:50051", change to "0.0.0.0:50051"
	//
	// SECURITY TODO: Split into two listeners — bind HubService (control plane)
	// to localhost only, and TransportConnectorService (P2P data plane) to
	// 0.0.0.0. This reduces the attack surface by preventing arbitrary pods
	// from sending control directives. See audit finding #5 (CRITICAL).
	listenEndpoint := strings.Replace(r.cfg.LocalEndpoint, "127.0.0.1", "0.0.0.0", 1)

	listener, err := transportconnector.ListenLocalEndpoint(listenEndpoint)
	if err != nil {
		return err
	}
	defer func() {
		if err := listener.Close(); err != nil {
			r.log.Error(err, "failed to close connector listener")
		}
	}()

	tunables := transportconnector.RuntimeTunablesFromEnv(
		transportconnector.OSEnv,
		transportconnector.RuntimeTunables{},
	)

	bridge, err := newHubBridge(ctx, r.cfg, r.log, tunables)
	if err != nil {
		return err
	}
	defer bridge.Close()

	opts := []grpc.ServerOption{}
	if telemetry.TracePropagationEnabled() {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}
	opts = append(opts, transportconnector.ServerOptions(
		transportconnector.OSEnv,
		transportconnector.DefaultMaxMessageSize,
		transportconnector.DefaultMaxMessageSize,
	)...)
	server := grpc.NewServer(opts...)

	// Register TransportConnectorServiceServer for local engram connections
	transportServer := newTransportServer(ctx, r.cfg, r.log, bridge, tunables)
	transportpb.RegisterTransportConnectorServiceServer(server, transportServer)
	bridge.notifyHandoff("cutover", "hub_connected")

	// Register HubService for P2P connections from upstream connectors
	// This allows this connector to act as a "mini-hub" for P2P routing
	p2pSvc := newP2PServer(r.log, bridge)
	transportServer.p2p = p2pSvc
	transportpb.RegisterHubServiceServer(server, p2pSvc)
	r.log.Info("P2P server registered, ready to accept upstream connections")

	errCh := make(chan error, 1)
	go func() {
		r.log.Info("Transport connector listening", "endpoint", listenEndpoint)
		errCh <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		r.log.Info("Shutting down transport connector")
		// Allow in-flight messages to complete before shutdown.
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer drainCancel()
		drained := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(drained)
		}()
		select {
		case <-drained:
		case <-drainCtx.Done():
			r.log.Info("Drain period expired, forcing stop")
			server.Stop()
		}
		return nil
	case err := <-errCh:
		return err
	}
}

type transportServer struct {
	transportpb.UnimplementedTransportConnectorServiceServer
	ctx            context.Context
	cfg            *Config
	log            logr.Logger
	bridge         *hubBridge
	reporter       *bindingStatusReporter
	gate           *mediaGate
	tunables       runtimeTunables
	handoffMu      sync.Mutex
	handoffState   handoffState
	handoffUpdates chan *transportpb.ControlResponse
	watchersMu     sync.Mutex
	watchers       map[*hangWatcher]struct{}
	p2p            *p2pServer
}

//nolint:logcheck // Constructors keep explicit logger injection separate from the long-lived server context.
func newTransportServer(ctx context.Context, cfg *Config, log logr.Logger, bridge *hubBridge, tunables runtimeTunables) *transportServer {
	server := &transportServer{
		ctx:            ctx,
		cfg:            cfg,
		log:            log,
		bridge:         bridge,
		reporter:       newBindingStatusReporter(cfg, log),
		gate:           newMediaGate(),
		tunables:       tunables,
		handoffUpdates: make(chan *transportpb.ControlResponse, 8),
		watchers:       make(map[*hangWatcher]struct{}),
	}
	if server.reporter != nil {
		server.reporter.heartbeatHook = server.touchWatchers
	}
	if bridge != nil {
		bridge.onFlow = server.applyFlowControl
		bridge.onHandoff = server.signalHandoff
	}
	return server
}

func (s *transportServer) registerWatcher(w *hangWatcher) func() {
	if s == nil || w == nil {
		return func() {}
	}
	s.watchersMu.Lock()
	s.watchers[w] = struct{}{}
	s.watchersMu.Unlock()
	return func() {
		s.watchersMu.Lock()
		delete(s.watchers, w)
		s.watchersMu.Unlock()
	}
}

func (s *transportServer) touchWatchers() {
	if s == nil {
		return
	}
	s.watchersMu.Lock()
	watchers := make([]*hangWatcher, 0, len(s.watchers))
	for w := range s.watchers {
		watchers = append(watchers, w)
	}
	s.watchersMu.Unlock()
	for _, w := range watchers {
		w.Touch()
	}
	if s.bridge != nil {
		s.bridge.touch()
	}
}

func (s *transportServer) bindingLabels() (string, string) {
	if s == nil || s.cfg == nil {
		return "", ""
	}
	return s.cfg.Binding.Reference.Namespace, s.cfg.Binding.Reference.Name
}

func (s *transportServer) recordDownstreamDrop(reason string) {
	ns, binding := s.bindingLabels()
	if ns == "" && binding == "" {
		return
	}
	metrics.RecordConnectorDownstreamDrop(ns, binding, reason)
}

func (s *transportServer) recordControlDirective(direction, directiveType string) {
	ns, binding := s.bindingLabels()
	if ns == "" && binding == "" {
		return
	}
	metrics.RecordConnectorControlDirective(ns, binding, direction, normalizeDirectiveType(directiveType))
}

func (s *transportServer) signalHandoff(phase, reason string) {
	if s == nil || strings.TrimSpace(phase) == "" {
		return
	}
	state := handoffState{
		phase:     strings.ToLower(strings.TrimSpace(phase)),
		reason:    strings.TrimSpace(reason),
		updatedAt: time.Now(),
	}
	s.handoffMu.Lock()
	s.handoffState = state
	s.handoffMu.Unlock()
	s.enqueueHandoff(state)
}

func (s *transportServer) enqueueHandoff(state handoffState) {
	if s == nil {
		return
	}
	directive := handoffDirective(state)
	if directive == nil {
		return
	}
	select {
	case s.handoffUpdates <- directive:
	default:
		s.log.V(1).Info("Dropping handoff directive; channel full", "phase", state.phase)
	}
}

func (s *transportServer) emitHandoffSnapshot() {
	if s == nil {
		return
	}
	s.handoffMu.Lock()
	state := s.handoffState
	s.handoffMu.Unlock()
	if state.phase == "" {
		return
	}
	s.enqueueHandoff(state)
}

func handoffDirective(state handoffState) *transportpb.ControlResponse {
	phase := strings.ToLower(strings.TrimSpace(state.phase))
	var action transportpb.ControlAction
	switch phase {
	case "draining":
		action = transportpb.ControlAction_CONTROL_ACTION_HANDOFF_DRAINING
	case "cutover":
		action = transportpb.ControlAction_CONTROL_ACTION_HANDOFF_CUTOVER
	case "ready":
		action = transportpb.ControlAction_CONTROL_ACTION_HANDOFF_READY
	default:
		return nil
	}
	md := map[string]string{
		"phase": phase,
	}
	if state.reason != "" {
		md["reason"] = state.reason
	}
	if !state.updatedAt.IsZero() {
		md["ts"] = strconv.FormatInt(state.updatedAt.UnixMilli(), 10)
	}
	return &transportpb.ControlResponse{Action: action, Metadata: md}
}

func (s *transportServer) reportReady(_ context.Context) {
	if s.reporter == nil {
		return
	}
	s.reporter.Start(s.ctx)
	s.reporter.ReportReady(s.ctx)
}

func (s *transportServer) startupCapabilitiesMode() string {
	if s == nil || s.reporter == nil {
		return coretransport.StartupCapabilitiesNone
	}
	if s.reporter.hasCurrentCapabilities() {
		return coretransport.StartupCapabilitiesRequired
	}
	return coretransport.StartupCapabilitiesNone
}

func startupCapabilitiesModeFromBindingInfo(info *transportpb.BindingInfo) string {
	if info == nil {
		return coretransport.StartupCapabilitiesNone
	}
	for _, codec := range info.GetAudioCodecs() {
		if strings.TrimSpace(codec) != "" {
			return coretransport.StartupCapabilitiesRequired
		}
	}
	for _, codec := range info.GetVideoCodecs() {
		if strings.TrimSpace(codec) != "" {
			return coretransport.StartupCapabilitiesRequired
		}
	}
	for _, mime := range info.GetBinaryTypes() {
		if strings.TrimSpace(mime) != "" {
			return coretransport.StartupCapabilitiesRequired
		}
	}
	return coretransport.StartupCapabilitiesNone
}

// Control establishes the connector control stream: it reports readiness,
// sends an initial connector.ready directive, then concurrently forwards
// capability updates and consumes directives with transportconnector.CallWithTimeout guarding
// every send/recv until the context is canceled
// (`internal/connector/connector.go:295-369`).
func (s *transportServer) Control(stream transportpb.TransportConnectorService_ControlServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.FailedPrecondition, "control stream missing metadata")
	}
	if err := coretransport.ValidateProtocolVersion(metadataValue(md, coretransport.ProtocolMetadataKey)); err != nil {
		s.log.Error(err, "Control stream rejected due to protocol version")
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	var watcher *hangWatcher
	if s.tunables.HangTimeout > 0 {
		watcher = newHangWatcher(s.tunables.HangTimeout, cancel, s.log.WithName("control-hang"))
		defer watcher.Stop()
	}
	unregisterWatcher := s.registerWatcher(watcher)
	defer unregisterWatcher()
	s.reportReady(ctx)
	if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, "control ready send", func(context.Context) error {
		return stream.Send(connectorReadyDirective(s.startupCapabilitiesMode()))
	}); err != nil {
		return err
	}

	updates := s.capabilityUpdates(ctx)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return s.forwardCapabilityUpdates(groupCtx, stream, updates)
	})
	group.Go(func() error {
		return s.forwardHandoffUpdates(groupCtx, stream, watcher)
	})
	group.Go(func() error {
		return s.consumeControlDirectives(groupCtx, stream, watcher)
	})
	s.emitHandoffSnapshot()

	if err := group.Wait(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

// Data implements the bidirectional data stream between the connector (engram)
// and the hub. Upstream packets (engram→hub) are received from the stream and
// forwarded via the hub bridge. Downstream packets (hub→engram) are read from
// the bridge receive channel and sent back on the stream.
func (s *transportServer) Data(stream transportpb.TransportConnectorService_DataServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.FailedPrecondition, "data stream missing metadata")
	}
	if err := coretransport.ValidateProtocolVersion(metadataValue(md, coretransport.ProtocolMetadataKey)); err != nil {
		s.log.Error(err, "Data stream rejected due to protocol version")
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	var watcher *hangWatcher
	if s.tunables.HangTimeout > 0 {
		watcher = newHangWatcher(s.tunables.HangTimeout, cancel, s.log.WithName("data-hang"))
		defer watcher.Stop()
	}
	unregisterWatcher := s.registerWatcher(watcher)
	defer unregisterWatcher()
	s.reportReady(ctx)

	group, groupCtx := errgroup.WithContext(ctx)

	// Upstream: engram → hub
	group.Go(func() error {
		return s.dataRecvLoop(groupCtx, stream, watcher)
	})

	// Downstream: hub → engram
	group.Go(func() error {
		return s.dataSendLoop(groupCtx, stream, watcher)
	})

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// dataRecvLoop reads DataRequest messages from the engram, converts them to
// DataPacket, and forwards them upstream to the hub via the bridge.
func (s *transportServer) dataRecvLoop(ctx context.Context, stream transportpb.TransportConnectorService_DataServer, watcher *hangWatcher) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				return nil
			}
			return err
		}
		if watcher != nil {
			watcher.Touch()
		}

		if !s.gate.AllowUpstream() {
			continue
		}

		packet := dataRequestToPacket(req)
		if packet == nil {
			continue
		}
		if err := s.bridge.Send(packet); err != nil {
			s.log.V(1).Info("Failed to send upstream packet to hub", "error", err)
		}
	}
}

// dataSendLoop reads DataPacket messages from the hub bridge and sends them
// downstream to the engram as DataResponse messages.
func (s *transportServer) dataSendLoop(ctx context.Context, stream transportpb.TransportConnectorService_DataServer, watcher *hangWatcher) error {
	recvCh := s.bridge.Recv()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case packet, ok := <-recvCh:
			if !ok {
				return nil
			}
			if !s.gate.AllowDownstream() {
				s.recordDownstreamDrop("gated")
				continue
			}

			resp := packetToDataResponse(packet)
			if resp == nil {
				continue
			}
			if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, "data send", func(context.Context) error {
				return stream.Send(resp)
			}); err != nil {
				return err
			}
			if shouldAcknowledgeDownstreamImmediately(packet) {
				s.bridge.RecordDelivery(packet)
			}
			if watcher != nil {
				watcher.Touch()
			}
		}
	}
}

func shouldAcknowledgeDownstreamImmediately(packet *transportpb.DataPacket) bool {
	return packet == nil || packet.GetEnvelope().GetSequence() == 0
}

// dataRequestToPacket converts a DataRequest (from the engram) to a DataPacket
// (hub internal format). The frame oneof is mapped to the flat DataPacket fields.
func dataRequestToPacket(req *transportpb.DataRequest) *transportpb.DataPacket {
	if req == nil {
		return nil
	}
	packet := &transportpb.DataPacket{
		Metadata:   req.GetMetadata(),
		Payload:    req.GetPayload(),
		Inputs:     req.GetInputs(),
		Transports: req.GetTransports(),
		Envelope:   req.GetEnvelope(),
	}
	switch f := req.GetFrame().(type) {
	case *transportpb.DataRequest_Audio:
		packet.Frame = &transportpb.DataPacket_Audio{Audio: f.Audio}
	case *transportpb.DataRequest_Video:
		packet.Frame = &transportpb.DataPacket_Video{Video: f.Video}
	case *transportpb.DataRequest_Binary:
		packet.Frame = &transportpb.DataPacket_Binary{Binary: f.Binary}
	}
	return packet
}

// packetToDataResponse converts a DataPacket (hub internal format) to a
// DataResponse (sent to the engram). The flat DataPacket frame fields are
// mapped to the DataResponse oneof.
func packetToDataResponse(packet *transportpb.DataPacket) *transportpb.DataResponse {
	if packet == nil {
		return nil
	}
	resp := &transportpb.DataResponse{
		Metadata: packet.GetMetadata(),
		Envelope: packet.GetEnvelope(),
	}
	switch {
	case packet.GetAudio() != nil:
		resp.Payload = packet.GetPayload()
		resp.Inputs = packet.GetInputs()
		resp.Transports = packet.GetTransports()
		resp.Frame = &transportpb.DataResponse_Audio{Audio: packet.GetAudio()}
	case packet.GetVideo() != nil:
		resp.Payload = packet.GetPayload()
		resp.Inputs = packet.GetInputs()
		resp.Transports = packet.GetTransports()
		resp.Frame = &transportpb.DataResponse_Video{Video: packet.GetVideo()}
	case packet.GetBinary() != nil:
		resp.Payload = packet.GetPayload()
		resp.Inputs = packet.GetInputs()
		resp.Transports = packet.GetTransports()
		resp.Frame = &transportpb.DataResponse_Binary{Binary: packet.GetBinary()}
	default:
		if frame := structuredPacketBinaryFrame(packet); frame != nil {
			resp.Frame = &transportpb.DataResponse_Binary{Binary: frame}
			break
		}
		resp.Payload = packet.GetPayload()
		resp.Inputs = packet.GetInputs()
		resp.Transports = packet.GetTransports()
	}
	return resp
}

func structuredPacketBinaryFrame(packet *transportpb.DataPacket) *transportpb.BinaryFrame {
	env, err := structuredEnvelopeFromPacket(packet)
	if err != nil || env == nil {
		return nil
	}
	frame, err := envelope.ToBinaryFrame(env)
	if err != nil {
		return nil
	}
	return frame
}

//nolint:gocyclo // Envelope translation stays easier to audit as a single field-mapping routine.
func structuredEnvelopeFromPacket(packet *transportpb.DataPacket) (*envelope.Envelope, error) {
	if packet == nil {
		return nil, nil
	}
	env := &envelope.Envelope{Version: envelope.LatestVersion}
	populated := false

	kind := strings.TrimSpace(packet.GetMetadata()["kind"])
	if kind != "" {
		env.Kind = kind
		populated = true
	}
	if messageID := strings.TrimSpace(packet.GetMetadata()[metadataEnvelopeMessageIDKey]); messageID != "" {
		env.MessageID = messageID
		populated = true
	}
	if payload := packet.GetPayload(); payload != nil {
		raw, err := protojson.Marshal(payload)
		if err != nil {
			return nil, err
		}
		env.Payload = raw
		populated = true
	}
	if inputs := packet.GetInputs(); inputs != nil {
		raw, err := protojson.Marshal(inputs)
		if err != nil {
			return nil, err
		}
		env.Inputs = raw
		populated = true
	}
	if len(packet.GetTransports()) > 0 {
		env.Transports = make([]envelope.TransportDescriptor, 0, len(packet.GetTransports()))
		for _, descriptor := range packet.GetTransports() {
			if descriptor == nil {
				continue
			}
			var config map[string]any
			if descriptor.GetConfig() != nil {
				config = descriptor.GetConfig().AsMap()
			}
			env.Transports = append(env.Transports, envelope.TransportDescriptor{
				Name:   descriptor.GetName(),
				Kind:   descriptor.GetKind(),
				Mode:   descriptor.GetMode(),
				Config: config,
			})
		}
		if len(env.Transports) > 0 {
			populated = true
		}
	}
	if len(packet.GetMetadata()) > 0 && populated {
		env.Metadata = maps.Clone(packet.GetMetadata())
	}
	if !populated {
		return nil, nil
	}
	return env, nil
}

func (s *transportServer) forwardCapabilityUpdates(ctx context.Context, stream transportpb.TransportConnectorService_ControlServer, updates <-chan capabilityState) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case state, ok := <-updates:
			if !ok {
				return nil
			}
			if directive := capabilityStateToDirective(state); directive != nil {
				if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, "control capability send", func(context.Context) error {
					return stream.Send(directive)
				}); err != nil {
					return err
				}
				s.recordControlDirective("sent", controlResponseType(directive))
				s.log.V(1).Info("Control: forwarded capability directive", "type", controlResponseType(directive))
			}
		}
	}
}

func (s *transportServer) forwardHandoffUpdates(ctx context.Context, stream transportpb.TransportConnectorService_ControlServer, watcher *hangWatcher) error {
	if s == nil {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case directive, ok := <-s.handoffUpdates:
			if !ok {
				return nil
			}
			if directive == nil {
				continue
			}
			if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, "control handoff send", func(context.Context) error {
				return stream.Send(directive)
			}); err != nil {
				return err
			}
			s.recordControlDirective("sent", controlResponseType(directive))
			s.log.V(1).Info("Control: forwarded handoff directive", "type", controlResponseType(directive))
			if watcher != nil {
				watcher.Touch()
			}
		}
	}
}

func (s *transportServer) consumeControlDirectives(ctx context.Context, stream transportpb.TransportConnectorService_ControlServer, watcher *hangWatcher) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Do NOT use RecvWithTimeout here — the Control stream is often idle
		// when no directives are sent. A hard 30 s timeout kills the stream
		// and cascades cancellations. Connection liveness is handled by gRPC
		// keepalive and the optional hang watcher (touched on heartbeats).
		directive, err := stream.Recv()
		if err != nil {
			if err == io.EOF || errors.Is(err, context.Canceled) || status.Code(err) == status.Code(context.Canceled) {
				return nil
			}
			return err
		}
		if directive != nil {
			s.recordControlDirective("received", controlRequestType(directive))
			s.log.V(1).Info("Control: received directive", "type", controlRequestType(directive))
			if watcher != nil {
				// Count any control traffic (including heartbeat) as liveness.
				watcher.Touch()
			}
		}
		if resp := s.handleControlDirective(ctx, directive); resp != nil {
			if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, "control send resp", func(context.Context) error {
				return stream.Send(resp)
			}); err != nil {
				return err
			}
			s.recordControlDirective("sent", controlResponseType(resp))
			s.log.V(1).Info("Control: sent directive response", "type", controlResponseType(resp))
			if watcher != nil {
				watcher.Touch()
			}
		}
	}
}

func connectorReadyDirective(startupCapabilitiesMode string) *transportpb.ControlResponse {
	mode := strings.ToLower(strings.TrimSpace(startupCapabilitiesMode))
	if mode == "" {
		mode = coretransport.StartupCapabilitiesNone
	}
	return &transportpb.ControlResponse{
		Action: transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY,
		Metadata: map[string]string{
			coretransport.StartupCapabilitiesMetadataKey: mode,
		},
	}
}

func (s *transportServer) capabilityUpdates(ctx context.Context) <-chan capabilityState {
	if s.reporter == nil {
		ch := make(chan capabilityState)
		close(ch)
		return ch
	}
	return s.reporter.WatchCapabilities(ctx)
}

func capabilityStateToDirective(state capabilityState) *transportpb.ControlResponse {
	md := make(map[string]string)
	if state.audio != nil {
		md["audio.codec"] = strings.TrimSpace(state.audio.Name)
		if state.audio.SampleRateHz > 0 {
			md["audio.sample_rate_hz"] = strconv.Itoa(int(state.audio.SampleRateHz))
		}
		if state.audio.Channels > 0 {
			md["audio.channels"] = strconv.Itoa(int(state.audio.Channels))
		}
	}
	if state.video != nil {
		md["video.codec"] = strings.TrimSpace(state.video.Name)
		if strings.TrimSpace(state.video.Profile) != "" {
			md["video.profile"] = strings.TrimSpace(state.video.Profile)
		}
	}
	if strings.TrimSpace(state.binary) != "" {
		md["binary.mime"] = strings.TrimSpace(state.binary)
	}
	if len(md) == 0 {
		return nil
	}
	return &transportpb.ControlResponse{
		Action:   transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_CAPABILITIES,
		Metadata: md,
	}
}

func normalizeDirectiveType(val string) string {
	typ := strings.ToLower(strings.TrimSpace(val))
	if typ == "" {
		return "unknown"
	}
	return typ
}

var controlActionTypeNames = map[transportpb.ControlAction]string{
	transportpb.ControlAction_CONTROL_ACTION_NOOP:                   "noop",
	transportpb.ControlAction_CONTROL_ACTION_START:                  "start",
	transportpb.ControlAction_CONTROL_ACTION_STOP:                   "stop",
	transportpb.ControlAction_CONTROL_ACTION_START_UPSTREAM:         "start-upstream",
	transportpb.ControlAction_CONTROL_ACTION_STOP_UPSTREAM:          "stop-upstream",
	transportpb.ControlAction_CONTROL_ACTION_START_DOWNSTREAM:       "start-downstream",
	transportpb.ControlAction_CONTROL_ACTION_STOP_DOWNSTREAM:        "stop-downstream",
	transportpb.ControlAction_CONTROL_ACTION_HEARTBEAT:              "heartbeat",
	transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY:        "connector.ready",
	transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_CAPABILITIES: "connector.capabilities",
	transportpb.ControlAction_CONTROL_ACTION_HANDOFF_DRAINING:       handoffDirectiveDraining,
	transportpb.ControlAction_CONTROL_ACTION_HANDOFF_CUTOVER:        handoffDirectiveCutover,
	transportpb.ControlAction_CONTROL_ACTION_HANDOFF_READY:          handoffDirectiveReady,
	transportpb.ControlAction_CONTROL_ACTION_ACK:                    "ack",
	transportpb.ControlAction_CONTROL_ACTION_ERROR:                  "error",
	transportpb.ControlAction_CONTROL_ACTION_CODEC_SELECT:           "codec-select",
}

func protoControlActionToType(action transportpb.ControlAction, custom string) string {
	if strings.TrimSpace(custom) != "" {
		return strings.TrimSpace(custom)
	}
	return controlActionTypeNames[action]
}

func controlRequestType(req *transportpb.ControlRequest) string {
	if req == nil {
		return ""
	}
	return protoControlActionToType(req.GetAction(), req.GetCustomAction())
}

func controlResponseType(resp *transportpb.ControlResponse) string {
	if resp == nil {
		return ""
	}
	return protoControlActionToType(resp.GetAction(), resp.GetCustomAction())
}

func metadataValue(md metadata.MD, key string) string {
	if len(md) == 0 || strings.TrimSpace(key) == "" {
		return ""
	}
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

//nolint:gocyclo // Control directives are protocol-level switch cases and read better in one dispatcher.
func (s *transportServer) handleControlDirective(ctx context.Context, directive *transportpb.ControlRequest) *transportpb.ControlResponse {
	if directive == nil {
		return nil
	}
	typ := normalizeDirectiveType(protoControlActionToType(directive.GetAction(), directive.GetCustomAction()))
	s.log.Info("Received control directive", "type", typ, "metadataKeys", sortedMetadataKeys(directive.GetMetadata()))
	switch typ {
	case "", "noop":
		return ackDirective("noop", false, map[string]string{"reason": "empty"})
	case downstreamDeliveryDirective:
		streamID, seq, partition, size, err := parseDownstreamDeliveryReceipt(directive.GetMetadata())
		if err != nil {
			s.log.Error(err, "Invalid downstream delivery receipt")
			return nil
		}
		if s.p2p != nil && s.p2p.recordReceipt(streamID, seq, partition, size) {
			return nil
		}
		s.bridge.RecordReceipt(seq, partition, size)
		return nil
	case "start", "resume":
		s.gate.EnableUpstream(true)
		s.gate.EnableDownstream(true)
		return ackDirective(typ, true, nil)
	case "stop", "pause":
		s.gate.EnableUpstream(false)
		s.gate.EnableDownstream(false)
		return ackDirective(typ, true, nil)
	case "start-upstream":
		s.gate.EnableUpstream(true)
		return ackDirective(typ, true, nil)
	case "stop-upstream":
		s.gate.EnableUpstream(false)
		return ackDirective(typ, true, nil)
	case "start-downstream":
		s.gate.EnableDownstream(true)
		return ackDirective(typ, true, nil)
	case "stop-downstream":
		s.gate.EnableDownstream(false)
		return ackDirective(typ, true, nil)
	case "heartbeat":
		if s.reporter != nil {
			s.reporter.RecordHeartbeat(ctx, sanitizeHeartbeatMetadata(directive.GetMetadata()))
		}
		return ackDirective(typ, true, map[string]string{"status": "alive"})
	case "ack":
		return nil
	case "codec-select":
		return ackDirective(typ, false, map[string]string{"reason": "codec selection not implemented"})
	default:
		return ackDirective(typ, false, map[string]string{"reason": "unknown"})
	}
}

func parseDownstreamDeliveryReceipt(values map[string]string) (string, uint64, string, int, error) {
	if len(values) == 0 {
		return "", 0, "", 0, fmt.Errorf("delivery receipt metadata missing")
	}
	streamID := strings.TrimSpace(values[deliveryReceiptStreamIDKey])
	rawSeq := strings.TrimSpace(values[deliveryReceiptSequenceKey])
	if rawSeq == "" {
		return "", 0, "", 0, fmt.Errorf("delivery receipt sequence missing")
	}
	seq, err := strconv.ParseUint(rawSeq, 10, 64)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf("parse receipt sequence: %w", err)
	}
	rawSize := strings.TrimSpace(values[deliveryReceiptSizeBytesKey])
	if rawSize == "" {
		return streamID, seq, strings.TrimSpace(values[deliveryReceiptPartitionKey]), 0, nil
	}
	size, err := strconv.Atoi(rawSize)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf("parse receipt size: %w", err)
	}
	if size < 0 {
		return "", 0, "", 0, fmt.Errorf("delivery receipt size must be non-negative")
	}
	return streamID, seq, strings.TrimSpace(values[deliveryReceiptPartitionKey]), size, nil
}

func (s *transportServer) applyFlowControl(flow *transportpb.FlowControl) {
	if s == nil || flow == nil {
		return
	}
	if flow.GetSignal() == transportpb.FlowControlSignal_FLOW_CONTROL_SIGNAL_PAUSE {
		s.gate.EnableUpstream(false)
		s.log.V(1).Info("Flow control: upstream paused")
	}
	if flow.GetSignal() == transportpb.FlowControlSignal_FLOW_CONTROL_SIGNAL_RESUME {
		s.gate.EnableUpstream(true)
		s.log.V(1).Info("Flow control: upstream resumed")
	}
}

func ackDirective(received string, handled bool, extra map[string]string) *transportpb.ControlResponse {
	md := map[string]string{
		"received": strings.TrimSpace(received),
		"handled":  strconv.FormatBool(handled),
	}
	for k, v := range extra {
		if strings.TrimSpace(k) == "" {
			continue
		}
		md[k] = v
	}
	return &transportpb.ControlResponse{
		Action:   transportpb.ControlAction_CONTROL_ACTION_ACK,
		Metadata: md,
	}
}

type flowControlMode string

const (
	flowControlNone    flowControlMode = "none"
	flowControlCredits flowControlMode = "credits"
	flowControlWindow  flowControlMode = "window"
)

type orderingMode string

const (
	orderingNone         orderingMode = "none"
	orderingPerStream    orderingMode = "per_stream"
	orderingPerPartition orderingMode = "per_partition"
)

type flowAckSettings struct {
	Messages *int    `json:"messages,omitempty"`
	Bytes    *int    `json:"bytes,omitempty"`
	MaxDelay *string `json:"maxDelay,omitempty"`
}

type flowControlConfig struct {
	Mode     string           `json:"mode,omitempty"`
	AckEvery *flowAckSettings `json:"ackEvery,omitempty"`
}

type deliveryConfig struct {
	Ordering  string `json:"ordering,omitempty"`
	Semantics string `json:"semantics,omitempty"`
}

type transportSettings struct {
	FlowControl *flowControlConfig `json:"flowControl,omitempty"`
	Delivery    *deliveryConfig    `json:"delivery,omitempty"`
}

type flowTracker struct {
	mu                 sync.Mutex
	mode               flowControlMode
	ordering           orderingMode
	ackEveryMessages   int
	ackEveryBytes      int
	ackEveryDelay      time.Duration
	pendingMessages    int
	pendingBytes       int
	lastSeq            uint64
	lastSeqByPartition map[string]uint64
	pendingAcks        map[string]uint64
	lastSend           time.Time
	log                logr.Logger
}

//nolint:gocyclo // Flow tracker setup keeps delivery and flow-control parsing in a single initializer.
func newFlowTracker(log logr.Logger, payload []byte) *flowTracker {
	tracker := &flowTracker{
		mode:             flowControlNone,
		ordering:         orderingNone,
		ackEveryMessages: 1,
		log:              log,
	}
	if len(payload) == 0 {
		return tracker
	}
	var settings transportSettings
	if err := json.Unmarshal(payload, &settings); err != nil {
		log.V(1).Info("Failed to parse transport settings payload for flow control", "error", err)
		return tracker
	}
	if settings.FlowControl == nil {
	} else {
		switch strings.ToLower(strings.TrimSpace(settings.FlowControl.Mode)) {
		case string(flowControlCredits):
			tracker.mode = flowControlCredits
		case string(flowControlWindow):
			tracker.mode = flowControlWindow
		case "", string(flowControlNone):
			tracker.mode = flowControlNone
		default:
			log.V(1).Info("Unknown flow control mode; defaulting to none", "mode", settings.FlowControl.Mode)
			tracker.mode = flowControlNone
		}
		if settings.FlowControl.AckEvery != nil {
			if settings.FlowControl.AckEvery.Messages != nil {
				if *settings.FlowControl.AckEvery.Messages > 0 {
					tracker.ackEveryMessages = *settings.FlowControl.AckEvery.Messages
				} else {
					log.V(1).Info("Invalid ackEvery.messages; defaulting to 1", "value", *settings.FlowControl.AckEvery.Messages)
				}
			}
			if settings.FlowControl.AckEvery.Bytes != nil && *settings.FlowControl.AckEvery.Bytes > 0 {
				tracker.ackEveryBytes = *settings.FlowControl.AckEvery.Bytes
			}
			if settings.FlowControl.AckEvery.MaxDelay != nil && strings.TrimSpace(*settings.FlowControl.AckEvery.MaxDelay) != "" {
				if d, err := time.ParseDuration(strings.TrimSpace(*settings.FlowControl.AckEvery.MaxDelay)); err == nil && d > 0 {
					tracker.ackEveryDelay = d
				} else {
					log.V(1).Info("Invalid ackEvery.maxDelay; ignoring", "value", *settings.FlowControl.AckEvery.MaxDelay)
				}
			}
		}
	}
	if settings.Delivery != nil {
		ordering := strings.ToLower(strings.TrimSpace(settings.Delivery.Ordering))
		switch ordering {
		case string(orderingPerStream):
			tracker.ordering = orderingPerStream
		case string(orderingPerPartition):
			tracker.ordering = orderingPerPartition
		case string(orderingNone), "":
			tracker.ordering = orderingNone
		default:
			log.V(1).Info("Unknown ordering mode; defaulting to none", "ordering", settings.Delivery.Ordering)
			tracker.ordering = orderingNone
		}
		semantics := strings.ToLower(strings.TrimSpace(settings.Delivery.Semantics))
		if semantics == "at_least_once" && tracker.ordering == orderingNone {
			tracker.ordering = orderingPerStream
		}
	}
	return tracker
}

func (t *flowTracker) reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.pendingMessages = 0
	t.pendingBytes = 0
	t.lastSeq = 0
	t.lastSeqByPartition = nil
	t.pendingAcks = nil
	t.lastSend = time.Time{}
	t.mu.Unlock()
}

func (t *flowTracker) recordDelivery(packet *transportpb.DataPacket) *transportpb.FlowControl {
	if t == nil || packet == nil {
		return nil
	}
	return t.recordReceipt(proto.Size(packet), packet.GetEnvelope().GetSequence(), packet.GetEnvelope().GetPartition())
}

//nolint:gocyclo // Receipt bookkeeping spans ordering, batching, and credit updates in one critical section.
func (t *flowTracker) recordReceipt(size int, seq uint64, partition string) *transportpb.FlowControl {
	if t == nil {
		return nil
	}
	now := time.Now()

	t.mu.Lock()
	advanced := false
	if t.ordering == orderingPerPartition {
		if seq > 0 {
			if t.lastSeqByPartition == nil {
				t.lastSeqByPartition = make(map[string]uint64)
			}
			if seq > t.lastSeqByPartition[partition] {
				t.lastSeqByPartition[partition] = seq
				advanced = true
			}
			if advanced && t.pendingAcks == nil {
				t.pendingAcks = make(map[string]uint64)
			}
			if advanced && seq > t.pendingAcks[partition] {
				t.pendingAcks[partition] = seq
			}
		}
	} else if seq > 0 && seq > t.lastSeq {
		t.lastSeq = seq
		advanced = true
	}
	if seq > 0 && t.ordering != orderingNone && !advanced {
		t.mu.Unlock()
		return nil
	}
	t.pendingMessages++
	t.pendingBytes += size

	shouldSend := t.ackEveryMessages > 0 && t.pendingMessages >= t.ackEveryMessages

	if t.ackEveryBytes > 0 && t.pendingBytes >= t.ackEveryBytes {
		shouldSend = true
	}
	if t.ackEveryDelay > 0 && (t.lastSend.IsZero() || now.Sub(t.lastSend) >= t.ackEveryDelay) {
		shouldSend = true
	}
	if !shouldSend {
		t.mu.Unlock()
		return nil
	}

	flow := &transportpb.FlowControl{}
	if t.ordering == orderingPerPartition {
		if len(t.pendingAcks) > 0 {
			partitions := make([]string, 0, len(t.pendingAcks))
			for partitionKey := range t.pendingAcks {
				partitions = append(partitions, partitionKey)
			}
			sort.Strings(partitions)
			flow.PartitionAcks = make([]*transportpb.PartitionAck, 0, len(partitions))
			for _, partitionKey := range partitions {
				ack := t.pendingAcks[partitionKey]
				if ack == 0 {
					continue
				}
				flow.PartitionAcks = append(flow.PartitionAcks, &transportpb.PartitionAck{Partition: partitionKey, Ack: ack})
			}
		}
	} else if t.lastSeq > 0 {
		flow.Ack = t.lastSeq
	}
	if t.mode == flowControlCredits {
		flow.CreditsMessages = clampUint32(t.pendingMessages)
		flow.CreditsBytes = clampUint32(t.pendingBytes)
	}
	t.pendingMessages = 0
	t.pendingBytes = 0
	if t.ordering == orderingPerPartition {
		t.pendingAcks = nil
	}
	t.lastSend = now
	t.mu.Unlock()

	if flow.Ack == 0 && len(flow.PartitionAcks) == 0 && flow.CreditsMessages == 0 && flow.CreditsBytes == 0 {
		return nil
	}
	return flow
}

func clampUint32(v int) uint32 {
	if v <= 0 {
		return 0
	}
	if v > int(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(v)
}

type hubBridge struct {
	cfg *Config
	log logr.Logger

	clientMu     sync.RWMutex
	conn         *grpc.ClientConn
	client       transportpb.HubService_ProcessClient
	streamCancel context.CancelFunc

	sendMu    sync.Mutex
	recvCh    chan *transportpb.DataPacket
	closeOnce sync.Once
	closed    atomic.Bool

	outboxMu        sync.Mutex
	outbox          []*transportpb.DataPacket
	outboxMax       int
	flushInProgress atomic.Bool

	reconnectMu  sync.Mutex
	reconnecting bool
	ctx          context.Context
	cancel       context.CancelFunc

	messageTimeout     time.Duration
	channelSendTimeout time.Duration
	watcher            *hangWatcher
	flow               *flowTracker
	onFlow             func(*transportpb.FlowControl)
	onHandoff          func(phase, reason string)
}

//nolint:logcheck // Bridge construction needs both an explicit logger and a parent lifecycle context.
func newHubBridge(ctx context.Context, cfg *Config, log logr.Logger, tunables runtimeTunables) (*hubBridge, error) {
	bridgeCtx, bridgeCancel := context.WithCancel(ctx)
	hb := &hubBridge{
		cfg:                cfg,
		log:                log,
		recvCh:             make(chan *transportpb.DataPacket, tunables.ChannelBufferSize),
		ctx:                bridgeCtx,
		cancel:             bridgeCancel,
		messageTimeout:     tunables.MessageTimeout,
		channelSendTimeout: tunables.ChannelSendTimeout,
	}
	hb.outboxMax = outboxMaxFromTunables(tunables)
	if hb.channelSendTimeout > 0 {
		log.Info("WARNING: channelSendTimeout > 0 is configured. "+
			"This is a lossy setting — packets may be silently dropped when the receive channel is full. "+
			"The blocking path (channelSendTimeout = 0) provides better backpressure.",
			"channelSendTimeout", hb.channelSendTimeout)
	}
	var settingsPayload []byte
	if cfg != nil && cfg.Binding.Info != nil {
		settingsPayload = cfg.Binding.Info.GetPayload()
	}
	hb.flow = newFlowTracker(log.WithName("flow-control"), settingsPayload)
	if tunables.HangTimeout > 0 {
		hb.watcher = newHangWatcher(tunables.HangTimeout, hb.forceReconnect, log.WithName("hub-bridge"))
	}
	conn, client, streamCancel, err := hb.openHubStream(ctx)
	if err != nil {
		bridgeCancel()
		return nil, err
	}
	hb.setConnection(conn, client, streamCancel)
	hb.startReadLoop(client)
	log.Info("Hub bridge read loop started", "step", cfg.StepID, "outboxMax", hb.outboxMax)
	return hb, nil
}

func outboxMaxFromTunables(tunables runtimeTunables) int {
	max := tunables.ChannelBufferSize
	if max < 64 {
		max = 64
	}
	if max > 2048 {
		max = 2048
	}
	return max
}

func (b *hubBridge) openHubStream(ctx context.Context) (*grpc.ClientConn, transportpb.HubService_ProcessClient, context.CancelFunc, error) {
	if b == nil || b.cfg == nil {
		return nil, nil, nil, fmt.Errorf("hub bridge config missing")
	}
	if b.cfg.Generation <= 0 {
		return nil, nil, nil, fmt.Errorf("%s must be set to a positive value", contracts.ConnectorGenerationEnv)
	}
	b.log.Info("Connecting to hub", "endpoint", b.cfg.HubEndpoint, "step", b.cfg.StepID, "storyRun", b.cfg.StoryRunName)
	conn, err := dialHub(ctx, b.cfg)
	if err != nil {
		b.log.Error(err, "Failed to dial hub", "endpoint", b.cfg.HubEndpoint)
		return nil, nil, nil, err
	}
	md := hubStreamMetadata(b.cfg)
	streamCtx, streamCancel := context.WithCancel(ctx)
	processCtx := metadata.NewOutgoingContext(streamCtx, md)
	client, err := transportpb.NewHubServiceClient(conn).Process(processCtx)
	if err != nil {
		b.log.Error(err, "Failed to create hub stream", "endpoint", b.cfg.HubEndpoint)
		streamCancel()
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("connecting to hub: %w", err)
	}
	b.log.Info("Successfully connected to hub and registered stream", "step", b.cfg.StepID, "storyRun", b.cfg.StoryRunName)
	return conn, client, streamCancel, nil
}

func hubStreamMetadata(cfg *Config) metadata.MD {
	startupMode := coretransport.StartupCapabilitiesNone
	if cfg != nil && cfg.Binding.Info != nil {
		startupMode = startupCapabilitiesModeFromBindingInfo(cfg.Binding.Info)
	}
	return metadata.Pairs(
		metaStoryRunName, cfg.StoryRunName,
		metaStoryRunNS, cfg.Namespace,
		metaCurrentStepID, cfg.StepID,
		metaConnectorGeneration, strconv.FormatInt(int64(cfg.Generation), 10),
		coretransport.ProtocolMetadataKey, coretransport.ProtocolVersion,
		coretransport.StartupCapabilitiesMetadataKey, startupMode,
	)
}

func (b *hubBridge) setConnection(conn *grpc.ClientConn, client transportpb.HubService_ProcessClient, streamCancel context.CancelFunc) {
	b.clientMu.Lock()
	b.conn = conn
	b.client = client
	b.streamCancel = streamCancel
	b.clientMu.Unlock()
}

func (b *hubBridge) getClient() transportpb.HubService_ProcessClient {
	b.clientMu.RLock()
	client := b.client
	b.clientMu.RUnlock()
	return client
}

func (b *hubBridge) startReadLoop(client transportpb.HubService_ProcessClient) {
	if client == nil {
		return
	}
	go b.readLoop(client)
}

func (b *hubBridge) readLoop(client transportpb.HubService_ProcessClient) {
	b.log.Info("Hub bridge readLoop started, waiting for packets from hub")
	for {
		// Do NOT use RecvWithTimeout here — downstream steps (synthesize,
		// playback, transcribe) may not receive hub packets for extended
		// periods when the pipeline is idle or waiting for upstream events.
		// A 30 s hard timeout tears down the hub bridge and cascades to
		// all local streams.  Connection liveness is handled by gRPC
		// keepalive and the optional hang watcher.
		response, err := client.Recv()
		if err != nil {
			b.handleDisconnect(client, err)
			return
		}
		if response == nil {
			b.log.V(1).Info("Received nil response from hub")
			continue
		}
		if flow := response.GetFlow(); flow != nil {
			b.handleFlow(flow)
		}
		packet := response.GetPacket()
		if packet == nil {
			b.log.V(1).Info("Received response with nil packet from hub")
			continue
		}
		b.log.Info("Received packet from hub, enqueueing for local engram", "metadataKeys", len(packet.Metadata))
		if err := b.enqueuePacket(packet); err != nil {
			b.log.Error(err, "failed to enqueue hub packet")
			b.handleDisconnect(client, err)
			return
		}
		b.touch()
	}
}

func (b *hubBridge) handleDisconnect(client transportpb.HubService_ProcessClient, err error) {
	if b == nil {
		return
	}
	if b.ctx.Err() != nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
		b.log.Info("Hub bridge readLoop exiting", "reason", err.Error())
		b.notifyHandoff("draining", "hub_disconnected")
	} else {
		b.log.Error(err, "hub stream recv failed")
		b.notifyHandoff("draining", "hub_recv_failed")
	}
	b.disconnectCurrent(client)
}

func (b *hubBridge) disconnectCurrent(client transportpb.HubService_ProcessClient) {
	if b == nil {
		return
	}
	if client == nil {
		b.startReconnect("hub_disconnected")
		return
	}
	b.clientMu.Lock()
	if b.client != client {
		b.clientMu.Unlock()
		return
	}
	conn := b.conn
	streamCancel := b.streamCancel
	b.conn = nil
	b.client = nil
	b.streamCancel = nil
	b.clientMu.Unlock()

	if streamCancel != nil {
		streamCancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
	b.startReconnect("hub_disconnected")
}

func (b *hubBridge) startReconnect(reason string) {
	if b == nil || b.ctx.Err() != nil {
		return
	}
	b.reconnectMu.Lock()
	if b.reconnecting {
		b.reconnectMu.Unlock()
		return
	}
	b.reconnecting = true
	b.reconnectMu.Unlock()
	go b.reconnectLoop(reason)
}

// maxReconnectAttempts is the maximum number of hub reconnection attempts
// before the connector gives up and exits. Zero means unlimited (legacy
// behaviour). Override via BOBRAVOZ_MAX_RECONNECT_ATTEMPTS env var at startup.
var maxReconnectAttempts = 200

func (b *hubBridge) reconnectLoop(reason string) {
	defer func() {
		b.reconnectMu.Lock()
		b.reconnecting = false
		b.reconnectMu.Unlock()
	}()
	backoff := time.Second
	attempts := 0
	for {
		if b.ctx.Err() != nil {
			return
		}
		attempts++
		if maxReconnectAttempts > 0 && attempts > maxReconnectAttempts {
			b.log.Error(fmt.Errorf("max reconnect attempts (%d) exceeded", maxReconnectAttempts),
				"Hub reconnect abandoned", "reason", reason)
			b.cancel()
			return
		}
		conn, client, streamCancel, err := b.openHubStream(b.ctx)
		if err != nil {
			// Check if the error is retryable based on gRPC status code.
			if st, ok := status.FromError(err); ok {
				switch st.Code() {
				case codes.PermissionDenied, codes.InvalidArgument, codes.Unimplemented:
					// Non-retryable errors — don't reconnect
					b.log.Error(err, "Hub connection failed with non-retryable error")
					b.cancel()
					return
				}
			}
			b.log.Error(err, "Hub reconnect failed", "reason", reason,
				"backoff", backoff, "attempt", attempts)
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			time.Sleep(backoff + jitter)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		b.setConnection(conn, client, streamCancel)
		b.startReadLoop(client)
		if b.flow != nil {
			b.flow.reset()
		}
		b.notifyHandoff("cutover", "hub_reconnected")
		b.flushOutboxAsync()
		return
	}
}

func (b *hubBridge) forceReconnect() {
	if b == nil {
		return
	}
	b.log.Info("Hub bridge hang timeout; reconnecting")
	client := b.getClient()
	if client == nil {
		b.startReconnect("hang_timeout")
		return
	}
	b.disconnectCurrent(client)
}

func (b *hubBridge) handleFlow(flow *transportpb.FlowControl) {
	if b == nil || flow == nil {
		return
	}
	if b.onFlow != nil {
		b.onFlow(flow)
	}
}

func (b *hubBridge) notifyHandoff(phase, reason string) {
	if b == nil || b.onHandoff == nil {
		return
	}
	b.onHandoff(phase, reason)
}

func (b *hubBridge) Send(packet *transportpb.DataPacket) error {
	if b == nil || packet == nil {
		return nil
	}
	if b.ctx != nil && b.ctx.Err() != nil {
		return b.ctx.Err()
	}
	client := b.getClient()
	if client == nil {
		b.enqueueOutbox(packet, "not_connected")
		b.startReconnect("send")
		return nil
	}
	if err := b.sendPacket(client, packet); err != nil {
		b.enqueueOutbox(packet, "send_error")
		b.handleDisconnect(client, err)
		return nil
	}
	b.touch()
	b.flushOutboxAsync()
	return nil
}

func (b *hubBridge) SendFlow(flow *transportpb.FlowControl) error {
	if b == nil || flow == nil {
		return nil
	}
	if b.ctx != nil && b.ctx.Err() != nil {
		return b.ctx.Err()
	}
	client := b.getClient()
	if client == nil {
		return nil
	}
	if err := b.sendFlow(client, flow); err != nil {
		b.handleDisconnect(client, err)
		return nil
	}
	b.touch()
	return nil
}

func (b *hubBridge) RecordDelivery(packet *transportpb.DataPacket) {
	if b == nil || b.flow == nil {
		return
	}
	flow := b.flow.recordDelivery(packet)
	if flow == nil {
		return
	}
	if err := b.SendFlow(flow); err != nil {
		b.log.V(1).Info("Failed to send flow control update", "error", err)
	}
}

func (b *hubBridge) RecordReceipt(seq uint64, partition string, size int) {
	if b == nil || b.flow == nil {
		return
	}
	flow := b.flow.recordReceipt(size, seq, partition)
	if flow == nil {
		return
	}
	if err := b.SendFlow(flow); err != nil {
		b.log.V(1).Info("Failed to send flow control update", "error", err)
	}
}

func (b *hubBridge) Recv() <-chan *transportpb.DataPacket {
	return b.recvCh
}

func (b *hubBridge) Close() {
	if b.watcher != nil {
		b.watcher.Stop()
	}
	b.notifyHandoff("draining", "bridge_closed")
	b.cancel()
	b.closed.Store(true)
	b.clientMu.Lock()
	streamCancel := b.streamCancel
	client := b.client
	conn := b.conn
	b.client = nil
	b.conn = nil
	b.streamCancel = nil
	b.clientMu.Unlock()
	if streamCancel != nil {
		streamCancel()
	}
	if client != nil {
		_ = client.CloseSend()
	}
	if conn != nil {
		_ = conn.Close()
	}
	b.closeOnce.Do(func() {
		close(b.recvCh)
	})
}

func (b *hubBridge) enqueuePacket(packet *transportpb.DataPacket) error {
	if packet == nil {
		return nil
	}
	if b.closed.Load() {
		return fmt.Errorf("hub bridge closed")
	}
	if b.channelSendTimeout <= 0 {
		select {
		case <-b.ctx.Done():
			return b.ctx.Err()
		case b.recvCh <- packet:
			return nil
		}
	}
	timer := time.NewTimer(b.channelSendTimeout)
	defer timer.Stop()
	select {
	case <-b.ctx.Done():
		return b.ctx.Err()
	case b.recvCh <- packet:
		return nil
	case <-timer.C:
		b.log.Info("Hub channel send timeout; dropping packet to preserve session",
			"timeout", b.channelSendTimeout)
		metrics.RecordConnectorDownstreamDrop(
			b.cfg.Binding.Reference.Namespace, b.cfg.Binding.Reference.Name, "channel_timeout")
		return nil
	}
}

func (b *hubBridge) sendPacket(client transportpb.HubService_ProcessClient, packet *transportpb.DataPacket) error {
	if client == nil || packet == nil {
		return fmt.Errorf("hub stream unavailable")
	}
	return transportconnector.CallWithTimeout(b.ctx, b.messageTimeout, "hub send", func(context.Context) error {
		b.sendMu.Lock()
		defer b.sendMu.Unlock()
		return client.Send(&transportpb.ProcessRequest{Packet: packet})
	})
}

func (b *hubBridge) sendFlow(client transportpb.HubService_ProcessClient, flow *transportpb.FlowControl) error {
	if client == nil || flow == nil {
		return fmt.Errorf("hub stream unavailable")
	}
	return transportconnector.CallWithTimeout(b.ctx, b.messageTimeout, "hub flow send", func(context.Context) error {
		b.sendMu.Lock()
		defer b.sendMu.Unlock()
		return client.Send(&transportpb.ProcessRequest{Flow: flow})
	})
}

// isControlPacket returns true when the packet carries transport metadata or
// lifecycle descriptors but no media frame (audio/video/binary). These packets
// are given priority in the outbox so that flow-control and topology signals are
// not dropped in favor of data packets.
func isControlPacket(packet *transportpb.DataPacket) bool {
	return packet != nil && packet.GetFrame() == nil
}

func (b *hubBridge) enqueueOutbox(packet *transportpb.DataPacket, reason string) {
	if b == nil || packet == nil || b.outboxMax <= 0 {
		return
	}
	b.outboxMu.Lock()
	dropped := 0
	if len(b.outbox) >= b.outboxMax {
		if isControlPacket(packet) {
			// The incoming packet is a control message — drop the oldest DATA
			// packet to make room, preserving control messages.
			idx := -1
			for i, p := range b.outbox {
				if !isControlPacket(p) {
					idx = i
					break
				}
			}
			if idx >= 0 {
				b.outbox = append(b.outbox[:idx], b.outbox[idx+1:]...)
				dropped = 1
			} else {
				// Only control messages in the outbox; drop the oldest one.
				b.outbox = b.outbox[1:]
				dropped = 1
			}
		} else {
			// Incoming packet is data — drop the oldest data packet. If the
			// oldest packet is a control message, scan forward for a data
			// packet to drop instead.
			idx := -1
			for i, p := range b.outbox {
				if !isControlPacket(p) {
					idx = i
					break
				}
			}
			if idx >= 0 {
				b.outbox = append(b.outbox[:idx], b.outbox[idx+1:]...)
			} else {
				// Only control messages — drop the oldest one to make room.
				b.outbox = b.outbox[1:]
			}
			dropped = 1
		}
	}
	b.outbox = append(b.outbox, packet)
	b.outboxMu.Unlock()
	if dropped > 0 {
		b.log.Info("Hub outbox dropped packets", "dropped", dropped, "reason", reason, "outboxMax", b.outboxMax)
	}
}

func (b *hubBridge) flushOutboxAsync() {
	if b == nil || b.outboxMax <= 0 {
		return
	}
	if !b.flushInProgress.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer b.flushInProgress.Store(false)
		b.flushOutbox()
	}()
}

func (b *hubBridge) flushOutbox() {
	for {
		batch := b.takeOutbox()
		if len(batch) == 0 {
			return
		}
		client := b.getClient()
		if client == nil {
			b.requeueFront(batch)
			b.startReconnect("flush")
			return
		}
		for i, packet := range batch {
			if err := b.sendPacket(client, packet); err != nil {
				b.requeueFront(batch[i:])
				b.handleDisconnect(client, err)
				return
			}
		}
	}
}

func (b *hubBridge) takeOutbox() []*transportpb.DataPacket {
	b.outboxMu.Lock()
	if len(b.outbox) == 0 {
		b.outboxMu.Unlock()
		return nil
	}
	batch := b.outbox
	b.outbox = nil
	b.outboxMu.Unlock()
	return batch
}

func (b *hubBridge) requeueFront(packets []*transportpb.DataPacket) {
	if b == nil || len(packets) == 0 || b.outboxMax <= 0 {
		return
	}
	b.outboxMu.Lock()
	combined := append(packets, b.outbox...)
	if len(combined) > b.outboxMax {
		combined = combined[len(combined)-b.outboxMax:]
	}
	b.outbox = combined
	b.outboxMu.Unlock()
}

func (b *hubBridge) touch() {
	if b.watcher != nil {
		b.watcher.Touch()
	}
}

func dialHub(ctx context.Context, cfg *Config) (*grpc.ClientConn, error) {
	opts := make([]grpc.DialOption, 0, 3)
	if cfg.HubDialTimeout > 0 {
		opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: cfg.HubDialTimeout,
		}))
	}
	if creds, err := transportconnector.HubCredentials(transportconnector.OSEnv); err == nil {
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		return nil, err
	}

	if telemetry.TracePropagationEnabled() {
		opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}
	if callOpts := transportconnector.ClientCallOptions(
		transportconnector.OSEnv,
		transportconnector.DefaultMaxMessageSize,
		transportconnector.DefaultMaxMessageSize,
	); len(callOpts) > 0 {
		opts = append(opts, grpc.WithDefaultCallOptions(callOpts...))
	}

	conn, err := grpc.NewClient(cfg.HubEndpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("create hub client %s: %w", cfg.HubEndpoint, err)
	}

	var (
		waitCtx context.Context
		cancel  context.CancelFunc
	)
	if cfg.HubDialTimeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, cfg.HubDialTimeout)
	} else {
		waitCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	if err := transportconnector.WaitForReady(waitCtx, conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("wait for hub readiness: %w", err)
	}
	return conn, nil
}

func sortedMetadataKeys(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type mediaGate struct {
	upstream   atomic.Bool
	downstream atomic.Bool
}

func newMediaGate() *mediaGate {
	g := &mediaGate{}
	g.upstream.Store(true)
	g.downstream.Store(true)
	return g
}

func (g *mediaGate) AllowUpstream() bool {
	if g == nil {
		return true
	}
	return g.upstream.Load()
}

func (g *mediaGate) AllowDownstream() bool {
	if g == nil {
		return true
	}
	return g.downstream.Load()
}

func (g *mediaGate) EnableUpstream(enabled bool) {
	if g == nil {
		return
	}
	g.upstream.Store(enabled)
}

func (g *mediaGate) EnableDownstream(enabled bool) {
	if g == nil {
		return
	}
	g.downstream.Store(enabled)
}
