package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bubustack/bobravoz-grpc/internal/telemetry"
	"github.com/bubustack/bobravoz-grpc/pkg/metrics"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportconnector "github.com/bubustack/core/runtime/transport/connector"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	metaStoryRunName  = "storyrun-name"
	metaStoryRunNS    = "storyrun-namespace"
	metaCurrentStepID = "current-step-id"

	metadataEnvelopeKindKey      = "bubu.envelope.kind"
	metadataEnvelopeMessageIDKey = "bubu.envelope.message_id"
	metadataEnvelopeTimeKey      = "bubu.envelope.timestamp_ms"

	handoffDirectiveDraining = "handoff.draining"
	handoffDirectiveCutover  = "handoff.cutover"
	handoffDirectiveReady    = "handoff.ready"
)

type handoffState struct {
	phase     string
	reason    string
	updatedAt time.Time
}

type runtimeTunables = transportconnector.RuntimeTunables

// Runner hosts the TransportConnector gRPC server and bridges frames to the hub.
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

	// Register TransportConnectorServer for local engram connections
	transportServer := newTransportServer(ctx, r.cfg, r.log, bridge, tunables)
	transportpb.RegisterTransportConnectorServer(server, transportServer)
	bridge.notifyHandoff("cutover", "hub_connected")

	// Register HubService for P2P connections from upstream connectors
	// This allows this connector to act as a "mini-hub" for P2P routing
	p2pSvc := newP2PServer(r.log, bridge)
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
		server.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

type transportServer struct {
	transportpb.UnimplementedTransportConnectorServer
	ctx            context.Context
	cfg            *Config
	log            logr.Logger
	bridge         *hubBridge
	reporter       *bindingStatusReporter
	reportOnce     sync.Once
	gate           *mediaGate
	tunables       runtimeTunables
	handoffMu      sync.Mutex
	handoffState   handoffState
	handoffUpdates chan *transportpb.ControlDirective
	watchersMu     sync.Mutex
	watchers       map[*hangWatcher]struct{}
}

func newTransportServer(ctx context.Context, cfg *Config, log logr.Logger, bridge *hubBridge, tunables runtimeTunables) *transportServer {
	server := &transportServer{
		ctx:            ctx,
		cfg:            cfg,
		log:            log,
		bridge:         bridge,
		reporter:       newBindingStatusReporter(cfg, log),
		gate:           newMediaGate(),
		tunables:       tunables,
		handoffUpdates: make(chan *transportpb.ControlDirective, 8),
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

func handoffDirective(state handoffState) *transportpb.ControlDirective {
	phase := strings.ToLower(strings.TrimSpace(state.phase))
	var typ string
	switch phase {
	case "draining":
		typ = handoffDirectiveDraining
	case "cutover":
		typ = handoffDirectiveCutover
	case "ready":
		typ = handoffDirectiveReady
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
	return &transportpb.ControlDirective{Type: typ, Metadata: md}
}

func (s *transportServer) reportReady(_ context.Context) {
	if s.reporter == nil {
		return
	}
	s.reporter.Start(s.ctx)
	s.reportOnce.Do(func() {
		s.reporter.ReportReady(s.ctx)
	})
}

func (s *transportServer) Publish(stream transportpb.TransportConnector_PublishServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	var watcher *hangWatcher
	if s.tunables.HangTimeout > 0 {
		watcher = newHangWatcher(s.tunables.HangTimeout, cancel, s.log.WithName("publish-hang"))
		defer watcher.Stop()
	}
	unregisterWatcher := s.registerWatcher(watcher)
	defer unregisterWatcher()
	s.reportReady(ctx)
	for {
		// Do NOT use RecvWithTimeout here — the Publish stream is inherently
		// bursty; engrams only publish when they have data.  A hard 30 s
		// timeout kills idle streams and cascades to all other goroutines
		// via the shared errgroup context.  Connection liveness is handled
		// by gRPC keepalive and the optional hang watcher instead.
		req, err := stream.Recv()
		if err != nil {
			if status.Code(err) == status.Code(context.Canceled) || err == context.Canceled {
				return err
			}
			if err == context.DeadlineExceeded {
				return err
			}
			if err == io.EOF {
				break
			}
			return err
		}

		// Log what the SDK is sending
		hasAudio := req.GetAudio() != nil
		hasVideo := req.GetVideo() != nil
		hasBinary := req.GetBinary() != nil
		audioPcmLen := 0
		if hasAudio {
			audioPcmLen = len(req.GetAudio().GetPcm())
		}
		s.log.Info("[CONNECTOR_PUBLISH] Received from local engram SDK",
			"hasAudio", hasAudio,
			"audioPcmLen", audioPcmLen,
			"hasVideo", hasVideo,
			"hasBinary", hasBinary)

		// Detect SDK publish heartbeats: no frame, just metadata.
		// Touch the hang watcher to keep the stream alive but do not
		// forward the heartbeat to the hub — it carries no real data.
		if isPublishHeartbeat(req) {
			if watcher != nil {
				watcher.Touch()
			}
			continue
		}

		s.recordCapabilitiesFromRequest(req)
		packet, err := publishRequestToHubPacket(req)
		if err != nil {
			return err
		}

		// Log what we're sending to hub
		hasAudioAfter := packet.GetAudio() != nil
		audioPcmLenAfter := 0
		if hasAudioAfter {
			audioPcmLenAfter = len(packet.GetAudio().GetPcm())
		}
		s.log.Info("[CONNECTOR_PUBLISH] Sending to hub",
			"hasAudio", hasAudioAfter,
			"audioPcmLen", audioPcmLenAfter,
			"hasPayload", packet.GetPayload() != nil,
			"hasInputs", packet.GetInputs() != nil)

		if !s.gate.AllowUpstream() {
			s.log.V(1).Info("Upstream flow paused; dropping frame", "type", describeFrame(req))
			continue
		}
		if watcher != nil {
			watcher.Touch()
		}
		if err := s.bridge.Send(packet); err != nil {
			return err
		}
	}
	return transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, cancel, "publish close", func() error {
		return stream.SendAndClose(&transportpb.PublishResponse{})
	})
}

// Subscribe proxies packets from the hub bridge to the local Engram stream,
// touching hang watchers, honoring AllowDownstream gating, and wrapping each
// send in transportconnector.CallWithTimeout until the context is canceled or the bridge closes
// (`internal/connector/connector.go:241-293`).
func (s *transportServer) Subscribe(_ *transportpb.SubscribeRequest, stream transportpb.TransportConnector_SubscribeServer) error {
	s.log.Info("Subscribe stream opened by local engram")
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	var watcher *hangWatcher
	if s.tunables.HangTimeout > 0 {
		watcher = newHangWatcher(s.tunables.HangTimeout, cancel, s.log.WithName("subscribe-hang"))
		defer watcher.Stop()
	}
	unregisterWatcher := s.registerWatcher(watcher)
	defer unregisterWatcher()
	s.reportReady(ctx)
	s.log.Info("Subscribe: waiting for packets from hub bridge")
	for {
		select {
		case <-ctx.Done():
			s.log.Info("Subscribe stream closed", "reason", ctx.Err())
			return ctx.Err()
		case packet, ok := <-s.bridge.Recv():
			if !ok {
				s.log.Info("Subscribe: hub bridge channel closed")
				s.recordDownstreamDrop("bridge_closed")
				return nil
			}
			s.log.Info("Subscribe: received packet from hub bridge, translating for engram", "metadataKeys", len(packet.Metadata))
			msg, err := hubPacketToPublishRequest(s.log, packet)
			if err != nil {
				s.log.Error(err, "failed to translate hub packet")
				continue
			}
			if msg == nil {
				s.log.Info("Subscribe: translated packet is nil (heartbeat or empty), skipping")
				if watcher != nil {
					watcher.Touch()
				}
				continue
			}
			s.recordCapabilitiesFromRequest(msg)
			if !s.gate.AllowDownstream() {
				s.log.V(1).Info("Downstream flow paused; dropping frame", "type", describeFrame(msg))
				s.recordDownstreamDrop("downstream_paused")
				continue
			}
			s.log.Info("Subscribe: sending packet to local engram")
			if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, cancel, "subscribe send", func() error {
				return stream.Send(msg)
			}); err != nil {
				s.log.Error(err, "Subscribe: failed to send to engram")
				return err
			}
			s.bridge.RecordDelivery(packet)
			s.log.Info("Subscribe: packet sent to engram successfully")
			if watcher != nil {
				watcher.Touch()
			}
		}
	}
}

// Control establishes the connector control stream: it reports readiness,
// sends an initial connector.ready directive, then concurrently forwards
// capability updates and consumes directives with transportconnector.CallWithTimeout guarding
// every send/recv until the context is canceled
// (`internal/connector/connector.go:295-369`).
func (s *transportServer) Control(stream transportpb.TransportConnector_ControlServer) error {
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
	if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, cancel, "control ready send", func() error {
		return stream.Send(connectorReadyDirective())
	}); err != nil {
		return err
	}

	updates := s.capabilityUpdates(ctx)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return s.forwardCapabilityUpdates(groupCtx, cancel, stream, updates, watcher)
	})
	group.Go(func() error {
		return s.forwardHandoffUpdates(groupCtx, cancel, stream, watcher)
	})
	group.Go(func() error {
		return s.consumeControlDirectives(groupCtx, cancel, stream, watcher)
	})
	s.emitHandoffSnapshot()

	if err := group.Wait(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

func (s *transportServer) forwardCapabilityUpdates(ctx context.Context, cancel context.CancelFunc, stream transportpb.TransportConnector_ControlServer, updates <-chan capabilityState, watcher *hangWatcher) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case state, ok := <-updates:
			if !ok {
				return nil
			}
			if directive := capabilityStateToDirective(state); directive != nil {
				if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, cancel, "control capability send", func() error {
					return stream.Send(directive)
				}); err != nil {
					return err
				}
				s.recordControlDirective("sent", directive.GetType())
				s.log.V(1).Info("Control: forwarded capability directive", "type", directive.GetType())
			}
		}
	}
}

func (s *transportServer) forwardHandoffUpdates(ctx context.Context, cancel context.CancelFunc, stream transportpb.TransportConnector_ControlServer, watcher *hangWatcher) error {
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
			if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, cancel, "control handoff send", func() error {
				return stream.Send(directive)
			}); err != nil {
				return err
			}
			s.recordControlDirective("sent", directive.GetType())
			s.log.V(1).Info("Control: forwarded handoff directive", "type", directive.GetType())
			if watcher != nil {
				watcher.Touch()
			}
		}
	}
}

func (s *transportServer) consumeControlDirectives(ctx context.Context, cancel context.CancelFunc, stream transportpb.TransportConnector_ControlServer, watcher *hangWatcher) error {
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
			s.recordControlDirective("received", directive.GetType())
			s.log.V(1).Info("Control: received directive", "type", directive.GetType())
			if watcher != nil {
				// Count any control traffic (including heartbeat) as liveness.
				watcher.Touch()
			}
		}
		if resp := s.handleControlDirective(ctx, directive); resp != nil {
			if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, cancel, "control send resp", func() error {
				return stream.Send(resp)
			}); err != nil {
				return err
			}
			s.recordControlDirective("sent", resp.GetType())
			s.log.V(1).Info("Control: sent directive response", "type", resp.GetType())
			if watcher != nil {
				watcher.Touch()
			}
		}
	}
}

func connectorReadyDirective() *transportpb.ControlDirective {
	return &transportpb.ControlDirective{Type: "connector.ready"}
}

func (s *transportServer) capabilityUpdates(ctx context.Context) <-chan capabilityState {
	if s.reporter == nil {
		ch := make(chan capabilityState)
		close(ch)
		return ch
	}
	return s.reporter.WatchCapabilities(ctx)
}

func capabilityStateToDirective(state capabilityState) *transportpb.ControlDirective {
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
	return &transportpb.ControlDirective{
		Type:     "connector.capabilities",
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

func (s *transportServer) handleControlDirective(ctx context.Context, directive *transportpb.ControlDirective) *transportpb.ControlDirective {
	if directive == nil {
		return nil
	}
	typ := normalizeDirectiveType(directive.GetType())
	s.log.Info("Received control directive", "type", typ, "metadata", directive.GetMetadata())
	switch typ {
	case "", "noop":
		return ackDirective("noop", false, map[string]string{"reason": "empty"})
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
			s.reporter.RecordHeartbeat(ctx, directive.GetMetadata())
		}
		return ackDirective(typ, true, map[string]string{"status": "alive"})
	case "codec-select":
		return ackDirective(typ, false, map[string]string{"reason": "codec selection not implemented"})
	default:
		return ackDirective(typ, false, map[string]string{"reason": "unknown"})
	}
}

func (s *transportServer) applyFlowControl(flow *transportpb.FlowControl) {
	if s == nil || flow == nil {
		return
	}
	if flow.GetPause() {
		s.gate.EnableUpstream(false)
		s.log.V(1).Info("Flow control: upstream paused")
	}
	if flow.GetResume() {
		s.gate.EnableUpstream(true)
		s.log.V(1).Info("Flow control: upstream resumed")
	}
}

func ackDirective(received string, handled bool, extra map[string]string) *transportpb.ControlDirective {
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
	return &transportpb.ControlDirective{Type: "ack", Metadata: md}
}

func (s *transportServer) recordCapabilitiesFromRequest(req *transportpb.PublishRequest) {
	if s.reporter == nil || req == nil {
		return
	}
	switch frame := req.GetFrame().(type) {
	case *transportpb.PublishRequest_Audio:
		s.reporter.ObserveAudioFrame(frame.Audio)
	case *transportpb.PublishRequest_Video:
		s.reporter.ObserveVideoFrame(frame.Video)
	case *transportpb.PublishRequest_Binary:
		s.reporter.ObserveBinaryFrame(frame.Binary)
	}
}

func describeFrame(req *transportpb.PublishRequest) string {
	if req == nil {
		return "unknown"
	}
	switch req.GetFrame().(type) {
	case *transportpb.PublishRequest_Audio:
		return "audio"
	case *transportpb.PublishRequest_Video:
		return "video"
	case *transportpb.PublishRequest_Binary:
		return "binary"
	default:
		return "unknown"
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
	size := proto.Size(packet)
	seq := packet.GetEnvelope().GetSequence()
	partition := packet.GetEnvelope().GetPartition()
	now := time.Now()

	t.mu.Lock()
	t.pendingMessages++
	t.pendingBytes += size
	if t.ordering == orderingPerPartition {
		if seq > 0 {
			if t.lastSeqByPartition == nil {
				t.lastSeqByPartition = make(map[string]uint64)
			}
			if seq > t.lastSeqByPartition[partition] {
				t.lastSeqByPartition[partition] = seq
			}
			if t.pendingAcks == nil {
				t.pendingAcks = make(map[string]uint64)
			}
			if seq > t.pendingAcks[partition] {
				t.pendingAcks[partition] = seq
			}
		}
	} else if seq > 0 && seq > t.lastSeq {
		t.lastSeq = seq
	}

	shouldSend := false
	if t.ackEveryMessages > 0 && t.pendingMessages >= t.ackEveryMessages {
		shouldSend = true
	}
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
	b.log.Info("Connecting to hub", "endpoint", b.cfg.HubEndpoint, "step", b.cfg.StepID, "storyRun", b.cfg.StoryRunName)
	conn, err := dialHub(ctx, b.cfg)
	if err != nil {
		b.log.Error(err, "Failed to dial hub", "endpoint", b.cfg.HubEndpoint)
		return nil, nil, nil, err
	}
	md := metadata.Pairs(
		metaStoryRunName, b.cfg.StoryRunName,
		metaStoryRunNS, b.cfg.Namespace,
		metaCurrentStepID, b.cfg.StepID,
		coretransport.ProtocolMetadataKey, coretransport.ProtocolVersion,
	)
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

func (b *hubBridge) reconnectLoop(reason string) {
	defer func() {
		b.reconnectMu.Lock()
		b.reconnecting = false
		b.reconnectMu.Unlock()
	}()
	backoff := time.Second
	for {
		if b.ctx.Err() != nil {
			return
		}
		conn, client, streamCancel, err := b.openHubStream(b.ctx)
		if err != nil {
			b.log.Error(err, "Hub reconnect failed", "reason", reason, "backoff", backoff)
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
	if packet == nil {
		return nil
	}
	if b.ctx.Err() != nil {
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
	if b.ctx.Err() != nil {
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
	return transportconnector.CallWithTimeout(b.ctx, b.messageTimeout, nil, "hub send", func() error {
		b.sendMu.Lock()
		defer b.sendMu.Unlock()
		return client.Send(&transportpb.ProcessRequest{Packet: packet})
	})
}

func (b *hubBridge) sendFlow(client transportpb.HubService_ProcessClient, flow *transportpb.FlowControl) error {
	if client == nil || flow == nil {
		return fmt.Errorf("hub stream unavailable")
	}
	return transportconnector.CallWithTimeout(b.ctx, b.messageTimeout, nil, "hub flow send", func() error {
		b.sendMu.Lock()
		defer b.sendMu.Unlock()
		return client.Send(&transportpb.ProcessRequest{Flow: flow})
	})
}

func (b *hubBridge) enqueueOutbox(packet *transportpb.DataPacket, reason string) {
	if b == nil || packet == nil || b.outboxMax <= 0 {
		return
	}
	b.outboxMu.Lock()
	dropped := 0
	if len(b.outbox) >= b.outboxMax {
		overflow := len(b.outbox) - b.outboxMax + 1
		if overflow < 1 {
			overflow = 1
		}
		b.outbox = b.outbox[overflow:]
		dropped = overflow
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
	if cfg.AllowInsecureHub {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else if creds, err := transportconnector.HubCredentials(transportconnector.OSEnv); err == nil {
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

// isPublishHeartbeat returns true when the SDK sent a keepalive on the
// publish stream (no frame, metadata contains the heartbeat marker).
func isPublishHeartbeat(req *transportpb.PublishRequest) bool {
	return req != nil && req.GetFrame() == nil && req.GetMetadata()["bubu-heartbeat"] == "true"
}

func publishRequestToHubPacket(req *transportpb.PublishRequest) (*transportpb.DataPacket, error) {
	if req == nil {
		return nil, fmt.Errorf("publish request is nil")
	}
	packet := &transportpb.DataPacket{
		Metadata:   cloneStringMap(req.GetMetadata()),
		Payload:    cloneStruct(req.GetPayload()),
		Inputs:     cloneStruct(req.GetInputs()),
		Transports: cloneTransportDescriptors(req.GetTransports()),
		Envelope:   cloneStreamEnvelope(req.GetEnvelope()),
	}
	switch frame := req.GetFrame().(type) {
	case *transportpb.PublishRequest_Audio:
		audioPacket, err := audioFrameToHubPacket(frame.Audio)
		if err != nil {
			return nil, err
		}
		packet.Audio = audioPacket.GetAudio()
		return packet, nil
	case *transportpb.PublishRequest_Video:
		videoPacket, err := videoFrameToHubPacket(frame.Video)
		if err != nil {
			return nil, err
		}
		packet.Video = videoPacket.GetVideo()
		return packet, nil
	case *transportpb.PublishRequest_Binary:
		binary := frame.Binary
		if binary == nil {
			return nil, fmt.Errorf("binary frame missing payload")
		}
		if strings.TrimSpace(binary.GetMimeType()) == envelope.MIMEType {
			envPacket, err := binaryFrameToHubPacket(binary)
			if err != nil {
				return nil, err
			}
			mergeDataPackets(packet, envPacket)
			return packet, nil
		}
		packet.Binary = &transportpb.BinaryFrame{
			Payload:     append([]byte(nil), binary.GetPayload()...),
			MimeType:    binary.GetMimeType(),
			TimestampMs: binary.GetTimestampMs(),
		}
		return packet, nil
	default:
		return nil, fmt.Errorf("unsupported frame type %T", frame)
	}
}

func audioFrameToHubPacket(frame *transportpb.AudioFrame) (*transportpb.DataPacket, error) {
	if frame == nil {
		return nil, fmt.Errorf("audio frame missing payload")
	}
	return &transportpb.DataPacket{
		Audio: &transportpb.AudioFrame{
			Pcm:          append([]byte(nil), frame.GetPcm()...),
			SampleRateHz: frame.GetSampleRateHz(),
			Channels:     frame.GetChannels(),
			Codec:        frame.GetCodec(),
			TimestampMs:  frame.GetTimestampMs(),
		},
	}, nil
}

func videoFrameToHubPacket(frame *transportpb.VideoFrame) (*transportpb.DataPacket, error) {
	if frame == nil {
		return nil, fmt.Errorf("video frame missing payload")
	}
	return &transportpb.DataPacket{
		Video: &transportpb.VideoFrame{
			Payload:     append([]byte(nil), frame.GetPayload()...),
			Codec:       frame.GetCodec(),
			Width:       frame.GetWidth(),
			Height:      frame.GetHeight(),
			TimestampMs: frame.GetTimestampMs(),
			Raw:         frame.GetRaw(),
		},
	}, nil
}

func binaryFrameToHubPacket(frame *transportpb.BinaryFrame) (*transportpb.DataPacket, error) {
	if frame == nil {
		return nil, fmt.Errorf("binary frame missing payload")
	}
	if strings.TrimSpace(frame.GetMimeType()) != envelope.MIMEType {
		return nil, fmt.Errorf("unsupported mime type %s", frame.GetMimeType())
	}
	env, err := envelope.Unmarshal(frame.GetPayload())
	if err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	if env.TimestampMs == 0 && frame.GetTimestampMs() > 0 {
		env.TimestampMs = int64(frame.GetTimestampMs())
	}
	// Envelope is transparent: unpack into DataPacket fields
	// Do NOT keep the original binary frame - it's just a transport wrapper
	packet := &transportpb.DataPacket{
		Metadata:   cloneStringMap(env.Metadata),
		Payload:    rawJSONToStruct(env.Payload),
		Inputs:     rawJSONToStruct(env.Inputs),
		Transports: convertEnvTransports(env.Transports),
	}
	injectEnvelopeHeaders(packet, env)
	return packet, nil
}

func mergeDataPackets(dst, src *transportpb.DataPacket) {
	if dst == nil || src == nil {
		return
	}
	if len(src.Metadata) > 0 {
		if dst.Metadata == nil {
			dst.Metadata = make(map[string]string, len(src.Metadata))
		}
		for k, v := range src.Metadata {
			if _, exists := dst.Metadata[k]; !exists {
				dst.Metadata[k] = v
			}
		}
	}
	if dst.Payload == nil && src.Payload != nil {
		dst.Payload = cloneStruct(src.Payload)
	}
	if dst.Inputs == nil && src.Inputs != nil {
		dst.Inputs = cloneStruct(src.Inputs)
	}
	if len(dst.Transports) == 0 && len(src.Transports) > 0 {
		dst.Transports = cloneTransportDescriptors(src.Transports)
	}
	if dst.Envelope == nil && src.Envelope != nil {
		dst.Envelope = cloneStreamEnvelope(src.Envelope)
	}
}

func hubPacketToPublishRequest(logger logr.Logger, packet *transportpb.DataPacket) (*transportpb.PublishRequest, error) {
	if packet == nil {
		return nil, nil
	}

	req := &transportpb.PublishRequest{
		Metadata:   cloneStringMap(packet.Metadata),
		Payload:    cloneStruct(packet.Payload),
		Inputs:     cloneStruct(packet.Inputs),
		Transports: cloneTransportDescriptors(packet.Transports),
		Envelope:   cloneStreamEnvelope(packet.Envelope),
	}

	// CRITICAL DEBUG: Log exactly what's in the packet
	hasAudio := packet.Audio != nil
	hasVideo := packet.Video != nil
	hasBinary := packet.Binary != nil
	hasPayload := packet.Payload != nil
	hasInputs := packet.Inputs != nil
	audioPcmLen := 0
	if hasAudio {
		audioPcmLen = len(packet.Audio.Pcm)
	}
	logger.Info("[CONNECTOR_TRANSLATE] hubPacketToPublishRequest",
		"hasAudio", hasAudio,
		"audioPcmLen", audioPcmLen,
		"hasVideo", hasVideo,
		"hasBinary", hasBinary,
		"hasPayload", hasPayload,
		"hasInputs", hasInputs)

	if audio := packet.GetAudio(); audio != nil {
		logger.Info("[CONNECTOR_TRANSLATE] Returning AudioFrame", "pcmLen", len(audio.GetPcm()))
		req.Frame = &transportpb.PublishRequest_Audio{
			Audio: &transportpb.AudioFrame{
				Pcm:          append([]byte(nil), audio.GetPcm()...),
				SampleRateHz: audio.GetSampleRateHz(),
				Channels:     audio.GetChannels(),
				Codec:        audio.GetCodec(),
				TimestampMs:  audio.GetTimestampMs(),
			},
		}
		return req, nil
	}

	if video := packet.GetVideo(); video != nil {
		req.Frame = &transportpb.PublishRequest_Video{
			Video: &transportpb.VideoFrame{
				Payload:     append([]byte(nil), video.GetPayload()...),
				Codec:       video.GetCodec(),
				Width:       video.GetWidth(),
				Height:      video.GetHeight(),
				TimestampMs: video.GetTimestampMs(),
				Raw:         video.GetRaw(),
			},
		}
		return req, nil
	}

	logger.Info("[CONNECTOR_TRANSLATE] No Audio/Video found, creating Envelope from Payload/Inputs")
	env := &envelope.Envelope{
		Metadata:   cloneStringMap(packet.Metadata),
		Payload:    structToRawJSON(packet.Payload),
		Inputs:     structToRawJSON(packet.Inputs),
		Transports: convertHubTransports(packet.Transports),
	}
	extractEnvelopeHeaders(env)
	if isEnvelopeEmpty(env) {
		logger.Info("[CONNECTOR_TRANSLATE] Envelope is empty, returning nil")
		return nil, nil
	}
	logger.Info("[CONNECTOR_TRANSLATE] Converting Envelope to BinaryFrame")
	frame, err := envelope.ToBinaryFrame(env)
	if err != nil {
		return nil, err
	}
	if env.TimestampMs > 0 {
		frame.TimestampMs = uint64(env.TimestampMs)
	}
	if bin := packet.GetBinary(); bin != nil {
		frame.TimestampMs = bin.GetTimestampMs()
		if mime := strings.TrimSpace(bin.GetMimeType()); mime != "" {
			frame.MimeType = mime
		}
	}
	req.Frame = &transportpb.PublishRequest_Binary{Binary: frame}
	return req, nil
}

func isEnvelopeEmpty(env *envelope.Envelope) bool {
	return len(env.Metadata) == 0 && len(env.Payload) == 0 && len(env.Inputs) == 0 && len(env.Transports) == 0
}

func injectEnvelopeHeaders(packet *transportpb.DataPacket, env *envelope.Envelope) {
	if env == nil {
		return
	}
	if packet.Metadata == nil {
		packet.Metadata = make(map[string]string, 3)
	}
	if strings.TrimSpace(env.Kind) != "" {
		packet.Metadata[metadataEnvelopeKindKey] = env.Kind
	}
	if strings.TrimSpace(env.MessageID) != "" {
		packet.Metadata[metadataEnvelopeMessageIDKey] = env.MessageID
	}
	if env.TimestampMs > 0 {
		packet.Metadata[metadataEnvelopeTimeKey] = strconv.FormatInt(env.TimestampMs, 10)
	}
}

func extractEnvelopeHeaders(env *envelope.Envelope) {
	if env == nil || len(env.Metadata) == 0 {
		return
	}
	if kind, ok := env.Metadata[metadataEnvelopeKindKey]; ok {
		env.Kind = kind
		delete(env.Metadata, metadataEnvelopeKindKey)
	}
	if messageID, ok := env.Metadata[metadataEnvelopeMessageIDKey]; ok {
		env.MessageID = messageID
		delete(env.Metadata, metadataEnvelopeMessageIDKey)
	}
	if ts, ok := env.Metadata[metadataEnvelopeTimeKey]; ok {
		if parsed, err := strconv.ParseInt(ts, 10, 64); err == nil {
			env.TimestampMs = parsed
		}
		delete(env.Metadata, metadataEnvelopeTimeKey)
	}
}

func convertEnvTransports(src []envelope.TransportDescriptor) []*transportpb.TransportDescriptor {
	if len(src) == 0 {
		return nil
	}
	out := make([]*transportpb.TransportDescriptor, len(src))
	for i := range src {
		td := src[i]
		out[i] = &transportpb.TransportDescriptor{
			Name:   td.Name,
			Kind:   td.Kind,
			Mode:   td.Mode,
			Config: convertConfigMap(td.Config),
		}
	}
	return out
}

func convertHubTransports(src []*transportpb.TransportDescriptor) []envelope.TransportDescriptor {
	if len(src) == 0 {
		return nil
	}
	out := make([]envelope.TransportDescriptor, len(src))
	for i := range src {
		td := src[i]
		out[i] = envelope.TransportDescriptor{
			Name:   td.GetName(),
			Kind:   td.GetKind(),
			Mode:   td.GetMode(),
			Config: structToNative(td.GetConfig()),
		}
	}
	return out
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func rawJSONToStruct(data []byte) *structpb.Struct {
	if len(data) == 0 {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil
	}
	st, err := structpb.NewStruct(payload)
	if err != nil {
		return nil
	}
	return st
}

func structToRawJSON(st *structpb.Struct) []byte {
	if st == nil {
		return nil
	}
	bytes, err := st.MarshalJSON()
	if err != nil {
		return nil
	}
	return bytes
}

func convertConfigMap(native map[string]any) *structpb.Struct {
	if len(native) == 0 {
		return nil
	}
	out, err := structpb.NewStruct(native)
	if err != nil {
		return nil
	}
	return out
}

func structToNative(st *structpb.Struct) map[string]any {
	if st == nil {
		return nil
	}
	return st.AsMap()
}

func cloneStruct(st *structpb.Struct) *structpb.Struct {
	if st == nil {
		return nil
	}
	clone, ok := proto.Clone(st).(*structpb.Struct)
	if !ok {
		return nil
	}
	return clone
}

func cloneTransportDescriptors(src []*transportpb.TransportDescriptor) []*transportpb.TransportDescriptor {
	if len(src) == 0 {
		return nil
	}
	out := make([]*transportpb.TransportDescriptor, 0, len(src))
	for _, td := range src {
		if td == nil {
			continue
		}
		clone, ok := proto.Clone(td).(*transportpb.TransportDescriptor)
		if !ok {
			continue
		}
		out = append(out, clone)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneStreamEnvelope(src *transportpb.StreamEnvelope) *transportpb.StreamEnvelope {
	if src == nil {
		return nil
	}
	return &transportpb.StreamEnvelope{
		StreamId:   src.GetStreamId(),
		Sequence:   src.GetSequence(),
		Partition:  src.GetPartition(),
		ChunkId:    src.GetChunkId(),
		ChunkIndex: src.GetChunkIndex(),
		ChunkCount: src.GetChunkCount(),
		ChunkBytes: src.GetChunkBytes(),
		TotalBytes: src.GetTotalBytes(),
	}
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
