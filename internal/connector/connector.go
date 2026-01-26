package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bubustack/bobravoz-grpc/internal/telemetry"
	"github.com/bubustack/bobravoz-grpc/pkg/metrics"
	transportconnector "github.com/bubustack/core/runtime/transport/connector"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
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
)

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
	listener, err := transportconnector.ListenLocalEndpoint(r.cfg.LocalEndpoint)
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
	transportpb.RegisterTransportConnectorServer(server, newTransportServer(ctx, r.cfg, r.log, bridge, tunables))

	errCh := make(chan error, 1)
	go func() {
		r.log.Info("Transport connector listening", "endpoint", r.cfg.LocalEndpoint)
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
	ctx        context.Context
	cfg        *Config
	log        logr.Logger
	bridge     *hubBridge
	reporter   *bindingStatusReporter
	reportOnce sync.Once
	gate       *mediaGate
	tunables   runtimeTunables
}

func newTransportServer(ctx context.Context, cfg *Config, log logr.Logger, bridge *hubBridge, tunables runtimeTunables) *transportServer {
	return &transportServer{
		ctx:      ctx,
		cfg:      cfg,
		log:      log,
		bridge:   bridge,
		reporter: newBindingStatusReporter(cfg, log),
		gate:     newMediaGate(),
		tunables: tunables,
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
	s.reportReady(ctx)
	for {
		req, err := transportconnector.RecvWithTimeout(ctx, s.tunables.MessageTimeout, cancel, "publish recv", stream.Recv)
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
	var watcher *hangWatcher
	if s.tunables.HangTimeout > 0 {
		watcher = newHangWatcher(s.tunables.HangTimeout, cancel, s.log.WithName("control-hang"))
		defer watcher.Stop()
	}
	s.reportReady(ctx)
	if err := transportconnector.CallWithTimeout(ctx, s.tunables.MessageTimeout, cancel, "control ready send", func() error {
		return stream.Send(connectorReadyDirective())
	}); err != nil {
		return err
	}
	if watcher != nil {
		watcher.Touch()
	}

	updates := s.capabilityUpdates(ctx)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return s.forwardCapabilityUpdates(groupCtx, cancel, stream, updates, watcher)
	})
	group.Go(func() error {
		return s.consumeControlDirectives(groupCtx, cancel, stream, watcher)
	})

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
				if watcher != nil {
					watcher.Touch()
				}
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

		directive, err := transportconnector.RecvWithTimeout(ctx, s.tunables.MessageTimeout, cancel, "control recv", stream.Recv)
		if err != nil {
			if err == io.EOF || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if directive != nil {
			s.recordControlDirective("received", directive.GetType())
			s.log.V(1).Info("Control: received directive", "type", directive.GetType())
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

type hubBridge struct {
	cfg *Config
	log logr.Logger

	conn   *grpc.ClientConn
	client transportpb.HubService_ProcessClient

	sendMu sync.Mutex
	recvCh chan *transportpb.DataPacket
	ctx    context.Context
	cancel context.CancelFunc

	messageTimeout     time.Duration
	channelSendTimeout time.Duration
	watcher            *hangWatcher
}

func newHubBridge(ctx context.Context, cfg *Config, log logr.Logger, tunables runtimeTunables) (*hubBridge, error) {
	log.Info("Connecting to hub", "endpoint", cfg.HubEndpoint, "step", cfg.StepID, "storyRun", cfg.StoryRunName)
	conn, err := dialHub(ctx, cfg)
	if err != nil {
		log.Error(err, "Failed to dial hub", "endpoint", cfg.HubEndpoint)
		return nil, err
	}
	md := metadata.Pairs(
		metaStoryRunName, cfg.StoryRunName,
		metaStoryRunNS, cfg.Namespace,
		metaCurrentStepID, cfg.StepID,
	)
	streamCtx, streamCancel := context.WithCancel(ctx)
	processCtx := metadata.NewOutgoingContext(streamCtx, md)
	client, err := transportpb.NewHubServiceClient(conn).Process(processCtx)
	if err != nil {
		log.Error(err, "Failed to create hub stream", "endpoint", cfg.HubEndpoint)
		streamCancel()
		_ = conn.Close()
		return nil, fmt.Errorf("connecting to hub: %w", err)
	}

	log.Info("Successfully connected to hub and registered stream", "step", cfg.StepID, "storyRun", cfg.StoryRunName)

	bridgeCtx, bridgeCancel := context.WithCancel(ctx)
	cancelAll := func() {
		streamCancel()
		bridgeCancel()
	}
	hb := &hubBridge{
		cfg:                cfg,
		log:                log,
		conn:               conn,
		client:             client,
		recvCh:             make(chan *transportpb.DataPacket, tunables.ChannelBufferSize),
		ctx:                bridgeCtx,
		cancel:             cancelAll,
		messageTimeout:     tunables.MessageTimeout,
		channelSendTimeout: tunables.ChannelSendTimeout,
	}
	if tunables.HangTimeout > 0 {
		hb.watcher = newHangWatcher(tunables.HangTimeout, cancelAll, log.WithName("hub-bridge"))
	}
	go hb.readLoop()
	log.Info("Hub bridge read loop started", "step", cfg.StepID)
	return hb, nil
}

func (b *hubBridge) readLoop() {
	defer close(b.recvCh)
	b.log.Info("Hub bridge readLoop started, waiting for packets from hub")
	for {
		response, err := transportconnector.RecvWithTimeout(b.ctx, b.messageTimeout, b.cancel, "hub recv", b.client.Recv)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				b.log.Info("Hub bridge readLoop exiting", "reason", err.Error())
				return
			}
			b.log.Error(err, "hub stream recv failed")
			return
		}
		if response == nil {
			b.log.V(1).Info("Received nil response from hub")
			continue
		}
		packet := response.GetPacket()
		if packet == nil {
			b.log.V(1).Info("Received response with nil packet from hub")
			continue
		}
		b.log.Info("Received packet from hub, enqueueing for local engram", "metadataKeys", len(packet.Metadata))
		if err := b.enqueuePacket(packet); err != nil {
			b.log.Error(err, "failed to enqueue hub packet")
			return
		}
		b.touch()
	}
}

func (b *hubBridge) Send(packet *transportpb.DataPacket) error {
	if packet == nil {
		return nil
	}
	err := transportconnector.CallWithTimeout(b.ctx, b.messageTimeout, b.cancel, "hub send", func() error {
		b.sendMu.Lock()
		defer b.sendMu.Unlock()
		return b.client.Send(&transportpb.ProcessRequest{Packet: packet})
	})
	if err != nil {
		return err
	}
	b.touch()
	return nil
}

func (b *hubBridge) Recv() <-chan *transportpb.DataPacket {
	return b.recvCh
}

func (b *hubBridge) Close() {
	if b.watcher != nil {
		b.watcher.Stop()
	}
	b.cancel()
	_ = b.client.CloseSend()
	_ = b.conn.Close()
}

func (b *hubBridge) enqueuePacket(packet *transportpb.DataPacket) error {
	if packet == nil {
		return nil
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
		return fmt.Errorf("hub channel send timed out after %s", b.channelSendTimeout)
	}
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

func publishRequestToHubPacket(req *transportpb.PublishRequest) (*transportpb.DataPacket, error) {
	if req == nil {
		return nil, fmt.Errorf("publish request is nil")
	}
	packet := &transportpb.DataPacket{
		Metadata:   cloneStringMap(req.GetMetadata()),
		Payload:    cloneStruct(req.GetPayload()),
		Inputs:     cloneStruct(req.GetInputs()),
		Transports: cloneTransportDescriptors(req.GetTransports()),
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
