package connector

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// p2pServer implements HubService for accepting P2P connections from upstream connectors.
// It forwards incoming packets to the local engram via the same mechanism as hub packets.
type p2pServer struct {
	transportpb.UnimplementedHubServiceServer
	log             logr.Logger
	bridge          *hubBridge
	settingsPayload []byte
	sendMu          sync.Mutex
	routesMu        sync.RWMutex
	routes          map[string]*p2pSession
}

type p2pSession struct {
	stream    transportpb.HubService_ProcessServer
	flow      *flowTracker
	streamIDs map[string]struct{}
}

// newP2PServer creates a P2P server that forwards packets to the local engram.
func newP2PServer(log logr.Logger, bridge *hubBridge) *p2pServer {
	var settingsPayload []byte
	if bridge != nil && bridge.cfg != nil && bridge.cfg.Binding.Info != nil {
		settingsPayload = bridge.cfg.Binding.Info.GetPayload()
	}
	return &p2pServer{
		log:             log.WithName("p2p-server"),
		bridge:          bridge,
		settingsPayload: settingsPayload,
		routes:          make(map[string]*p2pSession),
	}
}

// Process implements the bidirectional streaming RPC for P2P connections.
// Upstream connectors connect to this server instead of the hub.
func (s *p2pServer) Process(stream transportpb.HubService_ProcessServer) error {
	// Extract metadata from incoming connection
	ctx := stream.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.FailedPrecondition, "p2p stream missing metadata")
	}
	if err := coretransport.ValidateProtocolVersion(metadataValue(md, coretransport.ProtocolMetadataKey)); err != nil {
		s.log.Error(err, "P2P stream rejected due to protocol version")
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	s.log.Info("P2P connection established from upstream")
	session := s.newSession(stream)
	defer s.unregisterSession(session)

	// Create a goroutine to read from upstream and forward to local engram
	errCh := make(chan error, 2)

	// Goroutine 1: Read from upstream connector and forward to local engram
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					s.log.Info("P2P upstream closed connection")
					errCh <- nil
					return
				}
				s.log.Error(err, "P2P recv from upstream failed")
				errCh <- err
				return
			}

			if flow := req.GetFlow(); flow != nil {
				s.log.V(1).Info("P2P received upstream flow control; ignoring")
			}

			if req.Packet == nil {
				s.log.V(1).Info("P2P received nil packet from upstream, skipping")
				continue
			}

			s.log.V(2).Info("P2P received packet from upstream",
				"hasAudio", req.Packet.GetAudio() != nil,
				"hasPayload", req.Packet.Payload != nil,
				"metadataKeys", len(req.Packet.Metadata))

			if err := s.forwardPacket(ctx, session, req.Packet); err != nil {
				s.log.Info("P2P forwarding stopped", "error", err)
				errCh <- err
				return
			}
		}
	}()

	// Goroutine 2: Send periodic heartbeats to upstream to keep connection alive.
	heartbeatInterval := p2pHeartbeatInterval()
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		s.log.Info("P2P heartbeat sender started", "interval", heartbeatInterval)

		for {
			select {
			case <-ctx.Done():
				s.log.Info("P2P heartbeat sender stopped", "reason", ctx.Err())
				errCh <- ctx.Err()
				return
			case <-ticker.C:
				// Send heartbeat as FlowControl (not DataPacket) so the upstream
				// connector's readLoop routes it via handleFlow and never enqueues
				// it as a data packet visible to the engram SDK.
				heartbeat := &transportpb.ProcessResponse{
					Flow: &transportpb.FlowControl{},
				}
				if err := s.sendResponse(stream, heartbeat); err != nil {
					s.log.Error(err, "P2P failed to send heartbeat to upstream")
					errCh <- err
					return
				}
				s.log.V(2).Info("P2P heartbeat sent to upstream")
			}
		}
	}()

	// Wait for the first goroutine to complete, then let context cancellation
	// clean up the other. Drain the second error to avoid goroutine leak.
	err := <-errCh
	s.log.Info("P2P connection closed", "error", err)
	// Drain second goroutine result (context cancellation will cause it to exit).
	select {
	case <-errCh:
	default:
		// Second goroutine hasn't finished yet; it will exit when ctx is cancelled.
	}
	return err
}

func (s *p2pServer) newSession(stream transportpb.HubService_ProcessServer) *p2pSession {
	if s == nil || stream == nil {
		return nil
	}
	return &p2pSession{
		stream:    stream,
		flow:      newFlowTracker(s.log.WithName("p2p-flow"), s.settingsPayload),
		streamIDs: make(map[string]struct{}),
	}
}

func (s *p2pServer) registerPacket(session *p2pSession, packet *transportpb.DataPacket) {
	if s == nil || session == nil || packet == nil || packet.GetEnvelope() == nil {
		return
	}
	streamID := strings.TrimSpace(packet.GetEnvelope().GetStreamId())
	if streamID == "" {
		return
	}
	s.routesMu.Lock()
	if s.routes == nil {
		s.routes = make(map[string]*p2pSession)
	}
	if old, exists := s.routes[streamID]; exists && old != session {
		s.log.Info("P2P duplicate streamID registered; replacing previous route",
			"streamID", streamID)
		// Remove the streamID from the old session's tracking set so that
		// unregisterSession does not accidentally remove the new route.
		delete(old.streamIDs, streamID)
	}
	s.routes[streamID] = session
	session.streamIDs[streamID] = struct{}{}
	s.routesMu.Unlock()
}

func (s *p2pServer) unregisterSession(session *p2pSession) {
	if s == nil || session == nil {
		return
	}
	s.routesMu.Lock()
	for streamID := range session.streamIDs {
		if s.routes[streamID] == session {
			delete(s.routes, streamID)
		}
	}
	s.routesMu.Unlock()
}

func (s *p2pServer) recordReceipt(streamID string, seq uint64, partition string, size int) bool {
	if s == nil || strings.TrimSpace(streamID) == "" {
		return false
	}
	s.routesMu.RLock()
	session := s.routes[strings.TrimSpace(streamID)]
	s.routesMu.RUnlock()
	if session == nil || session.flow == nil {
		return false
	}
	session.recordReceipt(s, seq, partition, size)
	return true
}

func (session *p2pSession) recordDelivery(server *p2pServer, packet *transportpb.DataPacket) {
	if session == nil || server == nil || session.flow == nil || packet == nil {
		return
	}
	flow := session.flow.recordDelivery(packet)
	if flow == nil {
		return
	}
	if err := server.sendResponse(session.stream, &transportpb.ProcessResponse{Flow: flow}); err != nil {
		server.log.V(1).Info("P2P failed to send flow control update", "error", err)
	}
}

func (session *p2pSession) recordReceipt(server *p2pServer, seq uint64, partition string, size int) {
	if session == nil || server == nil || session.flow == nil {
		return
	}
	flow := session.flow.recordReceipt(size, seq, partition)
	if flow == nil {
		return
	}
	if err := server.sendResponse(session.stream, &transportpb.ProcessResponse{Flow: flow}); err != nil {
		server.log.V(1).Info("P2P failed to send flow control update", "error", err)
	}
}

func (s *p2pServer) forwardPacket(
	ctx context.Context,
	session *p2pSession,
	packet *transportpb.DataPacket,
) (err error) {
	if s == nil || s.bridge == nil || session == nil || packet == nil {
		return fmt.Errorf("p2p bridge unavailable")
	}
	defer func() {
		if r := recover(); r != nil {
			// Channel closed while attempting to forward; treat as graceful shutdown.
			err = fmt.Errorf("p2p bridge recv channel closed")
		}
	}()

	// Forward packet to local engram via the bridge's receive channel.
	// This is the same mechanism used for hub packets.
	select {
	case s.bridge.recvCh <- packet:
		s.log.V(2).Info("P2P packet forwarded to local engram")
		s.registerPacket(session, packet)
		if shouldAcknowledgeDownstreamImmediately(packet) {
			session.recordDelivery(s, packet)
		}
		return nil
	case <-ctx.Done():
		s.log.Info("P2P context cancelled while forwarding to engram")
		return ctx.Err()
	case <-s.bridge.ctx.Done():
		s.log.Info("P2P bridge closed while forwarding to engram")
		return s.bridge.ctx.Err()
	}
}

// p2pHeartbeatInterval returns the P2P heartbeat interval, optionally overridden
// via the BOBRAVOZ_P2P_HEARTBEAT_INTERVAL environment variable.
func p2pHeartbeatInterval() time.Duration {
	const defaultInterval = 10 * time.Second
	if v := os.Getenv("BOBRAVOZ_P2P_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultInterval
}

func (s *p2pServer) sendResponse(stream transportpb.HubService_ProcessServer, resp *transportpb.ProcessResponse) error {
	if s == nil || stream == nil || resp == nil {
		return nil
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return stream.Send(resp)
}
