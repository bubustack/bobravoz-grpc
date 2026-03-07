package connector

import (
	"context"
	"fmt"
	"io"
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
	log    logr.Logger
	bridge *hubBridge
	flow   *flowTracker
	sendMu sync.Mutex
}

// newP2PServer creates a P2P server that forwards packets to the local engram.
func newP2PServer(log logr.Logger, bridge *hubBridge) *p2pServer {
	var settingsPayload []byte
	if bridge != nil && bridge.cfg != nil && bridge.cfg.Binding.Info != nil {
		settingsPayload = bridge.cfg.Binding.Info.GetPayload()
	}
	return &p2pServer{
		log:    log.WithName("p2p-server"),
		bridge: bridge,
		flow:   newFlowTracker(log.WithName("p2p-flow"), settingsPayload),
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
				"hasAudio", req.Packet.Audio != nil,
				"hasPayload", req.Packet.Payload != nil,
				"metadataKeys", len(req.Packet.Metadata))

			if err := s.forwardPacket(ctx, stream, req.Packet); err != nil {
				s.log.Info("P2P forwarding stopped", "error", err)
				errCh <- err
				return
			}
		}
	}()

	// Goroutine 2: Send periodic heartbeats to upstream to keep connection alive
	// The upstream connector's readLoop waits for responses with a 30s timeout.
	// Without heartbeats, the connection times out and closes, causing reconnect loops.
	// Send heartbeats every 10s to match the hub's heartbeat interval.
	go func() {
		heartbeatInterval := 10 * time.Second
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

	// Wait for either goroutine to complete
	err := <-errCh
	s.log.Info("P2P connection closed", "error", err)
	return err
}

func (s *p2pServer) recordDelivery(stream transportpb.HubService_ProcessServer, packet *transportpb.DataPacket) {
	if s == nil || s.flow == nil || packet == nil {
		return
	}
	flow := s.flow.recordDelivery(packet)
	if flow == nil {
		return
	}
	if err := s.sendResponse(stream, &transportpb.ProcessResponse{Flow: flow}); err != nil {
		s.log.V(1).Info("P2P failed to send flow control update", "error", err)
	}
}

func (s *p2pServer) forwardPacket(
	ctx context.Context,
	stream transportpb.HubService_ProcessServer,
	packet *transportpb.DataPacket,
) (err error) {
	if s == nil || s.bridge == nil || packet == nil {
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
		s.recordDelivery(stream, packet)
		return nil
	case <-ctx.Done():
		s.log.Info("P2P context cancelled while forwarding to engram")
		return ctx.Err()
	case <-s.bridge.ctx.Done():
		s.log.Info("P2P bridge closed while forwarding to engram")
		return s.bridge.ctx.Err()
	}
}

func (s *p2pServer) sendResponse(stream transportpb.HubService_ProcessServer, resp *transportpb.ProcessResponse) error {
	if s == nil || stream == nil || resp == nil {
		return nil
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return stream.Send(resp)
}
