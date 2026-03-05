package connector

import (
	"io"
	"time"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/structpb"
)

// p2pServer implements HubService for accepting P2P connections from upstream connectors.
// It forwards incoming packets to the local engram via the same mechanism as hub packets.
type p2pServer struct {
	transportpb.UnimplementedHubServiceServer
	log    logr.Logger
	bridge *hubBridge
}

// newP2PServer creates a P2P server that forwards packets to the local engram.
func newP2PServer(log logr.Logger, bridge *hubBridge) *p2pServer {
	return &p2pServer{
		log:    log.WithName("p2p-server"),
		bridge: bridge,
	}
}

// Process implements the bidirectional streaming RPC for P2P connections.
// Upstream connectors connect to this server instead of the hub.
func (s *p2pServer) Process(stream transportpb.HubService_ProcessServer) error {
	// Extract metadata from incoming connection
	ctx := stream.Context()

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

			if req.Packet == nil {
				s.log.V(1).Info("P2P received nil packet from upstream, skipping")
				continue
			}

			s.log.V(2).Info("P2P received packet from upstream",
				"hasAudio", req.Packet.Audio != nil,
				"hasPayload", req.Packet.Payload != nil,
				"metadataKeys", len(req.Packet.Metadata))

			// Forward packet to local engram via the bridge's receive channel
			// This is the same mechanism used for hub packets
			select {
			case s.bridge.recvCh <- req.Packet:
				s.log.V(2).Info("P2P packet forwarded to local engram")
			case <-ctx.Done():
				s.log.Info("P2P context cancelled while forwarding to engram")
				errCh <- ctx.Err()
				return
			case <-s.bridge.ctx.Done():
				s.log.Info("P2P bridge closed while forwarding to engram")
				errCh <- s.bridge.ctx.Err()
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
				// Send heartbeat packet to upstream
				heartbeat := &transportpb.ProcessResponse{
					Packet: &transportpb.DataPacket{
						Metadata: map[string]string{"bubu-heartbeat": "true"},
						Payload:  &structpb.Struct{},
					},
				}
				if err := stream.Send(heartbeat); err != nil {
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
