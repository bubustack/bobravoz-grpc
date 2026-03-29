package connector

import (
	"context"
	"io"
	"testing"
	"time"

	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

type fakeProcessStream struct {
	ctx    context.Context
	recvCh chan *transportpb.ProcessRequest
	sendCh chan *transportpb.ProcessResponse
}

func newFakeProcessStream(ctx context.Context) *fakeProcessStream {
	return &fakeProcessStream{
		ctx:    ctx,
		recvCh: make(chan *transportpb.ProcessRequest, 4),
		sendCh: make(chan *transportpb.ProcessResponse, 4),
	}
}

func (f *fakeProcessStream) Send(resp *transportpb.ProcessResponse) error {
	f.sendCh <- resp
	return nil
}

func (f *fakeProcessStream) Recv() (*transportpb.ProcessRequest, error) {
	req, ok := <-f.recvCh
	if !ok {
		return nil, io.EOF
	}
	return req, nil
}

func (f *fakeProcessStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeProcessStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeProcessStream) SetTrailer(metadata.MD)       {}
func (f *fakeProcessStream) Context() context.Context     { return f.ctx }
func (f *fakeProcessStream) SendMsg(any) error            { return nil }
func (f *fakeProcessStream) RecvMsg(any) error            { return nil }

func TestP2PServerSendsFlowControlAckAfterReceipt(t *testing.T) {
	md := metadata.New(map[string]string{
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
	})
	ctx, cancel := context.WithCancel(metadata.NewIncomingContext(context.Background(), md))
	defer cancel()

	settings := []byte(`{"delivery":{"ordering":"per_stream"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	bridge := &hubBridge{
		cfg: &Config{
			Binding: coretransport.BindingPayload{
				Info: &transportpb.BindingInfo{Payload: settings},
			},
		},
		recvCh: make(chan *transportpb.DataPacket, 1),
		ctx:    ctx,
	}

	server := newP2PServer(logr.Discard(), bridge)
	stream := newFakeProcessStream(ctx)

	done := make(chan error, 1)
	go func() {
		done <- server.Process(stream)
	}()

	packet := &transportpb.DataPacket{Envelope: &transportpb.StreamEnvelope{StreamId: "downstream-step", Sequence: 5}}
	stream.recvCh <- &transportpb.ProcessRequest{Packet: packet}

	select {
	case <-bridge.recvCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected packet to be forwarded to bridge")
	}

	select {
	case resp := <-stream.sendCh:
		t.Fatalf("unexpected flow control response before receipt: %+v", resp)
	default:
	}

	require.True(t, server.recordReceipt("downstream-step", 5, "", 64))

	var flowResp *transportpb.ProcessResponse
	deadline := time.After(2 * time.Second)
	for flowResp == nil {
		select {
		case resp := <-stream.sendCh:
			if resp != nil && resp.GetFlow() != nil {
				flowResp = resp
			}
		case <-deadline:
			t.Fatalf("expected flow control response")
		}
	}

	require.Equal(t, uint64(5), flowResp.GetFlow().GetAck())

	close(stream.recvCh)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected p2p server to exit after stream closed")
	}
}
