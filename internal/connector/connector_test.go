package connector

import (
	"context"
	"io"
	"testing"
	"time"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestTransportServerReportReadyRetriesAfterBindingAppears(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	key := types.NamespacedName{Name: "binding", Namespace: "default"}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&transportv1alpha1.TransportBinding{}).Build()
	reporter := newFakeReporter(client, key, &transportpb.BindingInfo{}, logr.Discard())

	ctx := t.Context()

	server := &transportServer{
		ctx:      ctx,
		log:      logr.Discard(),
		reporter: reporter,
	}

	server.reportReady(ctx)

	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
	}
	require.NoError(t, client.Create(ctx, binding))

	server.reportReady(ctx)

	require.Eventually(t, func() bool {
		var updated transportv1alpha1.TransportBinding
		if err := client.Get(ctx, key, &updated); err != nil {
			return false
		}
		return conditions.GetCondition(updated.Status.Conditions, conditions.ConditionReady) != nil
	}, time.Second, 10*time.Millisecond)
}

func TestHubBridgeOpenHubStreamRejectsMissingGeneration(t *testing.T) {
	bridge := &hubBridge{
		cfg: &Config{
			HubEndpoint:  "hub:7443",
			StoryRunName: "storyrun-1",
			Namespace:    "default",
			StepID:       "step-a",
		},
		log: logr.Discard(),
	}

	_, _, _, err := bridge.openHubStream(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), contracts.ConnectorGenerationEnv)
}

func TestHubStreamMetadataIncludesStartupCapabilitiesMode(t *testing.T) {
	md := hubStreamMetadata(&Config{
		StoryRunName: "storyrun-1",
		Namespace:    "default",
		StepID:       "step-a",
		Generation:   7,
		Binding: coretransport.BindingPayload{
			Info: &transportpb.BindingInfo{
				AudioCodecs: []string{"pcm16"},
			},
		},
	})

	require.Equal(t, "storyrun-1", md.Get(metaStoryRunName)[0])
	require.Equal(t, coretransport.ProtocolVersion, md.Get(coretransport.ProtocolMetadataKey)[0])
	require.Equal(t, coretransport.StartupCapabilitiesRequired, md.Get(coretransport.StartupCapabilitiesMetadataKey)[0])
}

func TestHubStreamMetadataDefaultsStartupCapabilitiesToNone(t *testing.T) {
	md := hubStreamMetadata(&Config{
		StoryRunName: "storyrun-1",
		Namespace:    "default",
		StepID:       "step-a",
		Generation:   7,
	})

	require.Equal(t, coretransport.StartupCapabilitiesNone, md.Get(coretransport.StartupCapabilitiesMetadataKey)[0])
}

func TestPacketToDataResponseEncodesStructuredHookPacket(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{
		"type": "storyrun.ready",
		"hook": map[string]any{"event": "storyrun.ready"},
	})
	require.NoError(t, err)
	inputs, err := structpb.NewStruct(map[string]any{"userPrompt": "say hi"})
	require.NoError(t, err)

	resp := packetToDataResponse(&transportpb.DataPacket{
		Metadata: map[string]string{
			"kind":                       envelope.KindHook,
			metadataEnvelopeMessageIDKey: "ns/storyrun:storyrun.ready",
			"storyrun-name":              "storyrun",
		},
		Payload: payload,
		Inputs:  inputs,
	})
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetBinary())
	require.Equal(t, envelope.MIMEType, resp.GetBinary().GetMimeType())
	require.Equal(t, "ns/storyrun:storyrun.ready", resp.GetMetadata()[metadataEnvelopeMessageIDKey])
	require.NotNil(t, resp.GetFrame())
}

func TestTransportServerHandlesDownstreamDeliveryReceipt(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_stream"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	bridge := &hubBridge{
		ctx:  context.Background(),
		flow: newFlowTracker(logr.Discard(), settings),
		log:  logr.Discard(),
	}
	server := &transportServer{
		log:    logr.Discard(),
		bridge: bridge,
		gate:   newMediaGate(),
	}

	resp := server.handleControlDirective(context.Background(), &transportpb.ControlRequest{
		CustomAction: downstreamDeliveryDirective,
		Metadata: map[string]string{
			deliveryReceiptSequenceKey:  "11",
			deliveryReceiptSizeBytesKey: "64",
		},
	})
	require.Nil(t, resp)

	bridge.flow.mu.Lock()
	defer bridge.flow.mu.Unlock()
	require.Equal(t, uint64(11), bridge.flow.lastSeq)
	require.False(t, bridge.flow.lastSend.IsZero())
}

func TestTransportServerRoutesDownstreamDeliveryReceiptToP2P(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_stream"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	bridge := &hubBridge{
		ctx: context.Background(),
		log: logr.Discard(),
	}
	p2p := &p2pServer{
		log:             logr.Discard(),
		bridge:          bridge,
		settingsPayload: settings,
		routes:          make(map[string]*p2pSession),
	}
	stream := newFakeProcessStream(context.Background())
	session := p2p.newSession(stream)
	p2p.registerPacket(session, &transportpb.DataPacket{
		Envelope: &transportpb.StreamEnvelope{StreamId: "downstream-step", Sequence: 11},
	})

	server := &transportServer{
		log:    logr.Discard(),
		bridge: bridge,
		gate:   newMediaGate(),
		p2p:    p2p,
	}

	resp := server.handleControlDirective(context.Background(), &transportpb.ControlRequest{
		CustomAction: downstreamDeliveryDirective,
		Metadata: map[string]string{
			deliveryReceiptStreamIDKey:  "downstream-step",
			deliveryReceiptSequenceKey:  "11",
			deliveryReceiptSizeBytesKey: "64",
		},
	})
	require.Nil(t, resp)

	select {
	case flowResp := <-stream.sendCh:
		require.Equal(t, uint64(11), flowResp.GetFlow().GetAck())
	case <-time.After(time.Second):
		t.Fatal("expected flow control response")
	}
}

type blockingDataStream struct {
	ctx         context.Context
	sendStarted chan struct{}
	releaseSend chan struct{}
}

func newBlockingDataStream(ctx context.Context) *blockingDataStream {
	return &blockingDataStream{
		ctx:         ctx,
		sendStarted: make(chan struct{}, 1),
		releaseSend: make(chan struct{}),
	}
}

func (f *blockingDataStream) Send(*transportpb.DataResponse) error {
	select {
	case f.sendStarted <- struct{}{}:
	default:
	}
	<-f.releaseSend
	return nil
}

func (f *blockingDataStream) Recv() (*transportpb.DataRequest, error) {
	return nil, io.EOF
}

func (f *blockingDataStream) SetHeader(metadata.MD) error  { return nil }
func (f *blockingDataStream) SendHeader(metadata.MD) error { return nil }
func (f *blockingDataStream) SetTrailer(metadata.MD)       {}
func (f *blockingDataStream) Context() context.Context     { return f.ctx }
func (f *blockingDataStream) SendMsg(any) error            { return nil }
func (f *blockingDataStream) RecvMsg(any) error            { return nil }

func TestTransportServerDataSendLoopReturnsOnBlockedSendTimeout(t *testing.T) {
	ctx := t.Context()

	stream := newBlockingDataStream(ctx)
	bridge := &hubBridge{
		ctx:    ctx,
		recvCh: make(chan *transportpb.DataPacket, 1),
		log:    logr.Discard(),
	}
	server := &transportServer{
		log:      logr.Discard(),
		bridge:   bridge,
		gate:     newMediaGate(),
		tunables: runtimeTunables{MessageTimeout: 10 * time.Millisecond},
	}

	done := make(chan error, 1)
	go func() {
		done <- server.dataSendLoop(ctx, stream, nil)
	}()

	bridge.recvCh <- &transportpb.DataPacket{
		Envelope: &transportpb.StreamEnvelope{Sequence: 1},
	}

	select {
	case <-stream.sendStarted:
	case <-time.After(time.Second):
		t.Fatal("expected data send loop to attempt stream.Send")
	}

	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Contains(t, err.Error(), "timed out")
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected data send loop to return on timeout")
	}

	close(stream.releaseSend)
}

type fakeControlServerStream struct {
	ctx    context.Context
	sendCh chan *transportpb.ControlResponse
	recvCh chan *transportpb.ControlRequest
}

func newFakeControlServerStream(ctx context.Context) *fakeControlServerStream {
	return &fakeControlServerStream{
		ctx:    ctx,
		sendCh: make(chan *transportpb.ControlResponse, 8),
		recvCh: make(chan *transportpb.ControlRequest, 1),
	}
}

func (f *fakeControlServerStream) Send(resp *transportpb.ControlResponse) error {
	f.sendCh <- resp
	return nil
}

func (f *fakeControlServerStream) Recv() (*transportpb.ControlRequest, error) {
	select {
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	case req, ok := <-f.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	}
}

func (f *fakeControlServerStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeControlServerStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeControlServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeControlServerStream) Context() context.Context     { return f.ctx }
func (f *fakeControlServerStream) SendMsg(any) error            { return nil }
func (f *fakeControlServerStream) RecvMsg(any) error            { return nil }

func TestTransportServerControlSendsReadyBeforeCapabilityUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	key := types.NamespacedName{Name: "binding", Namespace: "default"}
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(binding).
		WithObjects(binding).
		Build()
	reporter := newFakeReporter(client, key, &transportpb.BindingInfo{
		AudioCodecs: []string{"pcm16"},
	}, logr.Discard())

	baseCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	streamCtx := metadata.NewIncomingContext(baseCtx, metadata.Pairs(
		coretransport.ProtocolMetadataKey, coretransport.ProtocolVersion,
	))
	stream := newFakeControlServerStream(streamCtx)
	server := &transportServer{
		ctx:            baseCtx,
		log:            logr.Discard(),
		reporter:       reporter,
		tunables:       runtimeTunables{MessageTimeout: time.Second},
		handoffUpdates: make(chan *transportpb.ControlResponse, 1),
	}

	done := make(chan error, 1)
	go func() {
		done <- server.Control(stream)
	}()

	var ready *transportpb.ControlResponse
	select {
	case ready = <-stream.sendCh:
	case <-time.After(time.Second):
		t.Fatal("expected initial control response")
	}
	require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY, ready.GetAction())
	require.Equal(t, coretransport.StartupCapabilitiesRequired, ready.GetMetadata()[coretransport.StartupCapabilitiesMetadataKey])

	var capabilities *transportpb.ControlResponse
	select {
	case capabilities = <-stream.sendCh:
	case <-time.After(time.Second):
		t.Fatal("expected capability control response after connector.ready")
	}
	require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_CAPABILITIES, capabilities.GetAction())
	require.Equal(t, "pcm16", capabilities.GetMetadata()["audio.codec"])

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("expected Control to exit after cancellation")
	}
}

func TestTransportServerControlSendsReadyWithNoStartupCapabilitiesMetadataWhenInitialStateEmpty(t *testing.T) {
	baseCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	streamCtx := metadata.NewIncomingContext(baseCtx, metadata.Pairs(
		coretransport.ProtocolMetadataKey, coretransport.ProtocolVersion,
	))
	stream := newFakeControlServerStream(streamCtx)
	server := &transportServer{
		ctx:            baseCtx,
		log:            logr.Discard(),
		tunables:       runtimeTunables{MessageTimeout: time.Second},
		handoffUpdates: make(chan *transportpb.ControlResponse, 1),
	}

	done := make(chan error, 1)
	go func() {
		done <- server.Control(stream)
	}()

	var ready *transportpb.ControlResponse
	select {
	case ready = <-stream.sendCh:
	case <-time.After(time.Second):
		t.Fatal("expected initial control response")
	}
	require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY, ready.GetAction())
	require.Equal(t, coretransport.StartupCapabilitiesNone, ready.GetMetadata()[coretransport.StartupCapabilitiesMetadataKey])

	select {
	case resp := <-stream.sendCh:
		t.Fatalf("did not expect startup capability update, got %+v", resp)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("expected Control to exit after cancellation")
	}
}

func TestTransportServerHandleControlDirectiveIgnoresAck(t *testing.T) {
	server := &transportServer{
		log:  logr.Discard(),
		gate: newMediaGate(),
	}

	resp := server.handleControlDirective(context.Background(), &transportpb.ControlRequest{
		Action: transportpb.ControlAction_CONTROL_ACTION_ACK,
		Metadata: map[string]string{
			"type":    "connector.ready",
			"handled": "true",
			"reason":  "startup",
		},
	})
	require.Nil(t, resp)
}

func TestTransportServerHandleControlDirectiveIgnoresLegacyStartupAck(t *testing.T) {
	server := &transportServer{
		log:  logr.Discard(),
		gate: newMediaGate(),
	}

	resp := server.handleControlDirective(context.Background(), &transportpb.ControlRequest{
		Action: transportpb.ControlAction_CONTROL_ACTION_ACK,
		Metadata: map[string]string{
			"type":    "connector.ready",
			"handled": "false",
			"reason":  "no_control_handler",
		},
	})
	require.Nil(t, resp)
}
