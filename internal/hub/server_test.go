package hub

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	hubv1 "github.com/bubustack/bobravoz-grpc/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// mockStream is a mock of the HubService_ProcessServer interface
type mockStream struct {
	mock.Mock
	RecvChan chan *hubv1.ProcessRequest
	SentChan chan *hubv1.ProcessResponse
	ctx      context.Context
}

func (m *mockStream) Send(resp *hubv1.ProcessResponse) error {
	m.SentChan <- resp
	args := m.Called(resp)
	return args.Error(0)
}

func (m *mockStream) Recv() (*hubv1.ProcessRequest, error) {
	req, ok := <-m.RecvChan
	if !ok {
		// The server's messageLoop specifically checks for `io.EOF`.
		// The mock needs to return that exact error.
		return nil, io.EOF
	}
	args := m.Called()
	// Allow overriding the error for specific test cases (e.g., network error)
	err := args.Error(1)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (m *mockStream) SetHeader(md metadata.MD) error {
	args := m.Called(md)
	return args.Error(0)
}

func (m *mockStream) SendHeader(md metadata.MD) error {
	args := m.Called(md)
	return args.Error(0)
}

func (m *mockStream) SetTrailer(md metadata.MD) {
	m.Called(md)
}

func (m *mockStream) Context() context.Context {
	m.Called()
	return m.ctx
}

func (m *mockStream) SendMsg(v any) error {
	args := m.Called(v)
	return args.Error(0)
}

func (m *mockStream) RecvMsg(v any) error {
	args := m.Called(v)
	return args.Error(0)
}

func newMockStream(ctx context.Context) *mockStream {
	return &mockStream{
		RecvChan: make(chan *hubv1.ProcessRequest, 1),
		SentChan: make(chan *hubv1.ProcessResponse, 1),
		ctx:      ctx,
	}
}

func newTestServer(objects ...client.Object) *Server {
	scheme := runtime.NewScheme()
	// Add types to scheme
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	return NewServer(fakeClient)
}

func TestServer_Process_MissingMetadata(t *testing.T) {
	s := newTestServer()
	ctx := context.Background() // No metadata attached
	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)

	err := s.Process(stream)

	assert.Error(t, err)
	assert.Equal(t, "missing metadata", err.Error())
}

func TestServer_Process_StreamEOF(t *testing.T) {
	s := newTestServer()
	md := metadata.New(map[string]string{
		metaStoryRunName:  "test-storyrun",
		metaStoryRunNS:    "test-ns",
		metaCurrentStepID: "step1",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)
	stream.On("Recv").Return(nil, nil) // Success on first call if any, then EOF

	close(stream.RecvChan) // This will trigger the io.EOF in Recv mock
	err := s.Process(stream)

	assert.NoError(t, err) // EOF is a graceful shutdown, not an error
}

func TestServer_Process_ContextCanceled(t *testing.T) {
	s := newTestServer()
	md := metadata.New(map[string]string{
		metaStoryRunName:  "test-storyrun",
		metaStoryRunNS:    "test-ns",
		metaCurrentStepID: "step1",
	})
	ctx, cancel := context.WithCancel(context.Background())
	ctx = metadata.NewIncomingContext(ctx, md)

	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)

	// Cancel the context to simulate client disconnect
	cancel()

	err := s.Process(stream)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestServer_Process_RecvError(t *testing.T) {
	s := newTestServer()
	md := metadata.New(map[string]string{
		metaStoryRunName:  "test-storyrun",
		metaStoryRunNS:    "test-ns",
		metaCurrentStepID: "step1",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)

	// Simulate a non-EOF error from Recv
	recvErr := errors.New("network error")
	stream.On("Recv").Return(nil, recvErr)

	// Use a small timeout to prevent the test from hanging
	go func() {
		// The Recv mock now pulls from the channel, so we need to send something
		// to unblock it so it can return the test error.
		stream.RecvChan <- &hubv1.ProcessRequest{}
		time.Sleep(100 * time.Millisecond)
		close(stream.RecvChan)
	}()

	err := s.Process(stream)

	assert.Error(t, err)
	assert.Equal(t, recvErr, err)
}

func TestServer_Process_ForwardMessage(t *testing.T) {
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "test-story", Namespace: "test-ns"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{ID: "step1", Name: "first-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram1"}}},
				{ID: "step2", Name: "second-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram2"}}},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storyrun", Namespace: "test-ns"},
		Spec:       runsv1alpha1.StoryRunSpec{StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}}},
	}

	s := newTestServer(story, storyRun)
	md := metadata.New(map[string]string{
		metaStoryRunName:  "test-storyrun",
		metaStoryRunNS:    "test-ns",
		metaCurrentStepID: "step1",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// Mock the downstream stream
	downstreamStream := newMockStream(ctx)
	downstreamStream.On("Context").Return(ctx)
	downstreamStream.On("Send", mock.Anything).Return(nil)

	// Add the downstream stream to the manager
	s.streamManager.AddStream("test-storyrun", "step2", downstreamStream)

	// Mock the upstream stream that sends the message
	upstreamStream := newMockStream(ctx)
	upstreamStream.On("Context").Return(ctx)
	upstreamStream.On("Recv").Return(nil, nil) // Will return EOF after first message

	// Send a request on the upstream channel
	payload, _ := structpb.NewStruct(map[string]any{"key": "value"})
	req := &hubv1.ProcessRequest{
		Packet: &hubv1.DataPacket{Payload: payload},
	}
	upstreamStream.RecvChan <- req

	// In a goroutine, run the process and close the channel to end the loop
	go func() {
		err := s.Process(upstreamStream)
		assert.NoError(t, err)
	}()

	// Wait for the message to be forwarded to the downstream stream
	select {
	case received := <-downstreamStream.SentChan:
		assert.Equal(t, payload, received.Packet.Payload)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for message to be forwarded")
	}

	// Close the upstream channel to terminate the Process loop gracefully
	close(upstreamStream.RecvChan)
}

func TestServer_Process_CELPrimitive(t *testing.T) {
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "test-story", Namespace: "test-ns"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{ID: "step1", Name: "first-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram1"}}},
				{
					ID:   "step2",
					Name: "transform-step",
					Type: "transform",
					With: &runtime.RawExtension{Raw: []byte(`{"output": "{{ payload.key + '-transformed' }}"}`)},
				},
				{ID: "step3", Name: "third-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram2"}}},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storyrun", Namespace: "test-ns"},
		Spec:       runsv1alpha1.StoryRunSpec{StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}}},
	}

	s := newTestServer(story, storyRun)
	md := metadata.New(map[string]string{
		metaStoryRunName:  "test-storyrun",
		metaStoryRunNS:    "test-ns",
		metaCurrentStepID: "step1",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// Mock the downstream stream
	downstreamStream := newMockStream(ctx)
	downstreamStream.On("Context").Return(ctx)
	downstreamStream.On("Send", mock.Anything).Return(nil)
	s.streamManager.AddStream("test-storyrun", "step3", downstreamStream)

	// Mock the upstream stream
	upstreamStream := newMockStream(ctx)
	upstreamStream.On("Context").Return(ctx)
	upstreamStream.On("Recv").Return(nil, nil)

	// Send a request
	payload, _ := structpb.NewStruct(map[string]any{"key": "value"})
	req := &hubv1.ProcessRequest{
		Packet: &hubv1.DataPacket{Payload: payload},
	}
	upstreamStream.RecvChan <- req

	go func() {
		err := s.Process(upstreamStream)
		assert.NoError(t, err)
	}()

	// Wait for the transformed message
	select {
	case received := <-downstreamStream.SentChan:
		fields := received.Packet.Payload.GetFields()
		assert.Contains(t, fields, "output")
		assert.Equal(t, "value-transformed", fields["output"].GetStringValue())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for transformed message")
	}

	close(upstreamStream.RecvChan)
}
