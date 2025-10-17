/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hub

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	bobrapetcel "github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// mockStream is a mock of the HubService_ProcessServer interface
type mockStream struct {
	mock.Mock
	RecvChan chan *transportpb.ProcessRequest
	SentChan chan *transportpb.ProcessResponse
	ctx      context.Context
}

func (m *mockStream) Send(resp *transportpb.ProcessResponse) error {
	m.SentChan <- resp
	args := m.Called(resp)
	return args.Error(0)
}

func (m *mockStream) Recv() (*transportpb.ProcessRequest, error) {
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
		RecvChan: make(chan *transportpb.ProcessRequest, 1),
		SentChan: make(chan *transportpb.ProcessResponse, 1),
		ctx:      ctx,
	}
}

func newTestServer(t *testing.T, objects ...client.Object) *Server {
	t.Helper()
	scheme := runtime.NewScheme()
	// Add types to scheme
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	server, err := NewServer(context.Background(), fakeClient, bobrapetcel.Config{})
	if err != nil {
		// In tests, we expect server creation to always succeed.
		panic(fmt.Sprintf("failed to create test server: %v", err))
	}
	t.Cleanup(server.Close)
	return server
}

func TestServer_Process_MissingMetadata(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background() // No metadata attached
	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)

	err := s.Process(stream)

	assert.Error(t, err)
	assert.Equal(t, "missing metadata", err.Error())
}

func TestServer_Process_StreamEOF(t *testing.T) {
	s := newTestServer(t)
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
	s := newTestServer(t)
	md := metadata.New(map[string]string{
		metaStoryRunName:  "test-storyrun",
		metaStoryRunNS:    "test-ns",
		metaCurrentStepID: "step1",
	})
	ctx, cancel := context.WithCancel(context.Background())
	ctx = metadata.NewIncomingContext(ctx, md)

	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)
	stream.On("Recv").Return(nil, nil)

	// Cancel the context to simulate client disconnect
	cancel()

	err := s.Process(stream)
	close(stream.RecvChan)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestServer_Process_RecvError(t *testing.T) {
	s := newTestServer(t)
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
		stream.RecvChan <- &transportpb.ProcessRequest{}
		time.Sleep(100 * time.Millisecond)
		close(stream.RecvChan)
	}()

	err := s.Process(stream)

	assert.Error(t, err)
	assert.Equal(t, recvErr, err)
}

func TestServer_Process_PerMessageTimeout(t *testing.T) {
	s := newTestServer(t)
	s.perMessageTimeout = 15 * time.Millisecond

	md := metadata.New(map[string]string{
		metaStoryRunName:  "test-storyrun",
		metaStoryRunNS:    "test-ns",
		metaCurrentStepID: "step1",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)
	stream.On("Recv").Return(nil, nil)

	go func() {
		// Allow the timeout to trigger before unblocking the mock.
		time.Sleep(50 * time.Millisecond)
		close(stream.RecvChan)
	}()

	err := s.Process(stream)

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.DeadlineExceeded, st.Code())
}

func TestGenerateStepRunNameWithinLimit(t *testing.T) {
	now := time.Unix(100, 0)
	got := generateStepRunName("storyrun", "step", now)
	want := "storyrun-step-100000"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
	if len(got) > validation.DNS1123SubdomainMaxLength {
		t.Fatalf("name length %d exceeds limit %d", len(got), validation.DNS1123SubdomainMaxLength)
	}
}

func TestGenerateStepRunNameTruncatesAndHashes(t *testing.T) {
	longStory := strings.Repeat("a", 240)
	longStep := strings.Repeat("b", 63)
	now := time.Unix(123456789, 0)

	got := generateStepRunName(longStory, longStep, now)
	if len(got) > validation.DNS1123SubdomainMaxLength {
		t.Fatalf("name length %d exceeds limit %d", len(got), validation.DNS1123SubdomainMaxLength)
	}

	base := fmt.Sprintf("%s-%s", longStory, longStep)
	sum := sha1.Sum([]byte(base))
	hashStr := hex.EncodeToString(sum[:4])
	suffix := fmt.Sprintf("%d", now.UnixMilli())

	if !strings.HasSuffix(got, suffix) {
		t.Fatalf("expected suffix %q in %q", suffix, got)
	}
	if !strings.Contains(got, "-"+hashStr+"-") {
		t.Fatalf("expected hash segment %q in %q", hashStr, got)
	}
	if strings.HasPrefix(got, "-") || strings.HasSuffix(got, "-") {
		t.Fatalf("unexpected leading/trailing hyphen in %q", got)
	}
}

func TestServer_Process_ForwardMessage(t *testing.T) {
	engram1 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram1", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engram2 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram2", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "test-story", Namespace: "test-ns"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{ID: "step1", Name: "first-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram1"}}},
				{
					ID:   "step2",
					Name: "second-step",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram2"}},
					With: &runtime.RawExtension{Raw: []byte(`{"capture": "{{ payload.key }}"}`)},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storyrun", Namespace: "test-ns"},
		Spec:       runsv1alpha1.StoryRunSpec{StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}}},
	}

	s := newTestServer(t, story, storyRun, engram1, engram2)
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
	s.streamManager.AddStream(ctx, "test-storyrun", "test-ns", "step2", downstreamStream)

	// Mock the upstream stream that sends the message
	upstreamStream := newMockStream(ctx)
	upstreamStream.On("Context").Return(ctx)
	upstreamStream.On("Recv").Return(nil, nil) // Will return EOF after first message

	// Send a request on the upstream channel
	payload, _ := structpb.NewStruct(map[string]any{"key": "value"})
	req := &transportpb.ProcessRequest{
		Packet: &transportpb.DataPacket{Payload: payload},
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
		require.NotNil(t, received.Packet.Inputs)
		assert.Equal(t, "value", received.Packet.Inputs.GetFields()["capture"].GetStringValue())
		assert.Equal(t, "step2", received.Packet.Metadata[metaCurrentStepID])
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for message to be forwarded")
	}

	// Close the upstream channel to terminate the Process loop gracefully
	close(upstreamStream.RecvChan)
}

func TestServer_Process_BatchStepReceivesEvaluatedInputs(t *testing.T) {
	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram2", Namespace: "test-ns"},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: "tmpl"},
			Mode:        enums.WorkloadModeJob,
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "test-story", Namespace: "test-ns"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{ID: "step1", Name: "first-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram1"}}},
				{
					ID:   "step2",
					Name: "batch-step",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram2"}},
					With: &runtime.RawExtension{Raw: []byte(`{"url": "{{ payload.url }}"}`)},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storyrun", Namespace: "test-ns"},
		Spec:       runsv1alpha1.StoryRunSpec{StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}}},
	}

	s := newTestServer(t, story, storyRun, engram)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload, _ := structpb.NewStruct(map[string]any{"url": "https://example.com"})
	err := s.processPacket(ctx, "test-storyrun", "test-ns", "step1", &transportpb.DataPacket{
		Payload: payload,
	})
	require.NoError(t, err)

	var stepRuns runsv1alpha1.StepRunList
	require.NoError(t, s.client.List(context.Background(), &stepRuns, client.InNamespace("test-ns")))
	require.Len(t, stepRuns.Items, 1)

	var inputs map[string]any
	require.NoError(t, json.Unmarshal(stepRuns.Items[0].Spec.Input.Raw, &inputs))
	assert.Equal(t, "https://example.com", inputs["url"])
}

func TestServer_Process_CELPrimitive(t *testing.T) {
	engram1 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram1", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engram2 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram2", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
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

	s := newTestServer(t, story, storyRun, engram1, engram2)
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
	s.streamManager.AddStream(ctx, "test-storyrun", "test-ns", "step3", downstreamStream)

	// Mock the upstream stream
	upstreamStream := newMockStream(ctx)
	upstreamStream.On("Context").Return(ctx)
	upstreamStream.On("Recv").Return(nil, nil)

	// Send a request
	payload, _ := structpb.NewStruct(map[string]any{"key": "value"})
	req := &transportpb.ProcessRequest{
		Packet: &transportpb.DataPacket{Payload: payload},
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

func TestServer_Process_MultiplePrimitives(t *testing.T) {
	engram1 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram1", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engram2 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram2", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "test-story", Namespace: "test-ns"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{ID: "step1", Name: "first-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram1"}}},
				{
					ID:   "step2",
					Name: "transform-step-one",
					Type: "transform",
					With: &runtime.RawExtension{Raw: []byte(`{"first": "{{ inputs.key + '-one' }}"}`)},
				},
				{
					ID:   "step3",
					Name: "transform-step-two",
					Type: "transform",
					With: &runtime.RawExtension{Raw: []byte(`{"second": "{{ inputs.first + '-two' }}"}`)},
				},
				{ID: "step4", Name: "fourth-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram2"}}},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storyrun", Namespace: "test-ns"},
		Spec:       runsv1alpha1.StoryRunSpec{StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}}},
	}

	s := newTestServer(t, story, storyRun, engram1, engram2)
	md := metadata.New(map[string]string{
		metaStoryRunName:  "test-storyrun",
		metaStoryRunNS:    "test-ns",
		metaCurrentStepID: "step1",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	downstreamStream := newMockStream(ctx)
	downstreamStream.On("Context").Return(ctx)
	downstreamStream.On("Send", mock.Anything).Return(nil)
	s.streamManager.AddStream(ctx, "test-storyrun", "test-ns", "step4", downstreamStream)

	upstreamStream := newMockStream(ctx)
	upstreamStream.On("Context").Return(ctx)
	upstreamStream.On("Recv").Return(nil, nil)

	payload, _ := structpb.NewStruct(map[string]any{"key": "value"})
	req := &transportpb.ProcessRequest{Packet: &transportpb.DataPacket{Payload: payload}}
	upstreamStream.RecvChan <- req

	go func() {
		err := s.Process(upstreamStream)
		assert.NoError(t, err)
	}()

	select {
	case received := <-downstreamStream.SentChan:
		fields := received.Packet.Payload.GetFields()
		assert.Equal(t, "value-one-two", fields["second"].GetStringValue())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for chained primitives result")
	}

	close(upstreamStream.RecvChan)
}
