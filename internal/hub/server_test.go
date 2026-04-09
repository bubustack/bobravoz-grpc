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

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	"github.com/bubustack/core/templating"
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
	return newTestServerWithConfig(t, "error", "bubu-materialize", objects...)
}

const packetKindXExpr = "{{ eq packet.kind \"x\" }}"

//nolint:gocyclo // Test bootstrap keeps scheme/object wiring together for realistic hub fixtures.
func newTestServerWithConfig(t *testing.T, offloadedPolicy, materializeEngram string, objects ...client.Object) *Server {
	t.Helper()
	// Ensure hub tests never attempt to initialize external storage backends.
	t.Setenv(contracts.StorageProviderEnv, "none")
	scheme := runtime.NewScheme()
	// Add types to scheme
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := catalogv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := transportv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	defaultTemplateName := "tmpl"
	providedTemplates := map[string]struct{}{}
	for _, obj := range objects {
		if template, ok := obj.(*catalogv1alpha1.EngramTemplate); ok {
			if template != nil && template.Name != "" {
				providedTemplates[template.Name] = struct{}{}
			}
		}
	}
	requiredTemplates := map[string]struct{}{}
	for _, obj := range objects {
		engram, ok := obj.(*bubuv1alpha1.Engram)
		if !ok || engram == nil {
			continue
		}
		templateName := strings.TrimSpace(engram.Spec.TemplateRef.Name)
		if templateName == "" {
			templateName = defaultTemplateName
			engram.Spec.TemplateRef.Name = templateName
		}
		requiredTemplates[templateName] = struct{}{}
	}
	for templateName := range requiredTemplates {
		if _, ok := providedTemplates[templateName]; ok {
			continue
		}
		objects = append(objects, &catalogv1alpha1.EngramTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: templateName},
		})
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(&runsv1alpha1.StepRun{}, &runsv1alpha1.StoryRun{}).Build()
	server, err := NewServer(context.Background(), fakeClient, templating.Config{}, offloadedPolicy, materializeEngram)
	if err != nil {
		// In tests, we expect server creation to always succeed.
		panic(fmt.Sprintf("failed to create test server: %v", err))
	}
	t.Cleanup(server.Close)
	return server
}

func rawExtensionFromMap(t *testing.T, data map[string]any) *runtime.RawExtension {
	t.Helper()
	b, err := json.Marshal(data)
	require.NoError(t, err)
	return &runtime.RawExtension{Raw: b}
}

func TestEvaluateStepCondition_StaticFalse(t *testing.T) {
	s := newTestServer(t)
	expr := "{{ eq inputs.flag true }}"
	payload, err := structpb.NewStruct(map[string]any{"kind": "x"})
	require.NoError(t, err)

	ok, deferred, err := s.evaluateStepCondition(context.Background(), &expr, payload, nil, nil, map[string]any{"flag": false}, false)
	require.NoError(t, err)
	assert.False(t, deferred)
	assert.False(t, ok)
}

func TestEvaluateStepCondition_RuntimePacket(t *testing.T) {
	s := newTestServer(t)
	expr := packetKindXExpr
	payload, err := structpb.NewStruct(map[string]any{"kind": "x"})
	require.NoError(t, err)

	ok, deferred, err := s.evaluateStepCondition(context.Background(), &expr, payload, nil, nil, nil, false)
	require.NoError(t, err)
	assert.False(t, deferred)
	assert.True(t, ok)
}

func TestEvaluateStepCondition_RuntimePacketMetadataMissingDoesNotFail(t *testing.T) {
	s := newTestServer(t)
	expr := "{{ eq (default \"\" packet.metadata.type) \"chat.message.v1\" }}"
	payload, err := structpb.NewStruct(map[string]any{"text": "hello"})
	require.NoError(t, err)

	ok, deferred, err := s.evaluateStepCondition(context.Background(), &expr, payload, nil, nil, nil, false)
	require.NoError(t, err)
	assert.False(t, deferred)
	assert.False(t, ok)
}

func TestEvaluateStepCondition_RuntimeIndexStepsMissingDoesNotFail(t *testing.T) {
	s := newTestServer(t)
	expr := "{{ eq (default false (index .steps \"context-user\").accepted) true }}"

	ok, deferred, err := s.evaluateStepCondition(context.Background(), &expr, nil, nil, nil, nil, false)
	require.NoError(t, err, "missing steps should not cause an error — default handles nil gracefully")
	assert.False(t, deferred)
	assert.False(t, ok, "condition should evaluate to false when step is absent")
}

func TestEvaluateEngramInputs_RuntimeIndexStepsMissingDoesNotFail(t *testing.T) {
	s := newTestServer(t)
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Pattern: enums.RealtimePattern,
		},
	}
	step := &bubuv1alpha1.Step{
		Name: "respond",
		Runtime: rawExtensionFromMap(t, map[string]any{
			"userPrompt": "{{ default \"\" (index .steps \"context-user\").text }}",
		}),
	}

	out, err := s.evaluateEngramInputs(context.Background(), story, step, nil, nil, nil, nil)
	require.NoError(t, err, "missing steps should not cause an error — default handles nil gracefully")
	require.NotNil(t, out, "engram inputs should resolve even when referenced step is absent")
}

func TestEvaluateEngramInputs_ContextUserRuntimeDoesNotRequirePreviousRespondStep(t *testing.T) {
	s := newTestServer(t)
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Pattern: enums.RealtimePattern,
		},
	}
	step := &bubuv1alpha1.Step{
		Name: "context-user",
		Runtime: rawExtensionFromMap(t, map[string]any{
			"key":            `{{ printf "%s:%s" inputs.event.id inputs.participant.identity }}`,
			"role":           "user",
			"text":           `{{ steps.transcribe.text }}`,
			"speakerId":      `{{ inputs.participant.identity }}`,
			"includeHistory": true,
		}),
	}

	out, err := s.evaluateEngramInputs(context.Background(), story, step, nil, nil, map[string]any{
		"transcribe": map[string]any{
			"outputs": map[string]any{
				"text": "hello there",
			},
		},
	}, map[string]any{
		"event": map[string]any{
			"id": "evt-123",
		},
		"participant": map[string]any{
			"identity": "alice",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, "evt-123:alice", out.AsMap()["key"])
	assert.Equal(t, "user", out.AsMap()["role"])
	assert.Equal(t, "hello there", out.AsMap()["text"])
	assert.Equal(t, "alice", out.AsMap()["speakerId"])
	assert.Equal(t, true, out.AsMap()["includeHistory"])
}

func TestLifecycleHookConsumerStepIndexes(t *testing.T) {
	readyExpr := "{{ eq (default \"\" packet.type) \"storyrun.ready\" }}"
	otherExpr := "{{ eq (default \"\" packet.type) \"speech.transcript.v1\" }}"
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "ingress",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "livekit-voice-stream"}},
				},
				{
					Name: "greet",
					If:   &readyExpr,
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "openai-assistant"}},
				},
				{
					Name: "respond",
					If:   &otherExpr,
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "openai-assistant"}},
				},
			},
		},
	}

	consumers := lifecycleHookConsumerStepIndexes(story, lifecycleHookStoryReadyEvent)
	require.Equal(t, []int{1}, consumers)
}

func TestAllStreamingStepStreamsConnected(t *testing.T) {
	s := newTestServer(t)
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name:      "ingress",
					Transport: "voice",
					Ref:       &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "livekit-voice-stream"}},
				},
				{
					Name:      "transcribe",
					Transport: "voice",
					Ref:       &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "openai-stt"}},
				},
				{
					Name: "fanout",
					Type: enums.StepTypeParallel,
				},
			},
		},
	}

	storyRunName := "storyrun"
	storyRunNS := "ns"
	require.False(t, s.allStreamingStepStreamsConnected(story, storyRunName, storyRunNS))

	keyIngress := s.streamManager.streamKey(storyRunName, storyRunNS, "ingress")
	s.streamManager.streams.Store(keyIngress, &streamEntry{})
	require.False(t, s.allStreamingStepStreamsConnected(story, storyRunName, storyRunNS))

	keyTranscribe := s.streamManager.streamKey(storyRunName, storyRunNS, "transcribe")
	s.streamManager.streams.Store(keyTranscribe, &streamEntry{})
	require.True(t, s.allStreamingStepStreamsConnected(story, storyRunName, storyRunNS))
}

func TestEmitLifecycleHookEvent_BuffersHookPacketForConsumerStep(t *testing.T) {
	ns := "ns"
	hookExpr := "{{ eq (default \"\" packet.type) \"storyrun.ready\" }}"

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name:      "ingress",
					Transport: "voice",
					Ref:       &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "livekit-voice-stream"}},
				},
				{
					Name:      "greet",
					Needs:     []string{"ingress"},
					Transport: "voice",
					If:        &hookExpr,
					Ref:       &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "openai-assistant"}},
					Runtime:   rawExtensionFromMap(t, map[string]any{"userPrompt": "say hi"}),
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
			Inputs:   rawExtensionFromMap(t, map[string]any{"participant": map[string]any{"identity": "alice"}}),
		},
	}
	engramGreet := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "openai-assistant", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}

	s := newTestServer(t, story, storyRun, engramGreet)
	consumerIdx := lifecycleHookConsumerStepIndexes(story, lifecycleHookStoryReadyEvent)
	require.Equal(t, []int{1}, consumerIdx)
	payload, err := lifecycleHookPayload(lifecycleHookStoryReadyEvent, storyRun.Name, storyRun.Namespace, "ingress")
	require.NoError(t, err)
	storyInputs, err := rawExtensionToMap(storyRun.Spec.Inputs)
	require.NoError(t, err)
	shouldRun, deferred, err := s.evaluateStepCondition(context.Background(), story.Spec.Steps[1].If, payload, nil, nil, storyInputs, false)
	require.NoError(t, err)
	require.False(t, deferred)
	require.True(t, shouldRun)
	evaluatedInputs, deferred, err := s.evaluateNextEngramInputs(context.Background(), storyRun.Name, story, &story.Spec.Steps[1], payload, nil, nil, storyInputs)
	require.NoError(t, err)
	require.False(t, deferred)
	require.NotNil(t, evaluatedInputs)
	assert.Equal(t, "say hi", evaluatedInputs.AsMap()["userPrompt"])

	sent := s.emitLifecycleHookEvent(context.Background(), storyRun, story, lifecycleHookStoryReadyEvent, "ingress")
	require.True(t, sent)

	keyGreet := s.streamManager.streamKey(storyRun.Name, ns, "greet")
	val, ok := s.streamManager.buffers.Load(keyGreet)
	require.True(t, ok)
	buf := val.(*MessageBuffer)
	require.Greater(t, buf.Size(), 0)

	buf.mu.Lock()
	require.NotEmpty(t, buf.messages)
	packet := buf.messages[0]
	buf.mu.Unlock()

	require.NotNil(t, packet.GetPayload())
	assert.Equal(t, lifecycleHookStoryReadyEvent, packet.GetPayload().AsMap()["type"])
	require.NotNil(t, packet.GetInputs())
	assert.Equal(t, "say hi", packet.GetInputs().AsMap()["userPrompt"])
	assert.Equal(t, lifecycleHookDedupKey(ns, storyRun.Name, lifecycleHookStoryReadyEvent, "ingress"), packet.GetMetadata()[metaEnvelopeMessageIDKey])
}

func TestProcessPacket_SkipConditionRoutesToNextEngram(t *testing.T) {
	ns := "ns"
	ifFalse := "{{ eq inputs.route \"b\" }}"

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-b",
					Needs: []string{"step-a"},
					If:    &ifFalse,
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
				{
					Name:  "step-c",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-c"}},
				},
			},
		},
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
			Inputs:   rawExtensionFromMap(t, map[string]any{"route": "c"}),
		},
	}

	engramB := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-b", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engramC := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-c", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}

	s := newTestServer(t, story, storyRun, engramB, engramC)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	keyB := s.streamManager.streamKey(storyRun.Name, ns, "step-b")
	_, okB := s.streamManager.buffers.Load(keyB)
	assert.False(t, okB)

	keyC := s.streamManager.streamKey(storyRun.Name, ns, "step-c")
	val, okC := s.streamManager.buffers.Load(keyC)
	require.True(t, okC)
	buf := val.(*MessageBuffer)
	assert.Greater(t, buf.Size(), 0)
}

func TestProcessPacket_HeartbeatSkipsStaticFalse(t *testing.T) {
	ns := "ns"
	ifFalse := "{{ eq inputs.route \"b\" }}"

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-b",
					Needs: []string{"step-a"},
					If:    &ifFalse,
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
				{
					Name:  "step-c",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-c"}},
				},
			},
		},
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
			Inputs:   rawExtensionFromMap(t, map[string]any{"route": "c"}),
		},
	}

	engramB := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-b", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engramC := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-c", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}

	s := newTestServer(t, story, storyRun, engramB, engramC)
	packet := &transportpb.DataPacket{Metadata: map[string]string{"bubu-heartbeat": trueString}}

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	keyB := s.streamManager.streamKey(storyRun.Name, ns, "step-b")
	_, okB := s.streamManager.buffers.Load(keyB)
	assert.False(t, okB)

	keyC := s.streamManager.streamKey(storyRun.Name, ns, "step-c")
	val, okC := s.streamManager.buffers.Load(keyC)
	require.True(t, okC)
	buf := val.(*MessageBuffer)
	assert.Greater(t, buf.Size(), 0)
}

func TestProcessPacket_ParallelFanOut(t *testing.T) {
	ns := "ns"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-parallel",
					Needs: []string{"step-a"},
					Type:  enums.StepTypeParallel,
				},
				{
					Name:  "step-b",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
				{
					Name:  "step-c",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-c"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	engramB := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-b", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engramC := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-c", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	s := newTestServer(t, story, storyRun, engramB, engramC)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	keyB := s.streamManager.streamKey(storyRun.Name, ns, "step-b")
	valB, okB := s.streamManager.buffers.Load(keyB)
	require.True(t, okB)
	assert.Greater(t, valB.(*MessageBuffer).Size(), 0)

	keyC := s.streamManager.streamKey(storyRun.Name, ns, "step-c")
	valC, okC := s.streamManager.buffers.Load(keyC)
	require.True(t, okC)
	assert.Greater(t, valC.(*MessageBuffer).Size(), 0)
}

func TestProcessPacket_ParallelPrimitiveDoesNotLeakSiblingStepVars(t *testing.T) {
	ns := "ns"
	ifExpr := "{{ hasKey .steps \"step-prim\" }}"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-parallel",
					Needs: []string{"step-a"},
					Type:  enums.StepTypeParallel,
				},
				{
					Name:  "step-prim",
					Needs: []string{"step-a"},
					Type:  enums.StepTypeCondition,
				},
				{
					Name:  "step-b",
					Needs: []string{"step-a"},
					If:    &ifExpr,
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
				{
					Name:  "step-c",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-c"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	engramB := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-b", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engramC := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-c", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}

	s := newTestServer(t, story, storyRun, engramB, engramC)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	keyB := s.streamManager.streamKey(storyRun.Name, ns, "step-b")
	_, okB := s.streamManager.buffers.Load(keyB)
	assert.False(t, okB)

	keyC := s.streamManager.streamKey(storyRun.Name, ns, "step-c")
	valC, okC := s.streamManager.buffers.Load(keyC)
	require.True(t, okC)
	assert.Greater(t, valC.(*MessageBuffer).Size(), 0)
}

func TestProcessPacket_BroadcastToMultipleEngrams(t *testing.T) {
	ns := "ns"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-b",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
				{
					Name:  "step-c",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-c"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	engramB := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-b", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engramC := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-c", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	s := newTestServer(t, story, storyRun, engramB, engramC)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	keyB := s.streamManager.streamKey(storyRun.Name, ns, "step-b")
	valB, okB := s.streamManager.buffers.Load(keyB)
	require.True(t, okB)
	assert.Greater(t, valB.(*MessageBuffer).Size(), 0)

	keyC := s.streamManager.streamKey(storyRun.Name, ns, "step-c")
	valC, okC := s.streamManager.buffers.Load(keyC)
	require.True(t, okC)
	assert.Greater(t, valC.(*MessageBuffer).Size(), 0)
}

func TestProcessPacket_RuntimeIfSkipsBranch(t *testing.T) {
	ns := "ns"
	ifExpr := packetKindXExpr
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-b",
					Needs: []string{"step-a"},
					If:    &ifExpr,
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
				{
					Name:  "step-c",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-c"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	engramB := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-b", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engramC := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-c", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	s := newTestServer(t, story, storyRun, engramB, engramC)
	payload, err := structpb.NewStruct(map[string]any{"kind": "y"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	keyB := s.streamManager.streamKey(storyRun.Name, ns, "step-b")
	_, okB := s.streamManager.buffers.Load(keyB)
	assert.False(t, okB)

	keyC := s.streamManager.streamKey(storyRun.Name, ns, "step-c")
	valC, okC := s.streamManager.buffers.Load(keyC)
	require.True(t, okC)
	assert.Greater(t, valC.(*MessageBuffer).Size(), 0)
}

func TestProcessPacket_RuntimeMetadataIfSkipsBranchWithoutCrashing(t *testing.T) {
	ns := "ns"
	ifExpr := "{{ eq (default \"\" packet.metadata.type) \"chat.message.v1\" }}"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "chat-respond",
					Needs: []string{"step-a"},
					If:    &ifExpr,
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
				{
					Name:  "buffer",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-c"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	engramB := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-b", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engramC := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-c", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	s := newTestServer(t, story, storyRun, engramB, engramC)
	payload, err := structpb.NewStruct(map[string]any{"text": "hello"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	keyB := s.streamManager.streamKey(storyRun.Name, ns, "chat-respond")
	_, okB := s.streamManager.buffers.Load(keyB)
	assert.False(t, okB)

	keyC := s.streamManager.streamKey(storyRun.Name, ns, "buffer")
	valC, okC := s.streamManager.buffers.Load(keyC)
	require.True(t, okC)
	assert.Greater(t, valC.(*MessageBuffer).Size(), 0)
}

func TestProcessPacket_JoinWaitsForAllNeeds(t *testing.T) {
	ns := "ns"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-b",
					Needs: []string{"step-b"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
				{
					Name:  "step-c",
					Needs: []string{"step-a", "step-b"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-c"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	engramC := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-c", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	s := newTestServer(t, story, storyRun, engramC)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)

	packetA := &transportpb.DataPacket{Payload: payload, Metadata: map[string]string{metaJoinKey: "join-1"}}
	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packetA))

	keyC := s.streamManager.streamKey(storyRun.Name, ns, "step-c")
	_, okC := s.streamManager.buffers.Load(keyC)
	assert.False(t, okC)

	packetB := &transportpb.DataPacket{Payload: payload, Metadata: map[string]string{metaJoinKey: "join-1"}}
	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-b", packetB))

	valC, okC := s.streamManager.buffers.Load(keyC)
	require.True(t, okC)
	assert.Greater(t, valC.(*MessageBuffer).Size(), 0)
}

func TestProcessPacket_StopPrimitive(t *testing.T) {
	ns := "ns"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-stop",
					Needs: []string{"step-a"},
					Type:  enums.StepTypeStop,
				},
				{
					Name:  "step-b",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	engramB := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-b", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	s := newTestServer(t, story, storyRun, engramB)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	keyB := s.streamManager.streamKey(storyRun.Name, ns, "step-b")
	_, okB := s.streamManager.buffers.Load(keyB)
	assert.False(t, okB)
}

func TestProcessPacket_ExecuteStoryCreatesChild(t *testing.T) {
	ns := "ns"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-exec",
					Needs: []string{"step-a"},
					Type:  enums.StepTypeExecuteStory,
					With: rawExtensionFromMap(t, map[string]any{
						"storyRef": map[string]any{"name": "child-story"},
						"with":     map[string]any{"key": "value"},
					}),
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	s := newTestServer(t, story, storyRun)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	var list runsv1alpha1.StoryRunList
	err = s.client.List(context.Background(), &list, client.InNamespace(ns), client.MatchingLabels{
		contracts.ParentStoryRunLabel: storyRun.Name,
		contracts.ParentStepLabel:     "step-exec",
	})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)
}

func TestProcessPacket_PrimitiveAfterParallelExecutes(t *testing.T) {
	ns := "ns"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Pattern: enums.RealtimePattern,
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-parallel",
					Needs: []string{"step-a"},
					Type:  enums.StepTypeParallel,
				},
				{
					Name:  "step-exec",
					Needs: []string{"step-a"},
					Type:  enums.StepTypeExecuteStory,
					With: rawExtensionFromMap(t, map[string]any{
						"storyRef": map[string]any{"name": "child-story"},
						"with":     map[string]any{"key": "value"},
					}),
				},
				{
					Name:  "step-b",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	engramB := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-b", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	s := newTestServer(t, story, storyRun, engramB)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	var list runsv1alpha1.StoryRunList
	err = s.client.List(context.Background(), &list, client.InNamespace(ns), client.MatchingLabels{
		contracts.ParentStoryRunLabel: storyRun.Name,
		contracts.ParentStepLabel:     "step-exec",
	})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)

	keyB := s.streamManager.streamKey(storyRun.Name, ns, "step-b")
	_, okB := s.streamManager.buffers.Load(keyB)
	assert.True(t, okB)
}

func TestProcessPacket_StreamingWithValidationBlocksStepsContext(t *testing.T) {
	ns := "ns"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Pattern: enums.RealtimePattern,
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-b",
					Needs: []string{"step-a"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
					With: rawExtensionFromMap(t, map[string]any{
						"foo": "{{ steps.step-a.outputs.value }}",
					}),
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	s := newTestServer(t, story, storyRun)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	err = s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "with block not valid for streaming")
}

func TestStreamingTemplateScopes(t *testing.T) {
	staticScope := streamingStaticScope()
	runtimeScope := streamingRuntimeScope()

	err := templating.ValidateTemplateString("{{ packet.id }}", staticScope)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "packet")

	err = templating.ValidateTemplateString("{{ steps.alpha.value }}", staticScope)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "steps")

	err = templating.ValidateTemplateString("{{ now }}", staticScope)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")

	err = templating.ValidateTemplateString("{{ packet.id }}", runtimeScope)
	require.NoError(t, err)

	err = templating.ValidateTemplateString("{{ steps.alpha.value }}", runtimeScope)
	require.NoError(t, err)

	err = templating.ValidateTemplateString("{{ inputs.flag }}", runtimeScope)
	require.NoError(t, err)
}

func TestProcessPacket_ExecuteStoryWaitsForCompletion(t *testing.T) {
	ns := "ns"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-exec",
					Needs: []string{"step-a"},
					Type:  enums.StepTypeExecuteStory,
					With: rawExtensionFromMap(t, map[string]any{
						"storyRef":          map[string]any{"name": "child-story"},
						"waitForCompletion": true,
						"with":              map[string]any{"key": "value"},
					}),
				},
				{
					Name:  "step-b",
					Needs: []string{"step-exec"},
					Ref:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	engramB := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-b", Namespace: ns},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	s := newTestServer(t, story, storyRun, engramB)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.processPacket(ctx, storyRun.Name, ns, "step-a", packet)
	}()

	var child runsv1alpha1.StoryRun
	require.Eventually(t, func() bool {
		var list runsv1alpha1.StoryRunList
		if err := s.client.List(context.Background(), &list, client.InNamespace(ns), client.MatchingLabels{
			contracts.ParentStoryRunLabel: storyRun.Name,
			contracts.ParentStepLabel:     "step-exec",
		}); err != nil {
			return false
		}
		if len(list.Items) != 1 {
			return false
		}
		child = list.Items[0]
		return true
	}, time.Second, 20*time.Millisecond)

	select {
	case err := <-done:
		t.Fatalf("process returned early: %v", err)
	case <-time.After(75 * time.Millisecond):
	}

	child.Status.Phase = enums.PhaseSucceeded
	child.Status.Message = "ok"
	require.NoError(t, s.client.Status().Update(context.Background(), &child))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatalf("process did not return after child completion")
	}
}

func TestEvaluateStepCondition_HeartbeatIgnoresRuntime(t *testing.T) {
	s := newTestServer(t)
	expr := packetKindXExpr

	ok, deferred, err := s.evaluateStepCondition(context.Background(), &expr, nil, nil, nil, nil, true)
	require.NoError(t, err)
	assert.False(t, deferred)
	assert.True(t, ok)
}

func TestEvaluateStepCondition_RuntimeInvalidContext(t *testing.T) {
	s := newTestServer(t)
	expr := "{{ trigger.foo }}"

	ok, deferred, err := s.evaluateStepCondition(context.Background(), &expr, nil, nil, nil, nil, false)
	require.Error(t, err)
	assert.False(t, deferred)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "function \"trigger\" not defined")
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

func TestServer_Process_RejectsProtocolVersionMismatch(t *testing.T) {
	s := newTestServer(t)
	md := metadata.New(map[string]string{
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: "1.0.1",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)

	err := s.Process(stream)

	assert.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "unsupported transport protocol version")
}

func TestServer_Process_RejectsMissingConnectorGeneration(t *testing.T) {
	s := newTestServer(t)
	md := metadata.New(map[string]string{
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)

	err := s.Process(stream)

	assert.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "missing connector-generation metadata")
}

func TestServer_Process_RejectsInvalidConnectorGeneration(t *testing.T) {
	s := newTestServer(t)
	md := metadata.New(map[string]string{
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "oops",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)

	err := s.Process(stream)

	assert.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "invalid connector-generation metadata")
}

func TestObserveStartupCapabilitiesMetadata(t *testing.T) {
	tests := []struct {
		name string
		md   metadata.MD
		want string
	}{
		{
			name: "required",
			md: metadata.New(map[string]string{
				coretransport.StartupCapabilitiesMetadataKey: coretransport.StartupCapabilitiesRequired,
			}),
			want: coretransport.StartupCapabilitiesRequired,
		},
		{
			name: "none",
			md: metadata.New(map[string]string{
				coretransport.StartupCapabilitiesMetadataKey: coretransport.StartupCapabilitiesNone,
			}),
			want: coretransport.StartupCapabilitiesNone,
		},
		{
			name: "missing",
			md:   metadata.New(nil),
			want: "missing",
		},
		{
			name: "invalid",
			md: metadata.New(map[string]string{
				coretransport.StartupCapabilitiesMetadataKey: "legacy",
			}),
			want: "invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, observeStartupCapabilitiesMetadata(tt.md))
		})
	}
}

func TestServer_Process_StreamEOF(t *testing.T) {
	s := newTestServer(t)
	md := metadata.New(map[string]string{
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
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
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
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
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
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

func TestServer_Process_ReassemblesChunkedBinaryPacket(t *testing.T) {
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "test-story", Namespace: "test-ns"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step1",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram1"}},
				},
				{
					Name: "step2",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram2"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storyrun", Namespace: "test-ns"},
		Spec:       runsv1alpha1.StoryRunSpec{StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}}},
	}
	engram1 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram1", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engram2 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram2", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}

	s := newTestServer(t, story, storyRun, engram1, engram2)
	md := metadata.New(map[string]string{
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	downstreamStream := newMockStream(ctx)
	downstreamStream.On("Context").Return(ctx)
	downstreamStream.On("Send", mock.Anything).Return(nil)
	s.streamManager.AddStream(ctx, "test-storyrun", "test-ns", "step2", downstreamStream)

	upstreamStream := newMockStream(ctx)
	upstreamStream.On("Context").Return(ctx)
	upstreamStream.On("Recv").Return(nil, nil)

	baseEnv := &transportpb.StreamEnvelope{
		StreamId:   "stream-1",
		Sequence:   7,
		Partition:  "p1",
		ChunkId:    "chunk-42",
		ChunkCount: 2,
		TotalBytes: uint32(len("foobar")),
	}
	req1 := &transportpb.ProcessRequest{
		Packet: &transportpb.DataPacket{
			Envelope: cloneStreamEnvelope(baseEnv),
			Frame: &transportpb.DataPacket_Binary{
				Binary: &transportpb.BinaryFrame{
					Payload:  []byte("foo"),
					MimeType: "application/octet-stream",
				},
			},
		},
	}
	req1.Packet.Envelope.ChunkIndex = 0
	req1.Packet.Envelope.ChunkBytes = uint32(len(req1.Packet.GetBinary().GetPayload()))

	req2 := &transportpb.ProcessRequest{
		Packet: &transportpb.DataPacket{
			Envelope: cloneStreamEnvelope(baseEnv),
			Frame: &transportpb.DataPacket_Binary{
				Binary: &transportpb.BinaryFrame{
					Payload:  []byte("bar"),
					MimeType: "application/octet-stream",
				},
			},
		},
	}
	req2.Packet.Envelope.ChunkIndex = 1
	req2.Packet.Envelope.ChunkBytes = uint32(len(req2.Packet.GetBinary().GetPayload()))

	done := make(chan error, 1)
	go func() {
		done <- s.Process(upstreamStream)
	}()
	upstreamStream.RecvChan <- req1
	upstreamStream.RecvChan <- req2
	close(upstreamStream.RecvChan)

	select {
	case received := <-downstreamStream.SentChan:
		require.NotNil(t, received.Packet)
		require.NotNil(t, received.Packet.GetBinary())
		assert.Equal(t, []byte("foobar"), received.Packet.GetBinary().GetPayload())
		require.NotNil(t, received.Packet.GetEnvelope())
		assert.Equal(t, buildStreamID("test-storyrun", "test-ns", "step2"), received.Packet.GetEnvelope().GetStreamId())
		assert.Equal(t, uint64(0), received.Packet.GetEnvelope().GetSequence())
		assert.Equal(t, "p1", received.Packet.GetEnvelope().GetPartition())
		assert.Empty(t, received.Packet.GetEnvelope().GetChunkId())
		assert.Equal(t, uint32(0), received.Packet.GetEnvelope().GetChunkCount())
		assert.Equal(t, uint32(0), received.Packet.GetEnvelope().GetChunkIndex())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for chunked packet to be forwarded")
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for Process to exit")
	}
}

func TestServer_Process_ReturnsChunkValidationError(t *testing.T) {
	s := newTestServer(t)
	md := metadata.New(map[string]string{
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	stream := newMockStream(ctx)
	stream.On("Context").Return(ctx)
	stream.On("Recv").Return(nil, nil)

	stream.RecvChan <- &transportpb.ProcessRequest{
		Packet: &transportpb.DataPacket{
			Envelope: &transportpb.StreamEnvelope{
				StreamId:   "stream-1",
				ChunkId:    "chunk-42",
				ChunkCount: 1,
			},
		},
	}
	close(stream.RecvChan)

	err := s.Process(stream)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunked packet missing binary frame")
}

func TestServer_Process_OffloadedInjectRoutesToMaterialize(t *testing.T) {
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-story",
			Namespace: "test-ns",
		},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step1",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name: "step2",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-b"}},
				},
			},
		},
	}
	withRaw, err := json.Marshal(map[string]any{
		"input": "{{ .steps.step1.outputs.large }}",
	})
	require.NoError(t, err)
	story.Spec.Steps[1].With = &runtime.RawExtension{Raw: withRaw}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-storyrun",
			Namespace: "test-ns",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: story.Name}},
		},
	}

	s := newTestServerWithConfig(t, "inject", "bubu-materialize", story, storyRun)

	payload, err := structpb.NewStruct(map[string]any{
		"large": map[string]any{
			templating.StorageRefKey: "outputs/test/large.json",
		},
	})
	require.NoError(t, err)

	currentStepID := getStepID(&story.Spec.Steps[0])
	packet := &transportpb.DataPacket{
		Metadata: map[string]string{
			metaStoryRunName:  storyRun.Name,
			metaStoryRunNS:    storyRun.Namespace,
			metaCurrentStepID: currentStepID,
		},
		Payload:  payload,
		Envelope: &transportpb.StreamEnvelope{StreamId: "upstream-stream", Sequence: 11, Partition: "materialize"},
	}

	err = s.processPacket(context.Background(), storyRun.Name, storyRun.Namespace, currentStepID, packet)
	require.NoError(t, err)

	targetStepID := getStepID(&story.Spec.Steps[1])
	materializeID := materializeStepID(targetStepID)
	materializeRunName := materializeStepRunName(storyRun.Name, materializeID)

	var stepRun runsv1alpha1.StepRun
	require.NoError(t, s.client.Get(context.Background(), client.ObjectKey{
		Name:      materializeRunName,
		Namespace: storyRun.Namespace,
	}, &stepRun))
	require.Equal(t, materializeID, stepRun.Spec.StepID)
	require.NotNil(t, stepRun.Spec.EngramRef)
	require.Equal(t, "bubu-materialize", stepRun.Spec.EngramRef.Name)

	key := s.streamManager.streamKey(storyRun.Name, storyRun.Namespace, materializeID)
	bufVal, ok := s.streamManager.buffers.Load(key)
	require.True(t, ok, "expected buffered packet for materialize step")
	buf, ok := bufVal.(*MessageBuffer)
	require.True(t, ok, "expected message buffer type")
	require.Len(t, buf.messages, 1)
	require.Equal(t, targetStepID, buf.messages[0].Metadata[metaMaterializeNextStep])
	require.NotNil(t, buf.messages[0].GetEnvelope())
	require.Equal(t, buildStreamID(storyRun.Name, storyRun.Namespace, materializeID), buf.messages[0].GetEnvelope().GetStreamId())
	require.Equal(t, uint64(0), buf.messages[0].GetEnvelope().GetSequence())
	require.Equal(t, "materialize", buf.messages[0].GetEnvelope().GetPartition())
}

func TestServer_Process_NoStreamDeadlineFromPerMessageTimeout(t *testing.T) {
	s := newTestServer(t)
	s.perMessageTimeout = 15 * time.Millisecond

	md := metadata.New(map[string]string{
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
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

	require.NoError(t, err)
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
					With: &runtime.RawExtension{Raw: []byte(`{"capture": "{{ inputs.key }}"}`)},
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
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
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
					With: &runtime.RawExtension{Raw: []byte(`{"url": "{{ inputs.url }}"}`)},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storyrun", Namespace: "test-ns"},
		Spec:       runsv1alpha1.StoryRunSpec{StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}}},
	}

	s := newTestServer(t, story, storyRun, engram)

	// Background goroutine: once the batch StepRun is created, mark it as Succeeded
	// so the hub's wait-for-completion loop can proceed.
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			var list runsv1alpha1.StepRunList
			if err := s.client.List(context.Background(), &list, client.InNamespace("test-ns")); err != nil || len(list.Items) == 0 {
				continue
			}
			sr := &list.Items[0]
			original := sr.DeepCopy()
			sr.Status.Phase = enums.PhaseSucceeded
			sr.Status.Output = &runtime.RawExtension{Raw: []byte(`{"result":"ok"}`)}
			_ = s.client.Status().Patch(context.Background(), sr, client.MergeFrom(original))
			return
		}
	}()

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
					With: &runtime.RawExtension{Raw: []byte(`{"output": "{{ inputs.key + '-transformed' }}"}`)},
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
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
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
		assert.Contains(t, fields, "key")
		assert.Equal(t, "value", fields["key"].GetStringValue())
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
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
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
		assert.Equal(t, "value", fields["key"].GetStringValue())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for chained primitives result")
	}

	close(upstreamStream.RecvChan)
}

func TestServer_Process_HotTransportBypassesEvaluation(t *testing.T) {
	engram1 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "ingress", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engram2 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "responder", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "test-story", Namespace: "test-ns"},
		Spec: bubuv1alpha1.StorySpec{
			Transports: []bubuv1alpha1.StoryTransport{
				{Name: "rt", TransportRef: "rt"},
			},
			Steps: []bubuv1alpha1.Step{
				{ID: "step1", Name: "ingress-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "ingress"}}, Transport: "rt"},
				{ID: "step2", Name: "responder-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "responder"}}, Needs: []string{"step1"}, Transport: "rt"},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storyrun", Namespace: "test-ns"},
		Spec:       runsv1alpha1.StoryRunSpec{StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}}},
	}
	story.Status.Transports = []bubuv1alpha1.StoryTransportStatus{
		{Name: "rt", TransportRef: "rt", Mode: enums.TransportModeHot},
	}

	s := newTestServer(t, story, storyRun, engram1, engram2)
	md := metadata.New(map[string]string{
		metaStoryRunName:                  "test-storyrun",
		metaStoryRunNS:                    "test-ns",
		metaCurrentStepID:                 "step1",
		metaConnectorGeneration:           "1",
		coretransport.ProtocolMetadataKey: coretransport.ProtocolVersion,
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	downstreamStream := newMockStream(ctx)
	downstreamStream.On("Context").Return(ctx)
	downstreamStream.On("Send", mock.Anything).Return(nil)
	s.streamManager.AddStream(ctx, "test-storyrun", "test-ns", "step2", downstreamStream)

	upstreamStream := newMockStream(ctx)
	upstreamStream.On("Context").Return(ctx)
	upstreamStream.On("Recv").Return(nil, nil)

	payload, _ := structpb.NewStruct(map[string]any{"text": "hello"})
	inputs, _ := structpb.NewStruct(map[string]any{"foo": "bar"})
	req := &transportpb.ProcessRequest{
		Packet: &transportpb.DataPacket{
			Payload: payload,
			Inputs:  inputs,
		},
	}
	upstreamStream.RecvChan <- req

	go func() {
		err := s.Process(upstreamStream)
		require.NoError(t, err)
	}()

	select {
	case received := <-downstreamStream.SentChan:
		require.NotNil(t, received.Packet.Payload)
		require.NotNil(t, received.Packet.Inputs)
		assert.Equal(t, "hello", received.Packet.Payload.GetFields()["text"].GetStringValue())
		assert.Equal(t, "bar", received.Packet.Inputs.GetFields()["foo"].GetStringValue())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for hot-path packet")
	}
	close(upstreamStream.RecvChan)
}

func TestProcessPacket_HotTransportBypassesEvaluationAfterParallel(t *testing.T) {
	engram1 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "ingress", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	engram2 := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "responder", Namespace: "test-ns"},
		Spec:       bubuv1alpha1.EngramSpec{Mode: enums.WorkloadModeDeployment},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "test-story", Namespace: "test-ns"},
		Spec: bubuv1alpha1.StorySpec{
			Transports: []bubuv1alpha1.StoryTransport{
				{Name: "rt", TransportRef: "rt"},
			},
			Steps: []bubuv1alpha1.Step{
				{ID: "step1", Name: "ingress-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "ingress"}}, Transport: "rt"},
				{ID: "parallel", Name: "parallel-step", Type: enums.StepTypeParallel, Needs: []string{"step1"}, Transport: "rt"},
				{ID: "step2", Name: "responder-step", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "responder"}}, Needs: []string{"step1"}, Transport: "rt"},
			},
		},
	}
	story.Status.Transports = []bubuv1alpha1.StoryTransportStatus{
		{Name: "rt", TransportRef: "rt", Mode: enums.TransportModeHot},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storyrun", Namespace: "test-ns"},
		Spec:       runsv1alpha1.StoryRunSpec{StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}}},
	}

	s := newTestServer(t, story, storyRun, engram1, engram2)
	sent := make(chan *transportpb.DataPacket, 1)
	downstreamStream := &mockProcessServer{
		sendFunc: func(packet *transportpb.DataPacket) error {
			sent <- packet
			return nil
		},
	}
	s.streamManager.AddStream(context.Background(), storyRun.Name, storyRun.Namespace, "step2", downstreamStream)

	payload, err := structpb.NewStruct(map[string]any{"text": "hello"})
	require.NoError(t, err)
	inputs, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)

	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, storyRun.Namespace, "step1", &transportpb.DataPacket{
		Payload:  payload,
		Inputs:   inputs,
		Envelope: &transportpb.StreamEnvelope{StreamId: "upstream-stream", Sequence: 23, Partition: "fanout-0"},
	}))

	select {
	case packet := <-sent:
		require.NotNil(t, packet.Payload)
		require.NotNil(t, packet.Inputs)
		assert.Equal(t, "hello", packet.Payload.GetFields()["text"].GetStringValue())
		assert.Equal(t, "bar", packet.Inputs.GetFields()["foo"].GetStringValue())
		require.NotNil(t, packet.GetEnvelope())
		assert.Equal(t, buildStreamID(storyRun.Name, storyRun.Namespace, "step2"), packet.GetEnvelope().GetStreamId())
		assert.Equal(t, uint64(0), packet.GetEnvelope().GetSequence())
		assert.Equal(t, "fanout-0", packet.GetEnvelope().GetPartition())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for hot-path packet after parallel")
	}
}

func TestProcessPacket_ExecuteStoryFireAndForgetReportsChildFailure(t *testing.T) {
	ns := "ns"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}},
				},
				{
					Name:  "step-exec",
					Needs: []string{"step-a"},
					Type:  enums.StepTypeExecuteStory,
					With: rawExtensionFromMap(t, map[string]any{
						"storyRef": map[string]any{"name": "child-story"},
					}),
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	s := newTestServer(t, story, storyRun)
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{Payload: payload}

	// Fire-and-forget: processPacket returns nil (success) immediately.
	require.NoError(t, s.processPacket(context.Background(), storyRun.Name, ns, "step-a", packet))

	// Find the created child StoryRun.
	var list runsv1alpha1.StoryRunList
	err = s.client.List(context.Background(), &list, client.InNamespace(ns), client.MatchingLabels{
		contracts.ParentStoryRunLabel: storyRun.Name,
		contracts.ParentStepLabel:     "step-exec",
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	child := &list.Items[0]

	// Simulate child failure.
	child.Status.Phase = enums.PhaseFailed
	child.Status.Message = "child engram crashed"
	require.NoError(t, s.client.Status().Update(context.Background(), child))

	// The background watcher polls every substoryPollInterval. Wait for it to
	// detect the failure and patch the parent's Degraded condition.
	require.Eventually(t, func() bool {
		var parent runsv1alpha1.StoryRun
		if err := s.client.Get(context.Background(), client.ObjectKeyFromObject(storyRun), &parent); err != nil {
			return false
		}
		for _, c := range parent.Status.Conditions {
			if c.Type == "Degraded" && c.Status == metav1.ConditionTrue {
				return strings.Contains(c.Message, "child engram crashed")
			}
		}
		return false
	}, 15*time.Second, 500*time.Millisecond, "parent StoryRun should have Degraded condition after fire-and-forget child failure")
}

func TestHandleBatchEngramIfNeeded_ReportsFailureOnParent(t *testing.T) {
	ns := "ns"
	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-engram", Namespace: ns},
		Spec: bubuv1alpha1.EngramSpec{
			Mode:        enums.WorkloadModeJob,
			TemplateRef: refs.EngramTemplateReference{Name: "tmpl"},
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: ns},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "batch-step",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "batch-engram"}},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "storyrun", Namespace: ns},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	s := newTestServer(t, story, storyRun, engram)

	payload, err := structpb.NewStruct(map[string]any{"input": "data"})
	require.NoError(t, err)

	step := &story.Spec.Steps[0]

	// Start handleBatchEngramIfNeeded in background — it will block waiting for completion.
	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, batchErr := s.handleBatchEngramIfNeeded(ctx, storyRun, story, step, engram, payload, "batch-step")
		errCh <- batchErr
	}()

	// Wait for the batch StepRun to be created, then mark it as failed.
	require.Eventually(t, func() bool {
		var stepRunList runsv1alpha1.StepRunList
		if err := s.client.List(context.Background(), &stepRunList, client.InNamespace(ns)); err != nil {
			return false
		}
		for i := range stepRunList.Items {
			sr := &stepRunList.Items[i]
			if sr.Spec.StepID == "batch-step" {
				sr.Status.Phase = enums.PhaseFailed
				sr.Status.LastFailureMsg = "batch job OOMKilled"
				_ = s.client.Status().Update(context.Background(), sr)
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "batch StepRun should be created")

	// handleBatchEngramIfNeeded should return an error.
	select {
	case batchErr := <-errCh:
		require.Error(t, batchErr)
		assert.Contains(t, batchErr.Error(), "batch job OOMKilled")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for handleBatchEngramIfNeeded to return")
	}

	// The substory reporter should have patched the parent StoryRun with a Degraded condition.
	require.Eventually(t, func() bool {
		var parent runsv1alpha1.StoryRun
		if err := s.client.Get(context.Background(), client.ObjectKeyFromObject(storyRun), &parent); err != nil {
			return false
		}
		for _, c := range parent.Status.Conditions {
			if c.Type == "Degraded" && c.Status == metav1.ConditionTrue {
				return strings.Contains(c.Message, "batch job OOMKilled")
			}
		}
		return false
	}, 5*time.Second, 200*time.Millisecond, "parent StoryRun should have Degraded condition after batch step failure")
}
