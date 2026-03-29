package pod

import (
	"context"
	"encoding/json"
	"testing"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	catalogv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/transport/bindinginfo"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDecodeBindingInfo(t *testing.T) {
	info := &transportpb.BindingInfo{
		Payload: []byte(`{"env":{"FOO":"bar","bad-name":"noop"}}`),
	}
	payload, err := protojson.Marshal(info)
	if err != nil {
		t.Fatalf("marshal binding info: %v", err)
	}
	envelope := map[string]any{
		"name":      "binding-demo",
		"namespace": "default",
		"binding":   json.RawMessage(payload),
	}
	bytes, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal binding envelope: %v", err)
	}

	result, err := decodeBindingInfo(string(bytes))
	if err != nil {
		t.Fatalf("decode binding info: %v", err)
	}
	if result == nil {
		t.Fatalf("expected binding info to be returned")
	}
	overrides := bindinginfo.EnvOverrides(result)
	if overrides["FOO"] != "bar" {
		t.Fatalf("expected FOO override to be bar, got %s", overrides["FOO"])
	}
	if overrides["bad-name"] != "noop" {
		t.Fatalf("expected raw bad-name override to be preserved in map")
	}
}

func TestAppendEnvOverrides(t *testing.T) {
	base := []corev1.EnvVar{{Name: "FOO", Value: "existing"}}
	info := &transportpb.BindingInfo{Payload: []byte(`{"env":{"FOO":"override","_VALID_":"1","1INVALID":"skip"}}`)}
	envs := coretransport.AppendBindingEnvOverrides(base, info)
	if got := lookupEnv(envs, "FOO"); got != "override" {
		t.Fatalf("expected FOO override to win, got %s", got)
	}
	if got := lookupEnv(envs, "_VALID_"); got != "" {
		t.Fatalf("expected _VALID_ env to be skipped unless declared or BUBU-prefixed, got %s", got)
	}
	if got := lookupEnv(envs, "1INVALID"); got != "" {
		t.Fatalf("expected invalid env name to be ignored, got %s", got)
	}
}

func TestInjectConnectorPropagatesEnvOverrides(t *testing.T) {
	info := &transportpb.BindingInfo{
		Payload: []byte(`{"env":{"` + contracts.GRPCMessageTimeoutEnv + `":"25s"}}`),
	}
	bindingPayload, err := protojson.Marshal(info)
	if err != nil {
		t.Fatalf("marshal binding info: %v", err)
	}
	envelope := map[string]any{
		"name":      "binding-demo",
		"namespace": "default",
		"binding":   json.RawMessage(bindingPayload),
	}
	bindingJSON, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "engram",
				Env: []corev1.EnvVar{{
					Name:  contracts.TransportSecurityModeEnv,
					Value: "plaintext",
				}, {
					Name:  contracts.ConnectorGenerationEnv,
					Value: "7",
				}},
			}},
		},
	}
	engram := &catalogv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
			Annotations: map[string]string{
				contracts.TransportBindingAnnotation: string(bindingJSON),
			},
		},
	}

	webhook := NewConnectorWebhook(nil, "image", corev1.PullIfNotPresent)
	if err := webhook.injectConnector(pod, engram, string(bindingJSON)); err != nil {
		t.Fatalf("inject connector: %v", err)
	}

	if len(pod.Spec.Containers) != 2 {
		t.Fatalf("expected connector container to be injected, got %d containers", len(pod.Spec.Containers))
	}
	connector := pod.Spec.Containers[1]
	if got := lookupEnv(connector.Env, contracts.GRPCMessageTimeoutEnv); got != "25s" {
		t.Fatalf("expected connector to receive message timeout override, got %s", got)
	}
	if got := lookupEnv(connector.Env, contracts.TransportSecurityModeEnv); got != contracts.TransportSecurityModeTLS {
		t.Fatalf("expected connector transport security mode to be forced to tls, got %s", got)
	}
	if got := lookupEnv(connector.Env, contracts.ConnectorGenerationEnv); got != "7" {
		t.Fatalf("expected connector to receive connector generation, got %s", got)
	}
	if got := pod.Labels[ConnectorInjectedLabel()]; got != injectedLabelValue {
		t.Fatalf("expected connector label to be applied, got %q", got)
	}
}

func TestResolveBindingEnvValueFromSanitizedAnnotation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := transportv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add transport scheme: %v", err)
	}
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-binding",
			Namespace: "default",
		},
	}
	binding.Spec.TransportRef = "grpc-demo"
	binding.Spec.Driver = "driver"
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(binding).Build()

	webhook := NewConnectorWebhook(client, "image", corev1.PullIfNotPresent)
	value, err := webhook.resolveBindingEnvValue(context.Background(), "default", "default/demo-binding")
	if err != nil {
		t.Fatalf("resolve binding env value: %v", err)
	}
	info, err := bindinginfo.Decode(value)
	if err != nil {
		t.Fatalf("decode binding info: %v", err)
	}
	if info.GetTransportRef() != "grpc-demo" {
		t.Fatalf("expected transport ref grpc-demo, got %s", info.GetTransportRef())
	}
}

func TestResolveBindingEnvValueRejectsCrossNamespaceReference(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := transportv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add transport scheme: %v", err)
	}
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	webhook := NewConnectorWebhook(client, "image", corev1.PullIfNotPresent)
	_, err := webhook.resolveBindingEnvValue(context.Background(), "default", "other/demo-binding")
	if err == nil {
		t.Fatal("expected cross-namespace binding reference to be rejected")
	}
}

func TestWebhookPodNoContainers(t *testing.T) {
	// A pod with empty Spec.Containers should be handled gracefully (not panic).
	// The webhook should skip injection and not panic.

	info := &transportpb.BindingInfo{
		Payload: []byte(`{"env":{"FOO":"bar"}}`),
	}
	bindingPayload, err := protojson.Marshal(info)
	if err != nil {
		t.Fatalf("marshal binding info: %v", err)
	}
	envelope := map[string]any{
		"name":      "binding-demo",
		"namespace": "default",
		"binding":   json.RawMessage(bindingPayload),
	}
	bindingJSON, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	// Pod with no containers
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "empty-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{}, // Empty containers slice
		},
	}

	engram := &catalogv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
			Annotations: map[string]string{
				contracts.TransportBindingAnnotation: string(bindingJSON),
			},
		},
	}

	webhook := NewConnectorWebhook(nil, "image", corev1.PullIfNotPresent)

	// This should NOT panic even with empty containers
	if err := webhook.injectConnector(pod, engram, string(bindingJSON)); err != nil {
		t.Fatalf("inject connector: %v", err)
	}

	// Verify the pod was not modified (no containers added)
	if len(pod.Spec.Containers) != 0 {
		t.Fatalf("expected no containers to be added to empty pod, got %d", len(pod.Spec.Containers))
	}

	// With no containers, injectConnector returns early without labeling.
	if _, ok := pod.Labels[ConnectorInjectedLabel()]; ok {
		t.Fatal("expected no connector label on empty pod")
	}
}

func TestWebhookPodNilContainers(t *testing.T) {
	// A pod with nil Spec.Containers should also be handled gracefully.

	info := &transportpb.BindingInfo{
		Payload: []byte(`{"env":{"BAR":"baz"}}`),
	}
	bindingPayload, err := protojson.Marshal(info)
	if err != nil {
		t.Fatalf("marshal binding info: %v", err)
	}
	envelope := map[string]any{
		"name":      "binding-nil",
		"namespace": "default",
		"binding":   json.RawMessage(bindingPayload),
	}
	bindingJSON, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	// Pod with nil containers (not just empty)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nil-containers-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: nil, // Nil containers
		},
	}

	engram := &catalogv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
			Annotations: map[string]string{
				contracts.TransportBindingAnnotation: string(bindingJSON),
			},
		},
	}

	webhook := NewConnectorWebhook(nil, "image", corev1.PullIfNotPresent)

	// This should NOT panic with nil containers
	if err := webhook.injectConnector(pod, engram, string(bindingJSON)); err != nil {
		t.Fatalf("inject connector: %v", err)
	}

	// Verify no containers were added (or it remained nil/empty)
	if len(pod.Spec.Containers) != 0 {
		t.Fatalf("expected no containers to be added to nil-containers pod, got %d", len(pod.Spec.Containers))
	}
}

func TestWebhookDecodeBindingInfoEmpty(t *testing.T) {
	// Empty binding value should return nil, not error
	result, err := decodeBindingInfo("")
	if err != nil {
		t.Fatalf("unexpected error for empty binding: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil for empty binding, got %+v", result)
	}

	// Whitespace-only should also return nil
	result, err = decodeBindingInfo("   ")
	if err != nil {
		t.Fatalf("unexpected error for whitespace binding: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil for whitespace binding, got %+v", result)
	}
}

func TestWebhookDecodeBindingInfoMalformed(t *testing.T) {
	// Malformed JSON should return an error
	_, err := decodeBindingInfo("not-valid-json")
	if err == nil {
		t.Fatal("expected error for malformed binding")
	}

	// Valid JSON but not a binding info structure — should not panic.
	// Whether this returns an error depends on the decoder; just verify no panic.
	_, _ = decodeBindingInfo(`{"random": "data"}`)
}
