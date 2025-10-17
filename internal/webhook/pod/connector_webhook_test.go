package pod

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/contracts"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestBindingEnvOverrides(t *testing.T) {
	info := &transportpb.BindingInfo{
		Payload: []byte(`{"env":{"FOO":"bar","bad-name":"noop"}}`),
	}
	payload, err := protojson.Marshal(info)
	if err != nil {
		t.Fatalf("marshal binding info: %v", err)
	}
	envelope := map[string]any{
		"binding": json.RawMessage(payload),
	}
	bytes, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal binding envelope: %v", err)
	}

	overrides := bindingEnvOverrides(string(bytes))
	if overrides["FOO"] != "bar" {
		t.Fatalf("expected FOO override to be bar, got %s", overrides["FOO"])
	}
	if overrides["bad-name"] != "noop" {
		t.Fatalf("expected raw bad-name override to be preserved in map")
	}
}

func TestAppendEnvOverrides(t *testing.T) {
	base := []corev1.EnvVar{{Name: "FOO", Value: "existing"}}
	overrides := map[string]string{
		"FOO":      "override",
		"_VALID_":  "1",
		"1INVALID": "skip",
	}
	envs := appendEnvOverrides(base, overrides)
	if got := lookupEnv(envs, "FOO"); got != "override" {
		t.Fatalf("expected FOO override to win, got %s", got)
	}
	if got := lookupEnv(envs, "_VALID_"); got != "1" {
		t.Fatalf("expected _VALID_ env to be injected, got %s", got)
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
	envelope := map[string]any{"binding": json.RawMessage(bindingPayload)}
	bindingJSON, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "engram"}},
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
	webhook.injectConnector(pod, engram, string(bindingJSON))

	if len(pod.Spec.Containers) != 2 {
		t.Fatalf("expected connector container to be injected, got %d containers", len(pod.Spec.Containers))
	}
	connector := pod.Spec.Containers[1]
	if got := lookupEnv(connector.Env, contracts.GRPCMessageTimeoutEnv); got != "25s" {
		t.Fatalf("expected connector to receive message timeout override, got %s", got)
	}
}
