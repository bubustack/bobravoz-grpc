package main

import (
	"errors"
	"testing"

	"github.com/bubustack/core/contracts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

type stubHealthRegistrar struct {
	healthzCount int
	readyzCount  int
	healthzErr   error
	readyzErr    error
}

func (s *stubHealthRegistrar) AddHealthzCheck(name string, check healthz.Checker) error {
	if name != "healthz" {
		return errors.New("unexpected healthz check name")
	}
	s.healthzCount++
	if check == nil {
		return errors.New("healthz checker is nil")
	}
	return s.healthzErr
}

func (s *stubHealthRegistrar) AddReadyzCheck(name string, check healthz.Checker) error {
	if name != "readyz" {
		return errors.New("unexpected readyz check name")
	}
	s.readyzCount++
	if check == nil {
		return errors.New("readyz checker is nil")
	}
	return s.readyzErr
}

func TestRegisterHealthChecksRegistersEachCheckOnce(t *testing.T) {
	mgr := &stubHealthRegistrar{}
	if err := registerHealthChecks(mgr); err != nil {
		t.Fatalf("registerHealthChecks returned error: %v", err)
	}
	if mgr.healthzCount != 1 {
		t.Fatalf("expected 1 healthz registration, got %d", mgr.healthzCount)
	}
	if mgr.readyzCount != 1 {
		t.Fatalf("expected 1 readyz registration, got %d", mgr.readyzCount)
	}
}

func TestRegisterHealthChecksReturnsReadyzError(t *testing.T) {
	wantErr := errors.New("readyz failed")
	mgr := &stubHealthRegistrar{readyzErr: wantErr}
	if err := registerHealthChecks(mgr); !errors.Is(err, wantErr) {
		t.Fatalf("expected readyz error %v, got %v", wantErr, err)
	}
}

func TestResolveSecurityModeDefaultsToTLS(t *testing.T) {
	t.Setenv(contracts.TransportSecurityModeEnv, "")
	mode, explicitlySet, err := resolveSecurityMode()
	if err != nil {
		t.Fatalf("resolveSecurityMode returned error: %v", err)
	}
	if mode != contracts.TransportSecurityModeTLS {
		t.Fatalf("expected default tls mode, got %s", mode)
	}
	if !explicitlySet {
		t.Fatalf("expected blank env to be treated as explicitly set")
	}
}

func TestResolveSecurityModeRejectsLegacyPlaintext(t *testing.T) {
	t.Setenv(contracts.TransportSecurityModeEnv, "plaintext")
	if _, _, err := resolveSecurityMode(); err == nil {
		t.Fatalf("expected legacy plaintext mode to be rejected")
	}
}

func TestInferConnectorImageReturnsManagerContainerImage(t *testing.T) {
	t.Setenv("POD_NAME", "manager-pod")
	t.Setenv("POD_NAMESPACE", "bobrapet-system")

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}

	reader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "manager-pod",
				Namespace: "bobrapet-system",
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "manager", Image: "ghcr.io/bubustack/bobravoz-grpc:v1.2.3"},
				},
			},
		}).
		Build()

	got, err := inferConnectorImage(t.Context(), reader)
	if err != nil {
		t.Fatalf("inferConnectorImage returned error: %v", err)
	}
	if want := "ghcr.io/bubustack/bobravoz-grpc:v1.2.3"; got != want {
		t.Fatalf("expected image %q, got %q", want, got)
	}
}
