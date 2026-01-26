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

package transport

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func TestGetHubServiceDNS_DefaultsIncludePort(t *testing.T) {
	t.Setenv(contracts.HubEndpointEnv, "")
	t.Setenv(contracts.HubServiceNameEnv, "")
	t.Setenv(contracts.HubServiceNamespaceEnv, "")
	t.Setenv(contracts.HubClusterDomainEnv, "")
	t.Setenv(contracts.HubPortEnv, "")

	got := getHubServiceDNS("work-ns")
	want := "bobravoz-grpc-hub.work-ns.svc.cluster.local:9000"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestGetHubServiceDNS_WithEnvOverrides(t *testing.T) {
	t.Setenv(contracts.HubEndpointEnv, "")
	t.Setenv(contracts.HubServiceNameEnv, "custom-hub")
	t.Setenv(contracts.HubServiceNamespaceEnv, "operator-ns")
	t.Setenv(contracts.HubClusterDomainEnv, "svc.corp.local")
	t.Setenv(contracts.HubPortEnv, "9443")

	got := getHubServiceDNS("ignored")
	want := "custom-hub.operator-ns.svc.corp.local:9443"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestGetHubServiceDNS_EndpointOverrideWins(t *testing.T) {
	t.Setenv(contracts.HubEndpointEnv, "hub.example.com:8443")
	t.Setenv(contracts.HubServiceNameEnv, "custom-hub")
	t.Setenv(contracts.HubServiceNamespaceEnv, "operator-ns")
	t.Setenv(contracts.HubClusterDomainEnv, "svc.corp.local")
	t.Setenv(contracts.HubPortEnv, "9443")

	got := getHubServiceDNS("ignored")
	want := "hub.example.com:8443"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestGetEngramNameForStepPerStoryRunUsesDeterministicCompose(t *testing.T) {
	story := &bubuv1alpha1.Story{}
	story.Name = strings.Repeat("story", 3)
	step := &bubuv1alpha1.Step{Name: strings.Repeat("a", 40)}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: strings.Repeat("b", 40),
		},
	}

	got := getEngramNameForStep(story, storyRun, step)
	want := composeWorkloadName(storyRun.Name, step.Name)
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestGetEngramNameForStepPerStoryMatchesStoryController(t *testing.T) {
	story := &bubuv1alpha1.Story{}
	story.Name = strings.Repeat("story", 3)
	step := &bubuv1alpha1.Step{Name: strings.Repeat("x", 40)}

	got := getEngramNameForStep(story, nil, step)
	want := composeWorkloadName(story.Name, step.Name)
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestGetEngramNameForStepFallsBackToRefName(t *testing.T) {
	refName := "preexisting-engram"
	step := &bubuv1alpha1.Step{
		Name: "unused",
		Ref: &refs.EngramReference{
			ObjectReference: refs.ObjectReference{Name: refName},
		},
	}

	if got := getEngramNameForStep(nil, nil, step); got != refName {
		t.Fatalf("expected %q, got %q", refName, got)
	}
}

func TestShouldEmitAnnotationFailureEventRespectsInterval(t *testing.T) {
	tr := &GRPCTransport{}
	now := time.Unix(0, 0)
	if !tr.shouldEmitAnnotationFailureEvent("default", "demo", now) {
		t.Fatalf("expected first emission to pass")
	}
	if tr.shouldEmitAnnotationFailureEvent("default", "demo", now.Add(10*time.Second)) {
		t.Fatalf("expected emissions within interval to be suppressed")
	}
	if !tr.shouldEmitAnnotationFailureEvent("default", "demo", now.Add(annotationFailureEventInterval+time.Second)) {
		t.Fatalf("expected emission after interval to be allowed")
	}
}

func TestApplyTransportUpdateReturnsPendingWhenBindingMissing(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := transportv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add transport scheme: %v", err)
	}
	tr := &GRPCTransport{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Log:    log.Log.WithName("test"),
	}
	story := &bubuv1alpha1.Story{}
	storyRun := &runsv1alpha1.StoryRun{}
	storyRun.Name = "storyrun"
	step := &bubuv1alpha1.Step{Name: "step"}

	err := tr.applyTransportUpdate(context.Background(), "default", storyRun, story, step, "engram")
	if !errors.Is(err, ErrBindingPending) {
		t.Fatalf("expected ErrBindingPending, got %v", err)
	}
}
