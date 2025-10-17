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
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/contracts"
	bindinghelper "github.com/bubustack/bobrapet/pkg/transport/binding"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// GRPCTransport reconciles the transport for streaming StoryRuns using gRPC.
type GRPCTransport struct {
	client.Client
	Log logr.Logger
}

// ErrBindingPending indicates the operator has not created the TransportBinding yet.
var ErrBindingPending = errors.New("transport binding pending")

// NewGRPCTransport creates a new GRPCTransport.
func NewGRPCTransport(cli client.Client) *GRPCTransport {
	return &GRPCTransport{
		Client: cli,
		Log:    log.Log.WithName("grpc-transport"),
	}
}

// Reconcile configures the gRPC connections between engrams in a streaming story.
func (r *GRPCTransport) Reconcile(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) error {
	if storyRun == nil {
		return fmt.Errorf("storyRun must not be nil when reconciling transport")
	}
	logger := log.FromContext(ctx).WithName("grpc-transport")
	logger.Info("Reconciling gRPC transport for StoryRun")
	return r.reconcileStoryGraph(ctx, storyRun.Namespace, storyRun, story)
}

func (r *GRPCTransport) reconcileStoryGraph(ctx context.Context, workloadNamespace string, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) error {
	logger := log.FromContext(ctx).WithName("grpc-transport")
	var bindingPending bool

	for i := range story.Spec.Steps {
		currentStep := &story.Spec.Steps[i]
		if currentStep.Ref == nil {
			continue
		}

		currentEngramName := getEngramNameForStep(story, storyRun, currentStep)
		logger.Info("Configuring transport binding for engram", "engram", currentEngramName)
		if err := r.applyTransportUpdate(ctx, workloadNamespace, storyRun, story, currentStep, currentEngramName); err != nil {
			if errors.Is(err, ErrBindingPending) {
				bindingPending = true
				continue
			}
			logger.Error(err, "Failed to configure upstream for engram", "engram", currentEngramName)
			return err
		}
	}

	if bindingPending {
		logger.V(1).Info("Transport bindings pending creation; will retry once operator finishes seeding them")
		return ErrBindingPending
	}

	return nil
}

func (r *GRPCTransport) applyTransportUpdate(
	ctx context.Context,
	namespace string,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	step *bubuv1alpha1.Step,
	engramName string,
) error {
	logger := log.FromContext(ctx).WithName("grpc-transport")
	bindingName := ""
	switch {
	case storyRun != nil && storyRun.Name != "":
		bindingName = bindinghelper.Name(storyRun.Name, step.Name)
	case story != nil && story.Name != "":
		bindingName = bindinghelper.StoryScopedName(story.Name, step.Name)
	default:
		logger.Info("Unable to determine transport binding name; skipping status update",
			"engram", engramName, "step", step.Name)
		return nil
	}
	binding := &transportv1alpha1.TransportBinding{}
	key := types.NamespacedName{Name: bindingName, Namespace: namespace}
	if err := r.Get(ctx, key, binding); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Transport binding not yet created, deferring status update",
				"binding", bindingName, "step", step.Name)
			return ErrBindingPending
		}
		return err
	}
	original := binding.DeepCopy()
	binding.Status.Endpoint = getHubServiceDNS(namespace)
	binding.Status.ObservedGeneration = binding.Generation
	cm := conditions.NewConditionManager(binding.Generation)
	resolved := strings.TrimSpace(binding.Status.Endpoint) != ""
	if resolved {
		cm.SetReadyCondition(&binding.Status.Conditions, true, conditions.ReasonTransportReady, "Transport binding resolved")
	} else {
		cm.SetReadyCondition(&binding.Status.Conditions, false, conditions.ReasonReconciling, "Resolving transport endpoints")
	}
	if err := r.Status().Patch(ctx, binding, client.MergeFrom(original)); err != nil {
		return err
	}
	annotationMsg := fmt.Sprintf("transport binding %s resolving endpoints", bindingName)
	if resolved {
		annotationMsg = fmt.Sprintf("transport binding %s resolved", bindingName)
	}
	r.markEngramTransportStatus(ctx, namespace, engramName, resolved, annotationMsg)
	logger.Info("Updated transport binding status",
		"binding", bindingName,
		"endpoint", binding.Status.Endpoint)
	return nil
}

func getEngramNameForStep(story *bubuv1alpha1.Story, storyRun *runsv1alpha1.StoryRun, step *bubuv1alpha1.Step) string {
	if step == nil {
		return ""
	}
	if storyRun != nil && storyRun.Name != "" {
		return composeWorkloadName(storyRun.Name, step.Name)
	}
	if story != nil && story.Name != "" {
		return composeWorkloadName(story.Name, step.Name)
	}
	if step.Ref != nil && step.Ref.Name != "" {
		return step.Ref.Name
	}
	return composeWorkloadName(step.Name)
}

func getHubServiceDNS(workloadNamespace string) string {
	if endpoint := strings.TrimSpace(os.Getenv(contracts.HubEndpointEnv)); endpoint != "" {
		return endpoint
	}

	serviceName := strings.TrimSpace(os.Getenv(contracts.HubServiceNameEnv))
	if serviceName == "" {
		serviceName = "bobravoz-grpc-hub"
	}

	serviceNamespace := strings.TrimSpace(os.Getenv(contracts.HubServiceNamespaceEnv))
	if serviceNamespace == "" {
		serviceNamespace = workloadNamespace
	}

	clusterDomain := strings.TrimSpace(os.Getenv(contracts.HubClusterDomainEnv))
	if clusterDomain == "" {
		clusterDomain = "svc.cluster.local"
	}

	port := 9000
	if v := strings.TrimSpace(os.Getenv(contracts.HubPortEnv)); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			port = parsed
		}
	}

	host := serviceName
	if serviceNamespace != "" {
		host = fmt.Sprintf("%s.%s", host, serviceNamespace)
	}
	if clusterDomain != "" {
		host = fmt.Sprintf("%s.%s", host, clusterDomain)
	}
	if port > 0 {
		host = fmt.Sprintf("%s:%d", host, port)
	}
	return host
}

func (r *GRPCTransport) markEngramTransportStatus(ctx context.Context, namespace, engramName string, ready bool, message string) {
	if engramName == "" {
		return
	}

	key := types.NamespacedName{Namespace: namespace, Name: engramName}
	var engram bubuv1alpha1.Engram
	if err := r.Get(ctx, key, &engram); err != nil {
		if !apierrors.IsNotFound(err) {
			r.Log.Error(err, "Failed to resolve engram for transport annotation update", "namespace", namespace, "engram", engramName)
		}
		return
	}

	annotations := engram.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	readyValue := "false"
	if ready {
		readyValue = "true"
	}
	trimmedMessage := strings.TrimSpace(message)
	currentValue := annotations[contracts.TransportReadyAnnotation]
	currentMessage := strings.TrimSpace(annotations[contracts.TransportReadyMessageAnnotation])
	if currentValue == readyValue && currentMessage == trimmedMessage {
		return
	}

	before := engram.DeepCopy()
	annotations[contracts.TransportReadyAnnotation] = readyValue
	if trimmedMessage == "" {
		delete(annotations, contracts.TransportReadyMessageAnnotation)
	} else {
		annotations[contracts.TransportReadyMessageAnnotation] = trimmedMessage
	}
	engram.SetAnnotations(annotations)

	if err := r.Patch(ctx, &engram, client.MergeFrom(before)); err != nil {
		r.Log.Error(err, "Failed to record transport annotation on engram", "namespace", namespace, "engram", engramName)
	}
}

func (r *GRPCTransport) EnsureCleanUp(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) error {
	logger := log.FromContext(ctx).WithName("grpc-transport")
	logger.Info("Skipping gRPC transport cleanup; streaming runtimes are owned by StepRuns and removed via garbage collection")
	return nil
}

// composeWorkloadName matches the operator's DNS-safe naming behavior.
func composeWorkloadName(parts ...string) string {
	base := strings.Join(parts, "-")
	if len(base) <= validation.DNS1123LabelMaxLength {
		return base
	}

	hasher := fnv.New32a()
	for _, part := range parts {
		_, _ = hasher.Write([]byte(part))
		_, _ = hasher.Write([]byte{0})
	}
	suffix := fmt.Sprintf("%08x", hasher.Sum32())

	prefixLen := validation.DNS1123LabelMaxLength - len(suffix) - 1
	if prefixLen < 1 {
		prefixLen = validation.DNS1123LabelMaxLength - len(suffix)
	}
	if prefixLen < 1 {
		if len(suffix) > validation.DNS1123LabelMaxLength {
			return suffix[:validation.DNS1123LabelMaxLength]
		}
		return suffix
	}

	prefix := base[:prefixLen]
	prefix = strings.TrimSuffix(prefix, "-")
	if len(prefix) == 0 {
		prefix = strings.Trim(base[:prefixLen], "-")
		if len(prefix) == 0 {
			prefix = "resource"
		}
	}

	result := fmt.Sprintf("%s-%s", prefix, suffix)
	if len(result) > validation.DNS1123LabelMaxLength {
		result = result[:validation.DNS1123LabelMaxLength]
		result = strings.TrimSuffix(result, "-")
		if len(result) == 0 {
			if len(suffix) > validation.DNS1123LabelMaxLength {
				return suffix[:validation.DNS1123LabelMaxLength]
			}
			return suffix
		}
	}
	return result
}
