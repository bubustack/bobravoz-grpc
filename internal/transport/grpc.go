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
	"sync"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	pkgtransport "github.com/bubustack/bobrapet/pkg/transport"
	bindinghelper "github.com/bubustack/bobrapet/pkg/transport/binding"
	transportmetrics "github.com/bubustack/bobravoz-grpc/pkg/metrics"
	"github.com/bubustack/core/contracts"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// GRPCTransport reconciles the transport for streaming StoryRuns using gRPC.
type GRPCTransport struct {
	client.Client
	Log      logr.Logger
	Recorder events.EventRecorder

	annotationFailureMu   sync.Mutex
	annotationFailureLast map[string]time.Time
}

// ErrBindingPending indicates the operator has not created the TransportBinding yet.
var ErrBindingPending = errors.New("transport binding pending")

const (
	transportReadyMessageMaxLen             = 512
	eventReasonEngramTransportAnnotationErr = "EngramTransportAnnotationFailed"
	annotationFailureEventInterval          = time.Minute
)

// NewGRPCTransport creates a new GRPCTransport.
func NewGRPCTransport(cli client.Client) *GRPCTransport {
	return &GRPCTransport{
		Client: cli,
		Log:    log.Log.WithName("grpc-transport"),
	}
}

func (r *GRPCTransport) SetRecorder(recorder events.EventRecorder) {
	r.Recorder = recorder
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
	if err := r.markEngramTransportStatus(ctx, namespace, engramName, resolved, annotationMsg, binding); err != nil {
		return err
	}
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

// markEngramTransportStatus fetches the Engram and patches
// TransportReady/TransportReadyMessage when the ready flag or trimmed message
// changes. Delegates core patch logic to coretransport.EngramAnnotationPatcher.
func (r *GRPCTransport) markEngramTransportStatus(ctx context.Context, namespace, engramName string, ready bool, message string, binding *transportv1alpha1.TransportBinding) error {
	if engramName == "" {
		return nil
	}

	patcher := pkgtransport.NewEngramAnnotationPatcher(r.Client, r.Log)
	patched, err := patcher.PatchReadyStatus(ctx, namespace, engramName, ready, message, func(ctx context.Context, key types.NamespacedName) (client.Object, error) {
		var engram bubuv1alpha1.Engram
		if err := r.Get(ctx, key, &engram); err != nil {
			return nil, err
		}
		return &engram, nil
	})

	if err != nil {
		r.Log.Error(err, "Failed to record transport annotation on engram", "namespace", namespace, "engram", engramName)
		if r.Recorder != nil && r.shouldEmitAnnotationFailureEvent(namespace, engramName, time.Now()) {
			r.Recorder.Eventf(
				&bubuv1alpha1.Engram{
					ObjectMeta: metav1.ObjectMeta{
						Name:      engramName,
						Namespace: namespace,
					},
				}, nil,
				corev1.EventTypeWarning,
				eventReasonEngramTransportAnnotationErr, "Reconcile",
				"Failed to patch transport readiness annotations for %s/%s: %v",
				namespace,
				engramName,
				err,
			)
		}
		return err
	}

	if patched {
		transportmetrics.RecordEngramTransportReadyChange(ready)
		r.Log.Info("Patched Engram transport readiness",
			"namespace", namespace,
			"engram", engramName,
			"ready", ready,
			"message", pkgtransport.TruncateMessage(message, pkgtransport.MaxReadyMessageLength),
			"codecs", summarizeBindingCodecs(binding),
		)
	}
	return nil
}

func summarizeBindingCodecs(binding *transportv1alpha1.TransportBinding) string {
	return fmt.Sprintf(
		"audio=%s video=%s binary=%s",
		summarizeAudioCodec(binding),
		summarizeVideoCodec(binding),
		summarizeBinaryCodec(binding),
	)
}

const codecSummaryUnknown = "unknown"

func summarizeAudioCodec(binding *transportv1alpha1.TransportBinding) string {
	if binding == nil {
		return codecSummaryUnknown
	}
	if binding.Status.NegotiatedAudio != nil && binding.Status.NegotiatedAudio.Name != "" {
		return binding.Status.NegotiatedAudio.Name
	}
	if binding.Spec.Audio != nil && len(binding.Spec.Audio.Codecs) > 0 && binding.Spec.Audio.Codecs[0].Name != "" {
		return binding.Spec.Audio.Codecs[0].Name
	}
	return codecSummaryUnknown
}

func summarizeVideoCodec(binding *transportv1alpha1.TransportBinding) string {
	if binding == nil {
		return codecSummaryUnknown
	}
	if binding.Status.NegotiatedVideo != nil && binding.Status.NegotiatedVideo.Name != "" {
		return binding.Status.NegotiatedVideo.Name
	}
	if binding.Spec.Video != nil && len(binding.Spec.Video.Codecs) > 0 && binding.Spec.Video.Codecs[0].Name != "" {
		return binding.Spec.Video.Codecs[0].Name
	}
	if binding.Spec.Video != nil && binding.Spec.Video.Raw {
		return "raw"
	}
	return codecSummaryUnknown
}

func summarizeBinaryCodec(binding *transportv1alpha1.TransportBinding) string {
	if binding == nil {
		return codecSummaryUnknown
	}
	if strings.TrimSpace(binding.Status.NegotiatedBinary) != "" {
		return binding.Status.NegotiatedBinary
	}
	if binding.Spec.Binary != nil && len(binding.Spec.Binary.MimeTypes) > 0 {
		return binding.Spec.Binary.MimeTypes[0]
	}
	return codecSummaryUnknown
}

func (r *GRPCTransport) shouldEmitAnnotationFailureEvent(namespace, engram string, now time.Time) bool {
	if namespace == "" || engram == "" {
		return true
	}
	key := fmt.Sprintf("%s/%s", namespace, engram)
	r.annotationFailureMu.Lock()
	defer r.annotationFailureMu.Unlock()
	if r.annotationFailureLast == nil {
		r.annotationFailureLast = make(map[string]time.Time)
	}
	// Evict stale entries to prevent unbounded map growth. Entries older than
	// 15 minutes are removed since they are no longer useful for dedup.
	const evictionTTL = 15 * time.Minute
	for k, v := range r.annotationFailureLast {
		if now.Sub(v) > evictionTTL {
			delete(r.annotationFailureLast, k)
		}
	}
	if last, ok := r.annotationFailureLast[key]; ok && now.Sub(last) < annotationFailureEventInterval {
		return false
	}
	r.annotationFailureLast[key] = now
	return true
}

// EnsureCleanUp walks each streaming step, recomputes the run-scoped Engram name,
// and marks the Engram transport readiness annotations false so the Stage-to-stage
// guard sees the cleanup signal before finalizers are removed
// (internal/transport/grpc.go:301-335).
func (r *GRPCTransport) EnsureCleanUp(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) (int, error) {
	logger := log.FromContext(ctx).WithName("grpc-transport")
	if story == nil {
		logger.Info("Skipping gRPC transport cleanup; story is nil")
		return 0, nil
	}

	namespace := story.Namespace
	if storyRun != nil && storyRun.Namespace != "" {
		namespace = storyRun.Namespace
	}
	if strings.TrimSpace(namespace) == "" {
		logger.Info("Skipping gRPC transport cleanup; namespace is unknown")
		return 0, nil
	}

	var cleaned int
	storyRunName := ""
	if storyRun != nil {
		storyRunName = storyRun.Name
	}
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if step.Ref == nil {
			continue
		}
		engramName := getEngramNameForStep(story, storyRun, step)
		if engramName == "" {
			continue
		}
		msg := fmt.Sprintf("transport binding %s cleaned up", step.Name)
		if err := r.markEngramTransportStatus(ctx, namespace, engramName, false, msg, nil); err != nil {
			logger.Error(err, "Failed to reset Engram transport readiness during cleanup", "engram", engramName)
			return cleaned, err
		}
		cleaned++
	}
	transportmetrics.RecordTransportCleanup(namespace, cleaned)
	logger.Info("Finished gRPC transport cleanup", "namespace", namespace, "storyRun", storyRunName, "engramAnnotationsReset", cleaned)
	return cleaned, nil
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
