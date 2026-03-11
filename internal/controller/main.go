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

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	bobrapetconditions "github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bobravoz-grpc/internal/transport"
	grpcmetrics "github.com/bubustack/bobravoz-grpc/pkg/metrics"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	transportFinalizer                    = "transport.bobravoz.bubustack.io/finalizer"
	bindingPendingMinRequeue              = 2 * time.Second
	bindingPendingMaxRequeue              = 30 * time.Second
	transportCleanedReason                = "TransportCleaned"
	eventReasonTransportReconcileFail     = "TransportReconcileFailed"
	eventReasonTransportReady             = "TransportReady"
	eventReasonTransportCleanupFailed     = "TransportCleanupFailed"
	eventReasonTransportCleanupSucceeded  = "TransportCleanupComplete"
	eventReasonInvalidTransportAnnotation = "InvalidTransportAnnotation"
	eventReasonParentStoryMissing         = "ParentStoryMissing"
)

// TransportReconciler reconciles a StoryRun object for streaming transport
type TransportReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

type recorderAwareTransport interface {
	SetRecorder(record.EventRecorder)
}

// +kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=engramtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=transport.bubustack.io,resources=transports,verbs=get;list;watch
// +kubebuilder:rbac:groups=transport.bubustack.io,resources=transportbindings,verbs=get;list;watch
// +kubebuilder:rbac:groups=transport.bubustack.io,resources=transportbindings/status,verbs=get;update;patch

// Reconcile loads the StoryRun and parent Story, skips non-streaming stories,
// patches ConditionTransportReady when transport annotations are invalid, and
// delegates to reconcileNormal or reconcileDelete based on DeletionTimestamp
// (`internal/controller/main.go:60-140`).
func (r *TransportReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("transport-reconciler")

	var storyRun runsv1alpha1.StoryRun
	if err := r.Get(ctx, req.NamespacedName, &storyRun); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// Fetch the parent Story to check the pattern
	var story bubuv1alpha1.Story
	storyNamespace := refs.ResolveNamespace(&storyRun, &storyRun.Spec.StoryRef.ObjectReference)
	storyKey := client.ObjectKey{Namespace: storyNamespace, Name: storyRun.Spec.StoryRef.Name}
	if err := r.Get(ctx, storyKey, &story); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Parent Story missing; skipping transport reconciliation",
				"storyRun", req.NamespacedName,
				"storyRef", storyRun.Spec.StoryRef,
			)
			if r.Recorder != nil {
				r.Recorder.Eventf(
					&storyRun,
					corev1.EventTypeWarning,
					eventReasonParentStoryMissing,
					"Parent Story %s/%s missing; skipping transport reconciliation for %s/%s",
					storyKey.Namespace,
					storyKey.Name,
					storyRun.Namespace,
					storyRun.Name,
				)
			}
			if storyRun.GetDeletionTimestamp() != nil && controllerutil.ContainsFinalizer(&storyRun, transportFinalizer) {
				controllerutil.RemoveFinalizer(&storyRun, transportFinalizer)
				if err := r.Update(ctx, &storyRun); err != nil {
					return ctrl.Result{}, err
				}
			}
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get parent Story for StoryRun",
			"storyRun", req.NamespacedName,
			"storyRef", storyRun.Spec.StoryRef,
		)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// This controller only cares about streaming patterns
	if story.Spec.Pattern != enums.StreamingPattern {
		return ctrl.Result{}, nil
	}

	transportType := strings.TrimSpace(story.Annotations[transport.AnnotationTransport])
	if transportType == "" {
		transportType = transport.GRPCTransportType
	}

	transportImpl, err := r.getTransportForStory(&story)
	if err != nil {
		logger.Error(err, "Failed to get transport for story")
		if r.Recorder != nil {
			r.Recorder.Eventf(
				&storyRun,
				corev1.EventTypeWarning,
				eventReasonInvalidTransportAnnotation,
				"Transport annotation %s invalid: %v",
				transport.AnnotationTransport,
				err,
			)
		}
		cm := bobrapetconditions.NewConditionManager(storyRun.Generation)
		patch := client.MergeFrom(storyRun.DeepCopy())
		cm.SetCondition(&storyRun.Status.Conditions, bobrapetconditions.ConditionTransportReady, "False", bobrapetconditions.ReasonInvalidConfiguration, err.Error())
		if perr := r.Status().Patch(ctx, &storyRun, patch); perr != nil {
			return ctrl.Result{}, perr
		}
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		if recorderAware, ok := transportImpl.(recorderAwareTransport); ok {
			recorderAware.SetRecorder(r.Recorder)
		}
	}

	cm := bobrapetconditions.NewConditionManager(storyRun.Generation)

	// Handle deletion
	if storyRun.GetDeletionTimestamp() != nil {
		return r.reconcileDelete(ctx, &storyRun, &story, transportImpl, transportType, cm)
	}

	return r.reconcileNormal(ctx, &storyRun, &story, transportImpl, transportType, cm)
}

// getTransportForStory reads transport.AnnotationTransport (defaulting to the
// built-in gRPC transport) and returns the registered implementation so the
// reconciler can delegate work.
func (r *TransportReconciler) getTransportForStory(story *bubuv1alpha1.Story) (transport.Transport, error) {
	transportType := transport.GRPCTransportType // Default transport
	if t, ok := story.Annotations[transport.AnnotationTransport]; ok {
		transportType = strings.TrimSpace(t)
		if transportType == "" {
			transportType = transport.GRPCTransportType
		}
	}
	if !transport.IsRegistered(transportType) {
		return nil, fmt.Errorf("unsupported transport %q from annotation %s", transportType, transport.AnnotationTransport)
	}
	transportImpl, err := transport.Get(transportType, r.Client)
	if err != nil {
		return nil, fmt.Errorf("build transport %q from annotation %s: %w", transportType, transport.AnnotationTransport, err)
	}
	return transportImpl, nil
}

// reconcileNormal adds the transport finalizer, marks the StoryRun Reconciling,
// delegates to the selected transport implementation, handles
// transport.ErrBindingPending with a short metric-backed requeue, and patches
// ConditionTransportReady when the transport reports success
// (internal/controller/main.go:187-243).
func (r *TransportReconciler) reconcileNormal(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, transportImpl transport.Transport, transportType string, cm *bobrapetconditions.ConditionManager) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("transport-reconciler").WithValues("storyRun", fmt.Sprintf("%s/%s", storyRun.Namespace, storyRun.Name))

	// Add finalizer
	if controllerutil.AddFinalizer(storyRun, transportFinalizer) {
		if err := r.Update(ctx, storyRun); err != nil {
			return ctrl.Result{}, err
		}
	}

	patch := client.MergeFrom(storyRun.DeepCopy())
	cm.SetCondition(&storyRun.Status.Conditions, bobrapetconditions.ConditionTransportReady, "False", bobrapetconditions.ReasonReconciling, "Reconciling transport")
	if err := r.Status().Patch(ctx, storyRun, patch); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling transport for streaming StoryRun", "transport", transportType)
	if err := transportImpl.Reconcile(ctx, storyRun, story); err != nil {
		if errors.Is(err, transport.ErrBindingPending) {
			waitDuration := time.Duration(0)
			for _, cond := range storyRun.Status.Conditions {
				if cond.Type == string(bobrapetconditions.ConditionTransportReady) && !cond.LastTransitionTime.IsZero() {
					waitDuration = time.Since(cond.LastTransitionTime.Time)
					break
				}
			}
			// Calculate exponential backoff with cap to avoid tight polling loops.
			// Start at 2s, double each 10s of waiting, cap at 30s.
			requeue := calculateBindingPendingRequeue(waitDuration)
			logger.Info("Waiting for operator to create TransportBindings", "transport", transportType, "bindingPendingFor", waitDuration, "requeueAfter", requeue)
			grpcmetrics.RecordTransportBindingPending(transportType)
			grpcmetrics.ObserveTransportBindingWait(transportType, "pending", waitDuration)
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
		logger.Error(err, "Failed to reconcile transport")
		if r.Recorder != nil {
			r.Recorder.Eventf(
				storyRun,
				corev1.EventTypeWarning,
				eventReasonTransportReconcileFail,
				"Transport %s reconciliation failed for %s/%s: %v",
				transportType,
				storyRun.Namespace,
				storyRun.Name,
				err,
			)
		}
		patch := client.MergeFrom(storyRun.DeepCopy())
		cm.SetCondition(&storyRun.Status.Conditions, bobrapetconditions.ConditionTransportReady, "False", bobrapetconditions.ReasonTransportFailed, err.Error())
		if err := r.Status().Patch(ctx, storyRun, patch); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, err
	}

	patch = client.MergeFrom(storyRun.DeepCopy())
	cm.SetCondition(&storyRun.Status.Conditions, bobrapetconditions.ConditionTransportReady, "True", bobrapetconditions.ReasonTransportReady, "Transport is ready")
	if err := r.Status().Patch(ctx, storyRun, patch); err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		bindingPendingFor := time.Duration(0)
		for _, cond := range storyRun.Status.Conditions {
			if cond.Type == string(bobrapetconditions.ConditionTransportReady) && !cond.LastTransitionTime.IsZero() {
				bindingPendingFor = time.Since(cond.LastTransitionTime.Time)
				break
			}
		}
		message := fmt.Sprintf(
			"Transport %s ready for StoryRun %s/%s",
			transportType,
			storyRun.Namespace,
			storyRun.Name,
		)
		if bindingPendingFor > 0 {
			grpcmetrics.ObserveTransportBindingWait(transportType, "ready", bindingPendingFor)
			message = fmt.Sprintf("%s after waiting %s for bindings", message, bindingPendingFor.Round(time.Millisecond))
		}
		r.Recorder.Eventf(
			storyRun,
			corev1.EventTypeNormal,
			eventReasonTransportReady,
			"%s",
			message,
		)
	}

	return ctrl.Result{}, nil
}

// reconcileDelete marks the transport condition as CleaningUp, calls the
// transport implementation's EnsureCleanUp hook, and removes the
// transportFinalizer once cleanup succeeds (internal/controller/main.go:245-279).
func (r *TransportReconciler) reconcileDelete(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, transportImpl transport.Transport, transportType string, cm *bobrapetconditions.ConditionManager) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("transport-reconciler")
	logger.Info("Cleaning up transport for deleted StoryRun", "storyRun", storyRun.Name, "transport", transportType)

	patch := client.MergeFrom(storyRun.DeepCopy())
	cm.SetCondition(&storyRun.Status.Conditions, bobrapetconditions.ConditionTransportReady, "False", bobrapetconditions.ReasonCleaningUp, fmt.Sprintf("Cleaning up %s transport", transportType))
	if err := r.Status().Patch(ctx, storyRun, patch); err != nil {
		return ctrl.Result{}, err
	}

	cleanedCount, err := transportImpl.EnsureCleanUp(ctx, storyRun, story)
	if err != nil {
		logger.Error(err, "Failed to cleanup transport", "transport", transportType)
		if r.Recorder != nil {
			r.Recorder.Eventf(
				storyRun,
				corev1.EventTypeWarning,
				eventReasonTransportCleanupFailed,
				"Transport %s cleanup failed for StoryRun %s/%s: %v",
				transportType,
				storyRun.Namespace,
				storyRun.Name,
				err,
			)
		}
		return ctrl.Result{}, err
	}
	logger.Info("Transport cleanup completed", "storyRun", storyRun.Name, "transport", transportType, "annotationsReset", cleanedCount)

	patch = client.MergeFrom(storyRun.DeepCopy())
	cm.SetCondition(&storyRun.Status.Conditions, bobrapetconditions.ConditionTransportReady, "False", transportCleanedReason, fmt.Sprintf("%s transport cleanup completed", transportType))
	if err := r.Status().Patch(ctx, storyRun, patch); err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		message := fmt.Sprintf(
			"Transport %s cleanup completed for StoryRun %s/%s (annotationsReset=%d)",
			transportType,
			storyRun.Namespace,
			storyRun.Name,
			cleanedCount,
		)
		r.Recorder.Eventf(
			storyRun,
			corev1.EventTypeNormal,
			eventReasonTransportCleanupSucceeded,
			"%s",
			message,
		)
	}

	// Remove finalizer
	if controllerutil.RemoveFinalizer(storyRun, transportFinalizer) {
		if err := r.Update(ctx, storyRun); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager registers TransportReconciler with controller-runtime so it
// watches StoryRun resources via the manager's shared client/cache.
func (r *TransportReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("transport-reconciler")
	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StoryRun{}).
		WithEventFilter(storyRunTransportRelevantPredicate()).
		Complete(r)
}

// storyRunTransportRelevantPredicate returns a predicate that filters StoryRun events
// to only those relevant for transport reconciliation:
//   - Create events (new streaming StoryRuns need transport setup)
//   - Delete events (transport cleanup via finalizer)
//   - Phase transitions to/from terminal states (cleanup triggers)
//   - Spec changes (generation changes)
//
// This prevents reconcile storms from status-only updates like Duration, StepsComplete,
// or Message changes that don't affect transport wiring.
func storyRunTransportRelevantPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			_, ok := e.Object.(*runsv1alpha1.StoryRun)
			return ok
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldRun, ok1 := e.ObjectOld.(*runsv1alpha1.StoryRun)
			newRun, ok2 := e.ObjectNew.(*runsv1alpha1.StoryRun)
			if !ok1 || !ok2 {
				return false
			}

			// Spec change (generation change) is always relevant.
			if oldRun.GetGeneration() != newRun.GetGeneration() {
				return true
			}

			// Phase transitions are relevant for transport lifecycle.
			if oldRun.Status.Phase != newRun.Status.Phase {
				return true
			}

			// Deletion timestamp being set triggers cleanup.
			if (oldRun.DeletionTimestamp == nil) != (newRun.DeletionTimestamp == nil) {
				return true
			}

			// Ignore Duration, StepsComplete, Message, and other frequently-updated
			// status fields that don't affect transport wiring.
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// Deletions are handled via finalizer, but allow the event in case
			// the object was deleted without the controller seeing the update.
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

// calculateBindingPendingRequeue computes an exponential backoff requeue duration
// based on how long we've been waiting for bindings. This prevents tight polling
// loops when TransportBinding creation is slow (webhooks, quotas, etc.).
//
// Algorithm:
//   - Start at bindingPendingMinRequeue (2s)
//   - Double every 10 seconds of waiting
//   - Cap at bindingPendingMaxRequeue (30s)
func calculateBindingPendingRequeue(waitDuration time.Duration) time.Duration {
	if waitDuration <= 0 {
		return bindingPendingMinRequeue
	}

	// Calculate how many 10-second intervals have passed
	intervals := int(waitDuration.Seconds() / 10)
	if intervals > 4 {
		intervals = 4 // Cap the exponent to avoid overflow
	}

	// Exponential backoff: 2s * 2^intervals
	requeue := bindingPendingMinRequeue
	for i := 0; i < intervals; i++ {
		requeue *= 2
	}

	if requeue > bindingPendingMaxRequeue {
		requeue = bindingPendingMaxRequeue
	}

	return requeue
}
