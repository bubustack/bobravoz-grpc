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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	bobrapetconditions "github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobravoz-grpc/internal/transport"
)

const transportFinalizer = "transport.bobravoz.bubustack.io/finalizer"

// TransportReconciler reconciles a StoryRun object for streaming transport
type TransportReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	ConditionManager *bobrapetconditions.ConditionManager
}

// +kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

func (r *TransportReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("transport-reconciler")

	var storyRun runsv1alpha1.StoryRun
	if err := r.Get(ctx, req.NamespacedName, &storyRun); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// Fetch the parent Story to check the pattern
	var story bubuv1alpha1.Story
	storyKey := client.ObjectKey{Namespace: storyRun.Namespace, Name: storyRun.Spec.StoryRef.Name}
	if err := r.Get(ctx, storyKey, &story); err != nil {
		logger.Error(err, "Failed to get parent Story for StoryRun", "storyRef", storyRun.Spec.StoryRef)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// This controller only cares about streaming patterns
	if story.Spec.Pattern != enums.StreamingPattern {
		return ctrl.Result{}, nil
	}

	transportImpl, err := r.getTransportForStory(&story)
	if err != nil {
		logger.Error(err, "Failed to get transport for story")
		// TODO: Update StoryRun status with an error condition
		return ctrl.Result{}, err
	}

	// Handle deletion
	if storyRun.GetDeletionTimestamp() != nil {
		return r.reconcileDelete(ctx, &storyRun, &story, transportImpl)
	}

	return r.reconcileNormal(ctx, &storyRun, &story, transportImpl)
}

func (r *TransportReconciler) getTransportForStory(story *bubuv1alpha1.Story) (transport.Transport, error) {
	transportType := transport.GRPCTransportType // Default transport
	if t, ok := story.Annotations[transport.AnnotationTransport]; ok {
		transportType = t
	}
	return transport.Get(transportType, r.Client)
}

func (r *TransportReconciler) reconcileNormal(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, transportImpl transport.Transport) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("transport-reconciler")

	if r.ConditionManager == nil {
		r.ConditionManager = bobrapetconditions.NewConditionManager(storyRun.Generation)
	} else {
		r.ConditionManager.UpdateGeneration(storyRun.Generation)
	}

	// Add finalizer
	if controllerutil.AddFinalizer(storyRun, transportFinalizer) {
		if err := r.Update(ctx, storyRun); err != nil {
			return ctrl.Result{}, err
		}
	}

	patch := client.MergeFrom(storyRun.DeepCopy())
	r.ConditionManager.SetCondition(&storyRun.Status.Conditions, bobrapetconditions.ConditionTransportReady, "False", bobrapetconditions.ReasonReconciling, "Reconciling transport")
	if err := r.Status().Patch(ctx, storyRun, patch); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling transport for streaming StoryRun", "storyRun", storyRun.Name, "transport", story.Annotations[transport.AnnotationTransport])
	if err := transportImpl.Reconcile(ctx, storyRun, story); err != nil {
		logger.Error(err, "Failed to reconcile transport")
		patch := client.MergeFrom(storyRun.DeepCopy())
		r.ConditionManager.SetCondition(&storyRun.Status.Conditions, bobrapetconditions.ConditionTransportReady, "False", bobrapetconditions.ReasonTransportFailed, err.Error())
		if err := r.Status().Patch(ctx, storyRun, patch); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, err
	}

	patch = client.MergeFrom(storyRun.DeepCopy())
	r.ConditionManager.SetCondition(&storyRun.Status.Conditions, bobrapetconditions.ConditionTransportReady, "True", bobrapetconditions.ReasonTransportReady, "Transport is ready")
	if err := r.Status().Patch(ctx, storyRun, patch); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *TransportReconciler) reconcileDelete(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, transportImpl transport.Transport) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("transport-reconciler")
	logger.Info("Cleaning up transport for deleted StoryRun", "storyRun", storyRun.Name)

	if r.ConditionManager == nil {
		r.ConditionManager = bobrapetconditions.NewConditionManager(storyRun.Generation)
	} else {
		r.ConditionManager.UpdateGeneration(storyRun.Generation)
	}

	patch := client.MergeFrom(storyRun.DeepCopy())
	r.ConditionManager.SetCondition(&storyRun.Status.Conditions, bobrapetconditions.ConditionTransportReady, "False", bobrapetconditions.ReasonCleaningUp, "Cleaning up transport")
	if err := r.Status().Patch(ctx, storyRun, patch); err != nil {
		return ctrl.Result{}, err
	}

	if err := transportImpl.EnsureCleanUp(ctx, storyRun, story); err != nil {
		logger.Error(err, "Failed to cleanup transport")
		return ctrl.Result{}, err
	}

	// Remove finalizer
	if controllerutil.RemoveFinalizer(storyRun, transportFinalizer) {
		if err := r.Update(ctx, storyRun); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TransportReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StoryRun{}).
		Complete(r)
}
