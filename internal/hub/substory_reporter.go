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
	"fmt"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// substoryWatchTimeout is the maximum time to watch a fire-and-forget child StoryRun.
	// After this duration, the watcher stops regardless of child state.
	substoryWatchTimeout = 30 * time.Minute

	// substoryPollInterval is how often the watcher polls the child StoryRun status.
	substoryPollInterval = 5 * time.Second

	// substoryPatchTimeout is the timeout for individual status patch operations.
	substoryPatchTimeout = 5 * time.Second
)

// substoryReporter watches fire-and-forget child StoryRuns and patches the
// parent StoryRun's Degraded condition when a child fails. It also provides
// reportStepFailure for recording batch step failures on the parent.
type substoryReporter struct {
	client client.Client
	log    logr.Logger
}

func newSubstoryReporter(c client.Client, log logr.Logger) *substoryReporter {
	return &substoryReporter{client: c, log: log}
}

// watchFireAndForget starts a background goroutine that polls the child StoryRun
// until it reaches a terminal phase. If the child fails, the parent StoryRun's
// Degraded condition is set.
func (r *substoryReporter) watchFireAndForget(parentCtx context.Context, parentName, parentNamespace, childName, stepID string) {
	if r == nil || parentName == "" || childName == "" {
		return
	}
	go r.watchChild(parentCtx, parentName, parentNamespace, childName, stepID)
}

func (r *substoryReporter) watchChild(parentCtx context.Context, parentName, parentNamespace, childName, stepID string) {
	ctx, cancel := context.WithTimeout(parentCtx, substoryWatchTimeout)
	defer cancel()

	childKey := types.NamespacedName{Name: childName, Namespace: parentNamespace}
	ticker := time.NewTicker(substoryPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.V(1).Info("Substory watcher timed out",
				"parent", parentName, "child", childName, "step", stepID)
			return
		case <-ticker.C:
		}

		var child runsv1alpha1.StoryRun
		if err := r.client.Get(ctx, childKey, &child); err != nil {
			if apierrors.IsNotFound(err) {
				// Child deleted before reaching terminal — nothing to report.
				r.log.V(1).Info("Fire-and-forget child StoryRun deleted",
					"parent", parentName, "child", childName, "step", stepID)
				return
			}
			r.log.Error(err, "Substory watcher: failed to get child",
				"parent", parentName, "child", childName)
			continue
		}

		if !child.Status.Phase.IsTerminal() || child.Status.Phase == "" {
			continue
		}

		// Child reached terminal phase.
		if child.Status.Phase == "Succeeded" || child.Status.Phase == "Skipped" {
			r.log.V(1).Info("Fire-and-forget child succeeded",
				"parent", parentName, "child", childName, "step", stepID,
				"phase", child.Status.Phase)
			return
		}

		// Child failed — report on parent.
		message := child.Status.Message
		if message == "" {
			message = fmt.Sprintf("phase %s", child.Status.Phase)
		}
		reason := fmt.Sprintf("fire-and-forget sub-story %q (step %q) failed: %s", childName, stepID, message)
		r.log.Info("Fire-and-forget child failed; reporting on parent",
			"parent", parentName, "child", childName, "step", stepID,
			"phase", child.Status.Phase, "message", message)

		r.patchParentDegraded(parentName, parentNamespace, conditions.ReasonDependencyFailed, reason)
		return
	}
}

// reportStepFailure records a batch step or primitive failure on the parent
// StoryRun as a Degraded condition. Called asynchronously (fire-and-forget).
func (r *substoryReporter) reportStepFailure(parentName, parentNamespace, stepID, failureMsg string) {
	if r == nil || parentName == "" || stepID == "" {
		return
	}
	go func() {
		reason := fmt.Sprintf("batch step %q failed: %s", stepID, failureMsg)
		r.patchParentDegraded(parentName, parentNamespace, conditions.ReasonExecutionFailed, reason)
	}()
}

// patchParentDegraded sets the Degraded condition on the parent StoryRun.
func (r *substoryReporter) patchParentDegraded(parentName, parentNamespace, condReason, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), substoryPatchTimeout)
	defer cancel()

	key := types.NamespacedName{Name: parentName, Namespace: parentNamespace}
	var parent runsv1alpha1.StoryRun
	if err := r.client.Get(ctx, key, &parent); err != nil {
		r.log.Error(err, "Substory reporter: parent StoryRun not found",
			"storyRun", key)
		return
	}

	// Don't patch if parent is already in a terminal phase.
	if parent.Status.Phase.IsTerminal() && parent.Status.Phase != "" {
		r.log.V(1).Info("Substory reporter: parent already terminal, skipping patch",
			"storyRun", key, "phase", parent.Status.Phase)
		return
	}

	original := parent.DeepCopy()
	cm := conditions.NewConditionManager(parent.Generation)
	cm.SetDegradedCondition(&parent.Status.Conditions, true, condReason, message)

	if err := r.client.Status().Patch(ctx, &parent, client.MergeFrom(original)); err != nil {
		r.log.Error(err, "Substory reporter: failed to patch parent StoryRun",
			"storyRun", key)
		return
	}
	r.log.V(1).Info("Substory reporter: patched parent StoryRun Degraded condition",
		"storyRun", key, "reason", condReason)
}
