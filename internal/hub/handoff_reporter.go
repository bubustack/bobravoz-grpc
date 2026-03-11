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
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// handoffReporter patches StepRun.Status.Handoff when the hub's stream
// handoff phase changes. It runs each patch with a short timeout so the
// hot data-plane path is never blocked.
type handoffReporter struct {
	client client.Client
	log    logr.Logger
}

// newHandoffReporter creates a reporter that patches StepRun status.
func newHandoffReporter(c client.Client, log logr.Logger) *handoffReporter {
	return &handoffReporter{client: c, log: log}
}

// report is intended to be called as a HandoffCallback (fire-and-forget in a goroutine).
func (r *handoffReporter) report(storyRunName, storyRunNamespace, stepID, phase, reason string) {
	if storyRunName == "" || storyRunNamespace == "" || stepID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stepRunName := kubeutil.ComposeName(storyRunName, stepID)
	key := types.NamespacedName{Name: stepRunName, Namespace: storyRunNamespace}

	var stepRun runsv1alpha1.StepRun
	if err := r.client.Get(ctx, key, &stepRun); err != nil {
		r.log.V(1).Info("Handoff report: StepRun not found; skipping",
			"stepRun", key, "phase", phase, "error", err)
		return
	}

	original := stepRun.DeepCopy()

	if stepRun.Status.Handoff == nil {
		stepRun.Status.Handoff = &runsv1alpha1.HandoffStatus{}
	}
	stepRun.Status.Handoff.Phase = runsv1alpha1.HandoffPhase(phase)
	stepRun.Status.Handoff.Message = reason
	now := metav1.Now()
	stepRun.Status.Handoff.UpdatedAt = &now

	if err := r.client.Status().Patch(ctx, &stepRun, client.MergeFrom(original)); err != nil {
		r.log.V(1).Info("Handoff report: failed to patch StepRun status",
			"stepRun", key, "phase", phase, "error", err)
		return
	}
	r.log.V(1).Info("Handoff status reported",
		"stepRun", key, "phase", phase, "reason", reason)
}
