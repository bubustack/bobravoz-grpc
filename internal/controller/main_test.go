package controller

import (
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bobrapetconditions "github.com/bubustack/bobrapet/pkg/conditions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestStoryRunTransportRelevantPredicateRequeuesDeletingRunWithFinalizer(t *testing.T) {
	now := metav1.NewTime(time.Unix(1, 0))
	oldRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run",
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{transportFinalizer},
		},
	}
	newRun := oldRun.DeepCopy()
	newRun.ResourceVersion = "2"
	newRun.Status.Message = "metadata or status changed while deleting"

	if got := storyRunTransportRelevantPredicate().Update(event.UpdateEvent{
		ObjectOld: oldRun,
		ObjectNew: newRun,
	}); !got {
		t.Fatal("expected deleting StoryRun with transport finalizer to keep reconciling")
	}
}

func TestStoryRunTransportRelevantPredicateIgnoresStatusOnlyUpdateWhenNotDeleting(t *testing.T) {
	oldRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run",
			Namespace: "default",
		},
	}
	newRun := oldRun.DeepCopy()
	newRun.Status.Message = "status changed"

	if got := storyRunTransportRelevantPredicate().Update(event.UpdateEvent{
		ObjectOld: oldRun,
		ObjectNew: newRun,
	}); got {
		t.Fatal("expected non-deleting status-only update to be ignored")
	}
}

func TestSetTransportConditionsIncludesReady(t *testing.T) {
	cm := bobrapetconditions.NewConditionManager(7)
	var conditions []metav1.Condition

	setTransportConditions(cm, &conditions, true, bobrapetconditions.ReasonTransportReady, "Transport is ready")

	ready := bobrapetconditions.GetCondition(conditions, bobrapetconditions.ConditionReady)
	if ready == nil {
		t.Fatal("expected Ready condition to be present")
	}
	if ready.Status != metav1.ConditionTrue {
		t.Fatalf("expected Ready=True, got %s", ready.Status)
	}

	transportReady := bobrapetconditions.GetCondition(conditions, bobrapetconditions.ConditionTransportReady)
	if transportReady == nil {
		t.Fatal("expected TransportReady condition to be present")
	}
	if transportReady.Status != metav1.ConditionTrue {
		t.Fatalf("expected TransportReady=True, got %s", transportReady.Status)
	}
}

func TestSetTransportConditionsMarksReadyFalseWhenTransportNotReady(t *testing.T) {
	cm := bobrapetconditions.NewConditionManager(7)
	var conditions []metav1.Condition

	setTransportConditions(cm, &conditions, false, bobrapetconditions.ReasonReconciling, "Reconciling transport")

	ready := bobrapetconditions.GetCondition(conditions, bobrapetconditions.ConditionReady)
	if ready == nil {
		t.Fatal("expected Ready condition to be present")
	}
	if ready.Status != metav1.ConditionFalse {
		t.Fatalf("expected Ready=False, got %s", ready.Status)
	}

	transportReady := bobrapetconditions.GetCondition(conditions, bobrapetconditions.ConditionTransportReady)
	if transportReady == nil {
		t.Fatal("expected TransportReady condition to be present")
	}
	if transportReady.Status != metav1.ConditionFalse {
		t.Fatalf("expected TransportReady=False, got %s", transportReady.Status)
	}
}
