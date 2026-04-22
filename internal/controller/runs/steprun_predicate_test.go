package runs

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
)

func TestStepRunReconcilePredicateIncludesDeletionStart(t *testing.T) {
	t.Parallel()

	predicate := stepRunReconcilePredicate()
	oldStep := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "step",
			Namespace:  "default",
			Generation: 1,
		},
	}
	newStep := oldStep.DeepCopy()
	deletingAt := metav1.NewTime(time.Now())
	newStep.DeletionTimestamp = &deletingAt

	if !predicate.Update(event.UpdateEvent{ObjectOld: oldStep, ObjectNew: newStep}) {
		t.Fatal("expected deletion-start update to trigger reconcile")
	}
}

func TestStepRunReconcilePredicateIgnoresStatusOnlyUpdates(t *testing.T) {
	t.Parallel()

	predicate := stepRunReconcilePredicate()
	oldStep := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "step",
			Namespace:  "default",
			Generation: 1,
		},
	}
	newStep := oldStep.DeepCopy()
	newStep.Status.Phase = enums.PhaseRunning

	if predicate.Update(event.UpdateEvent{ObjectOld: oldStep, ObjectNew: newStep}) {
		t.Fatal("expected status-only update to be ignored")
	}
}

func TestStepRunReconcilePredicateIncludesGenerationChanges(t *testing.T) {
	t.Parallel()

	predicate := stepRunReconcilePredicate()
	oldStep := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "step",
			Namespace:  "default",
			Generation: 1,
		},
	}
	newStep := oldStep.DeepCopy()
	newStep.Generation = 2

	if !predicate.Update(event.UpdateEvent{ObjectOld: oldStep, ObjectNew: newStep}) {
		t.Fatal("expected generation update to trigger reconcile")
	}
}
