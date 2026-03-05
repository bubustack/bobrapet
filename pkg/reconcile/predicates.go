package reconcile

import (
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
)

type runLifecycleSnapshot struct {
	generation int64
	phase      enums.Phase
	startedAt  *metav1.Time
	finishedAt *metav1.Time
}

func buildMeaningfulRunPredicate(
	extract func(event.UpdateEvent) (runLifecycleSnapshot, runLifecycleSnapshot, bool),
) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return true },
		DeleteFunc: func(e event.DeleteEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldSnap, newSnap, ok := extract(e)
			if !ok {
				return false
			}
			return hasMeaningfulRunUpdate(
				oldSnap.generation,
				newSnap.generation,
				oldSnap.phase,
				newSnap.phase,
				oldSnap.startedAt,
				newSnap.startedAt,
				oldSnap.finishedAt,
				newSnap.finishedAt,
			)
		},
	}
}

// StepRunMeaningfulUpdates returns a predicate that:
//   - allows Create events (new runs matter)
//   - ignores annotation-only churn (e.g., token marking)
//   - allows updates when generation changes (spec change) OR when status changes in meaningful ways
//   - ignores Delete events (optional; change if you want deletions to trigger recounts)
func StepRunMeaningfulUpdates() predicate.Predicate {
	return buildMeaningfulRunPredicate(func(e event.UpdateEvent) (runLifecycleSnapshot, runLifecycleSnapshot, bool) {
		oldRun, ok1 := e.ObjectOld.(*runsv1alpha1.StepRun)
		newRun, ok2 := e.ObjectNew.(*runsv1alpha1.StepRun)
		if !ok1 || !ok2 {
			return runLifecycleSnapshot{}, runLifecycleSnapshot{}, false
		}
		return runLifecycleSnapshot{
				generation: oldRun.GetGeneration(),
				phase:      oldRun.Status.Phase,
				startedAt:  oldRun.Status.StartedAt,
				finishedAt: oldRun.Status.FinishedAt,
			}, runLifecycleSnapshot{
				generation: newRun.GetGeneration(),
				phase:      newRun.Status.Phase,
				startedAt:  newRun.Status.StartedAt,
				finishedAt: newRun.Status.FinishedAt,
			}, true
	})
}

// StoryRunMeaningfulUpdates returns a predicate that:
//   - allows Create events
//   - allows updates on meaningful lifecycle transitions (phase, started/finished timestamps)
//   - ignores annotation-only churn (token marking)
//   - ignores Delete events by default
func StoryRunMeaningfulUpdates() predicate.Predicate {
	return buildMeaningfulRunPredicate(func(e event.UpdateEvent) (runLifecycleSnapshot, runLifecycleSnapshot, bool) {
		oldRun, ok1 := e.ObjectOld.(*runsv1alpha1.StoryRun)
		newRun, ok2 := e.ObjectNew.(*runsv1alpha1.StoryRun)
		if !ok1 || !ok2 {
			return runLifecycleSnapshot{}, runLifecycleSnapshot{}, false
		}
		return runLifecycleSnapshot{
				generation: oldRun.GetGeneration(),
				phase:      oldRun.Status.Phase,
				startedAt:  oldRun.Status.StartedAt,
				finishedAt: oldRun.Status.FinishedAt,
			}, runLifecycleSnapshot{
				generation: newRun.GetGeneration(),
				phase:      newRun.Status.Phase,
				startedAt:  newRun.Status.StartedAt,
				finishedAt: newRun.Status.FinishedAt,
			}, true
	})
}

// StoryRunTriggerRelevantPredicate enqueues only on transitions that should affect Story triggers.
// This must ignore annotation-only writes (e.g., token marking) and status fields that change
// frequently (Duration, StepsComplete, Message) to prevent reconcile storms.
//
// Only trigger on:
//   - Phase transitions (the primary signal for trigger counting)
//   - StartedAt/FinishedAt being set (lifecycle milestones)
//
// This prevents O(n²) reconcile loops where running StoryRuns update Duration every reconcile,
// which would otherwise trigger the Story controller repeatedly.
func StoryRunTriggerRelevantPredicate() predicate.Predicate {
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

			// Phase change is the primary trigger-relevant signal.
			if oldRun.Status.Phase != newRun.Status.Phase {
				return true
			}

			// StartedAt being set indicates run has started (trigger milestone).
			if (oldRun.Status.StartedAt == nil) != (newRun.Status.StartedAt == nil) {
				return true
			}

			// FinishedAt being set indicates run has completed (trigger milestone).
			if (oldRun.Status.FinishedAt == nil) != (newRun.Status.FinishedAt == nil) {
				return true
			}

			// Ignore Duration, StepsComplete, Message, Conditions, and other
			// frequently-updated fields that don't affect trigger counting.
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

// ImpulseUsageRelevantPredicate triggers Story usage recount when an Impulse is created/deleted,
// or when the Impulse's StoryRef changes (moves between stories/namespaces).
func ImpulseUsageRelevantPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			_, ok := e.Object.(*v1alpha1.Impulse)
			return ok
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			_, ok := e.Object.(*v1alpha1.Impulse)
			return ok
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObj, ok1 := e.ObjectOld.(*v1alpha1.Impulse)
			newObj, ok2 := e.ObjectNew.(*v1alpha1.Impulse)
			if !ok1 || !ok2 {
				return false
			}

			// Recount if storyRef changed.
			if !apiequality.Semantic.DeepEqual(oldObj.Spec.StoryRef, newObj.Spec.StoryRef) {
				return true
			}

			// Deletion transition can matter for usage counts (finalizer paths, GC).
			if (oldObj.DeletionTimestamp == nil) != (newObj.DeletionTimestamp == nil) {
				return true
			}

			return false
		},
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

// StepRunEngramTriggerRelevantUpdates enqueues only when a StepRun is created
// or its spec changes (generation changes). It ignores all status transitions.
func StepRunEngramTriggerRelevantUpdates() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return true },
		DeleteFunc: func(e event.DeleteEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldRun, ok1 := e.ObjectOld.(*runsv1alpha1.StepRun)
			newRun, ok2 := e.ObjectNew.(*runsv1alpha1.StepRun)
			if !ok1 || !ok2 {
				return false
			}
			return oldRun.Generation != newRun.Generation
		},
	}
}

func hasMeaningfulRunUpdate(
	oldGeneration, newGeneration int64,
	oldPhase, newPhase enums.Phase,
	oldStarted, newStarted *metav1.Time,
	oldFinished, newFinished *metav1.Time,
) bool {
	if oldGeneration != newGeneration {
		return true
	}

	if oldPhase != newPhase {
		return true
	}

	if (oldStarted == nil) != (newStarted == nil) {
		return true
	}

	if (oldFinished == nil) != (newFinished == nil) {
		return true
	}

	// Ignore annotation-only updates (token marking ends up here).
	return false
}
