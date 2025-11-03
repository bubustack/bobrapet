package status

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	v1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
)

// ImpulseTriggerStats captures aggregated trigger counters + timestamps for an
// Impulse based on the StoryRuns that reference it.
type ImpulseTriggerStats struct {
	TriggersReceived int64
	StoriesLaunched  int64
	FailedTriggers   int64
	LastTrigger      *metav1.Time
	LastSuccess      *metav1.Time
}

// AggregateImpulseTriggerStats scans StoryRuns and returns the aggregated
// counters/timestamps that should be recorded on the Impulse status.
func AggregateImpulseTriggerStats(impulse *v1alpha1.Impulse, runs []runsv1alpha1.StoryRun) ImpulseTriggerStats {
	stats := ImpulseTriggerStats{}
	if impulse == nil {
		return stats
	}

	for i := range runs {
		run := &runs[i]
		if !targetsImpulse(run, impulse) {
			continue
		}
		stats.recordTriggerTimestamp(run)
		stats.recordTriggerTokens(run)
		stats.recordSuccessTimestamp(run)
	}
	return stats
}

// ApplyImpulseTriggerStats copies the aggregated stats onto the Impulse status
// while keeping timestamps monotonic.
func ApplyImpulseTriggerStats(impulse *v1alpha1.Impulse, observedGeneration int64, stats ImpulseTriggerStats) {
	if impulse == nil {
		return
	}
	impulse.Status.ObservedGeneration = observedGeneration
	impulse.Status.TriggersReceived = stats.TriggersReceived
	impulse.Status.StoriesLaunched = stats.StoriesLaunched
	impulse.Status.FailedTriggers = stats.FailedTriggers

	if stats.LastTrigger != nil &&
		(impulse.Status.LastTrigger == nil || stats.LastTrigger.After(impulse.Status.LastTrigger.Time)) {
		impulse.Status.LastTrigger = stats.LastTrigger.DeepCopy()
	}
	if stats.LastSuccess != nil &&
		(impulse.Status.LastSuccess == nil || stats.LastSuccess.After(impulse.Status.LastSuccess.Time)) {
		impulse.Status.LastSuccess = stats.LastSuccess.DeepCopy()
	}
}

func (s *ImpulseTriggerStats) recordTriggerTimestamp(run *runsv1alpha1.StoryRun) {
	candidate := run.CreationTimestamp
	if run.Status.StartedAt != nil {
		candidate = *run.Status.StartedAt
	}
	if s.LastTrigger == nil || candidate.After(s.LastTrigger.Time) {
		s.LastTrigger = candidate.DeepCopy()
	}
}

func (s *ImpulseTriggerStats) recordSuccessTimestamp(run *runsv1alpha1.StoryRun) {
	if !run.Status.HasTriggerToken(contracts.StoryRunTriggerTokenImpulseSuccess) {
		return
	}
	if run.Status.FinishedAt == nil {
		return
	}
	if s.LastSuccess == nil || run.Status.FinishedAt.After(s.LastSuccess.Time) {
		s.LastSuccess = run.Status.FinishedAt.DeepCopy()
	}
}

func (s *ImpulseTriggerStats) recordTriggerTokens(run *runsv1alpha1.StoryRun) {
	if run.Status.HasTriggerToken(contracts.StoryRunTriggerTokenImpulse) {
		s.TriggersReceived++
	}
	if run.Status.HasTriggerToken(contracts.StoryRunTriggerTokenImpulseSuccess) {
		s.StoriesLaunched++
	}
	if run.Status.HasTriggerToken(contracts.StoryRunTriggerTokenImpulseFailed) {
		s.FailedTriggers++
	}
}

func targetsImpulse(run *runsv1alpha1.StoryRun, impulse *v1alpha1.Impulse) bool {
	if run.Spec.ImpulseRef == nil || run.Spec.ImpulseRef.Name != impulse.Name {
		return false
	}
	ns := refs.ResolveNamespace(run, &run.Spec.ImpulseRef.ObjectReference)
	return ns == impulse.Namespace
}
