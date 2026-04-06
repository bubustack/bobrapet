package status

import (
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
)

func resolvePhaseReason(phase enums.Phase, reason string) string {
	if reason != "" {
		return reason
	}
	switch phase {
	case enums.PhasePending:
		return conditions.ReasonPending
	case enums.PhaseRunning:
		return conditions.ReasonRunning
	case enums.PhasePaused:
		return conditions.ReasonPaused
	case enums.PhaseBlocked:
		return conditions.ReasonBlocked
	case enums.PhaseScheduling:
		return conditions.ReasonScheduled
	case enums.PhaseTimeout:
		return conditions.ReasonTimedOut
	case enums.PhaseSkipped:
		return conditions.ReasonSkipped
	case enums.PhaseCompensated:
		return conditions.ReasonCompensated
	case enums.PhaseCanceled, enums.PhaseFinished, enums.PhaseAborted:
		return conditions.ReasonCanceled
	case enums.PhaseSucceeded:
		return conditions.ReasonCompleted
	case enums.PhaseFailed:
		return conditions.ReasonExecutionFailed
	default:
		return conditions.ReasonReconciling
	}
}
