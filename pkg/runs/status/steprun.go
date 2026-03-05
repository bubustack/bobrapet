package status

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/metrics"
)

// PatchStepRunPhase centralizes StepRun phase transitions by mirroring the controller
// logic that stamps timestamps, duration metrics, and Ready-condition updates.
// Uses RetryableStatusPatch (merge patch) so only Phase, Conditions, and related
// fields are sent; Status.Output from the SDK is preserved and not overwritten.
func PatchStepRunPhase(
	ctx context.Context,
	k8sClient client.Client,
	step *runsv1alpha1.StepRun,
	phase enums.Phase,
	reason string,
	message string,
) error {
	return kubeutil.RetryableStatusPatch(ctx, k8sClient, step, func(obj client.Object) {
		s := obj.(*runsv1alpha1.StepRun)
		if s.Status.Retries < 0 {
			s.Status.Retries = 0
		}
		s.Status.Phase = phase
		s.Status.ObservedGeneration = s.Generation
		ensureTraceInfo(ctx, &s.Status.Trace)

		cm := conditions.NewConditionManager(s.Generation)
		condReason := resolvePhaseReason(phase, reason)
		progressing := !phase.IsTerminal()
		degraded := phase == enums.PhaseFailed || phase == enums.PhaseTimeout || phase == enums.PhaseAborted

		switch phase {
		case enums.PhasePending:
			if s.Status.StartedAt == nil {
				s.Status.StartedAt = &metav1.Time{Time: time.Now()}
			}
			cm.SetCondition(
				&s.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				condReason,
				message,
			)
		case enums.PhaseBlocked:
			cm.SetCondition(
				&s.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				condReason,
				message,
			)
			s.Status.LastFailureMsg = message
		case enums.PhaseRunning:
			cm.SetCondition(
				&s.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				condReason,
				message,
			)
		case enums.PhasePaused:
			cm.SetCondition(
				&s.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				condReason,
				message,
			)
		case enums.PhaseSucceeded:
			if s.Status.FinishedAt == nil {
				now := metav1.Now()
				s.Status.FinishedAt = &now
				if s.Status.StartedAt != nil {
					duration := now.Sub(s.Status.StartedAt.Time)
					s.Status.Duration = duration.Round(time.Second).String()
					metrics.RecordStepRunMetrics(s.Namespace, s.Spec.EngramRef.Name, string(phase), duration)
				}
			}
			cm.SetCondition(
				&s.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionTrue,
				condReason,
				message,
			)
		case enums.PhaseFailed:
			if s.Status.FinishedAt == nil {
				now := metav1.Now()
				s.Status.FinishedAt = &now
				if s.Status.StartedAt != nil {
					duration := now.Sub(s.Status.StartedAt.Time)
					s.Status.Duration = duration.Round(time.Second).String()
					metrics.RecordStepRunMetrics(s.Namespace, s.Spec.EngramRef.Name, string(phase), duration)
				}
			}
			cm.SetCondition(
				&s.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				condReason,
				message,
			)
		default:
			if phase.IsTerminal() {
				if s.Status.FinishedAt == nil {
					now := metav1.Now()
					s.Status.FinishedAt = &now
					if s.Status.StartedAt != nil {
						duration := now.Sub(s.Status.StartedAt.Time)
						s.Status.Duration = duration.Round(time.Second).String()
						metrics.RecordStepRunMetrics(s.Namespace, s.Spec.EngramRef.Name, string(phase), duration)
					}
				}
				cm.SetCondition(
					&s.Status.Conditions,
					conditions.ConditionReady,
					metav1.ConditionFalse,
					condReason,
					message,
				)
			} else {
				cm.SetCondition(
					&s.Status.Conditions,
					conditions.ConditionReady,
					metav1.ConditionFalse,
					condReason,
					message,
				)
			}
		}

		cm.SetProgressingCondition(&s.Status.Conditions, progressing, condReason, message)
		cm.SetDegradedCondition(&s.Status.Conditions, degraded, condReason, message)
	})
}

// PatchStepRunHandoff updates the StepRun handoff status in a retryable status patch.
func PatchStepRunHandoff(
	ctx context.Context,
	k8sClient client.Client,
	step *runsv1alpha1.StepRun,
	handoff *runsv1alpha1.HandoffStatus,
) error {
	return kubeutil.RetryableStatusPatch(ctx, k8sClient, step, func(obj client.Object) {
		s := obj.(*runsv1alpha1.StepRun)
		if handoff == nil {
			s.Status.Handoff = nil
			return
		}
		copyStatus := *handoff
		if copyStatus.UpdatedAt == nil {
			now := metav1.Now()
			copyStatus.UpdatedAt = &now
		}
		s.Status.Handoff = &copyStatus
	})
}
