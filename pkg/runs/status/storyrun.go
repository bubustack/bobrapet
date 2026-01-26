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
	"github.com/bubustack/core/contracts"
)

// PatchStoryRunPhase centralizes StoryRun phase transitions by cloning the controller-runtime
// status patch logic used across reconcilers. It stamps phase/message/observedGeneration,
// updates StartedAt/FinishedAt timestamps, records duration metrics, keeps the Ready
// condition in sync with the new phase, and marks trigger tokens for idempotent counting
// by Story/Impulse controllers. All side effects are applied via RetryableStatusPatch
// so concurrent reconciles observe consistent status snapshots.
func PatchStoryRunPhase(
	ctx context.Context,
	k8sClient client.Client,
	srun *runsv1alpha1.StoryRun,
	phase enums.Phase,
	message string,
) error {
	return kubeutil.RetryableStatusPatch(ctx, k8sClient, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		sr.Status.Phase = phase
		sr.Status.Message = message
		sr.Status.ObservedGeneration = sr.Generation

		cm := conditions.NewConditionManager(sr.Generation)

		switch phase {
		case enums.PhaseRunning:
			if sr.Status.StartedAt == nil {
				now := metav1.Now()
				sr.Status.StartedAt = &now
			}
			if sr.Status.StartedAt != nil {
				sr.Status.Duration = time.Since(sr.Status.StartedAt.Time).Round(time.Second).String()
			}
			// Mark base trigger tokens when StoryRun first starts.
			// These are used by Story/Impulse controllers for idempotent trigger counting.
			sr.Status.AddTriggerToken(contracts.StoryRunTriggerTokenStory)
			sr.Status.AddTriggerToken(contracts.StoryRunTriggerTokenImpulse)
			cm.SetCondition(
				&sr.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				conditions.ReasonReconciling,
				message,
			)
		case enums.PhaseSucceeded:
			if sr.Status.FinishedAt == nil {
				now := metav1.Now()
				sr.Status.FinishedAt = &now
				if sr.Status.StartedAt != nil {
					duration := now.Sub(sr.Status.StartedAt.Time)
					sr.Status.Duration = duration.Round(time.Second).String()
					metrics.RecordStoryRunMetrics(sr.Namespace, sr.Spec.StoryRef.Name, string(phase), duration)
				}
			}
			// Mark success token for Impulse controller counting.
			sr.Status.AddTriggerToken(contracts.StoryRunTriggerTokenImpulseSuccess)
			cm.SetCondition(
				&sr.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionTrue,
				conditions.ReasonCompleted,
				message,
			)
		case enums.PhaseFailed:
			if sr.Status.FinishedAt == nil {
				now := metav1.Now()
				sr.Status.FinishedAt = &now
				if sr.Status.StartedAt != nil {
					duration := now.Sub(sr.Status.StartedAt.Time)
					sr.Status.Duration = duration.Round(time.Second).String()
					metrics.RecordStoryRunMetrics(sr.Namespace, sr.Spec.StoryRef.Name, string(phase), duration)
				}
			}
			// Mark failure token for Impulse controller counting.
			sr.Status.AddTriggerToken(contracts.StoryRunTriggerTokenImpulseFailed)
			cm.SetCondition(
				&sr.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				conditions.ReasonExecutionFailed,
				message,
			)
		default:
			if phase.IsTerminal() {
				if sr.Status.FinishedAt == nil {
					now := metav1.Now()
					sr.Status.FinishedAt = &now
					if sr.Status.StartedAt != nil {
						duration := now.Sub(sr.Status.StartedAt.Time)
						sr.Status.Duration = duration.Round(time.Second).String()
					}
				}
				reason := conditions.ReasonExecutionFailed
				if phase == enums.PhaseFinished {
					reason = conditions.ReasonCanceled
				}
				cm.SetCondition(
					&sr.Status.Conditions,
					conditions.ConditionReady,
					metav1.ConditionFalse,
					reason,
					message,
				)
			} else {
				cm.SetCondition(
					&sr.Status.Conditions,
					conditions.ConditionReady,
					metav1.ConditionFalse,
					conditions.ReasonReconciling,
					message,
				)
			}
		}
	})
}
