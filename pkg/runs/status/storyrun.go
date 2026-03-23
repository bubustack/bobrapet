package status

import (
	"context"
	"encoding/json"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
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
	reason string,
	message string,
) error {
	return kubeutil.RetryableStatusPatch(ctx, k8sClient, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		oldPhase := sr.Status.Phase
		sr.Status.Phase = phase
		sr.Status.Message = message
		sr.Status.ObservedGeneration = sr.Generation
		ensureTraceInfo(ctx, &sr.Status.Trace)

		cm := conditions.NewConditionManager(sr.Generation)
		condReason := resolvePhaseReason(phase, reason)
		progressing := !phase.IsTerminal()
		degraded := phase == enums.PhaseFailed || phase == enums.PhaseTimeout || phase == enums.PhaseAborted

		switch phase {
		case enums.PhaseRunning:
			if oldPhase != enums.PhaseRunning {
				sr.Status.Attempts++
			} else if sr.Status.Attempts == 0 {
				// Defensive: ensure attempts is non-zero once running.
				sr.Status.Attempts = 1
			}
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
				condReason,
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
				condReason,
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
				condReason,
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
				cm.SetCondition(
					&sr.Status.Conditions,
					conditions.ConditionReady,
					metav1.ConditionFalse,
					condReason,
					message,
				)
			} else {
				cm.SetCondition(
					&sr.Status.Conditions,
					conditions.ConditionReady,
					metav1.ConditionFalse,
					condReason,
					message,
				)
			}
		}

		cm.SetProgressingCondition(&sr.Status.Conditions, progressing, condReason, message)
		cm.SetDegradedCondition(&sr.Status.Conditions, degraded, condReason, message)
	})
}

// BuildStoryRunError constructs a structured error for StoryRun.status.error.
// The returned RawExtension can be assigned to srun.Status.Error before patching.
func BuildStoryRunError(reason, message string, failedSteps map[string]bool, stepStates map[string]runsv1alpha1.StepState) *k8sruntime.RawExtension {
	errObj := map[string]any{
		"type":    reason,
		"message": message,
	}

	if len(failedSteps) > 0 {
		steps := make([]map[string]any, 0, len(failedSteps))
		for name := range failedSteps {
			step := map[string]any{"name": name}
			if state, ok := stepStates[name]; ok {
				step["phase"] = string(state.Phase)
				if state.Message != "" {
					step["message"] = state.Message
				}
			}
			steps = append(steps, step)
		}
		errObj["failedSteps"] = steps
	}

	raw, err := json.Marshal(errObj)
	if err != nil {
		return nil
	}
	return &k8sruntime.RawExtension{Raw: raw}
}
