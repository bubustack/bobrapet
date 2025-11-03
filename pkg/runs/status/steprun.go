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
// logic that stamps timestamps, duration metrics, and Ready-condition updates. All
// writes flow through RetryableStatusPatch so concurrent reconciles converge on the
// same snapshot.
func PatchStepRunPhase(
	ctx context.Context,
	k8sClient client.Client,
	step *runsv1alpha1.StepRun,
	phase enums.Phase,
	message string,
) error {
	return kubeutil.RetryableStatusUpdate(ctx, k8sClient, step, func(obj client.Object) {
		s := obj.(*runsv1alpha1.StepRun)
		if s.Status.Retries < 0 {
			s.Status.Retries = 0
		}
		s.Status.Phase = phase
		s.Status.ObservedGeneration = s.Generation

		cm := conditions.NewConditionManager(s.Generation)

		switch phase {
		case enums.PhasePending:
			if s.Status.StartedAt == nil {
				s.Status.StartedAt = &metav1.Time{Time: time.Now()}
			}
			cm.SetCondition(
				&s.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				conditions.ReasonReconciling,
				message,
			)
		case enums.PhaseBlocked:
			cm.SetCondition(
				&s.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				conditions.ReasonSchedulingFailed,
				message,
			)
			s.Status.LastFailureMsg = message
		case enums.PhaseRunning:
			cm.SetCondition(
				&s.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				conditions.ReasonReconciling,
				message,
			)
		case enums.PhasePaused:
			cm.SetCondition(
				&s.Status.Conditions,
				conditions.ConditionReady,
				metav1.ConditionFalse,
				conditions.ReasonAwaitingTransport,
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
				conditions.ReasonCompleted,
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
				conditions.ReasonExecutionFailed,
				message,
			)
		}
	})
}
