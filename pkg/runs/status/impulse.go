package status

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
)

// PatchImpulsePhase centralizes Impulse phase transitions by applying
// RetryableStatusPatch and keeping the Ready condition in sync with the new
// phase.
func PatchImpulsePhase(
	ctx context.Context,
	k8sClient client.Client,
	impulse *v1alpha1.Impulse,
	phase enums.Phase,
	message string,
) error {
	return kubeutil.RetryableStatusPatch(ctx, k8sClient, impulse, func(obj client.Object) {
		i := obj.(*v1alpha1.Impulse)
		i.Status.Phase = phase
		i.Status.ObservedGeneration = i.Generation

		cm := conditions.NewConditionManager(i.Generation)
		status := metav1.ConditionFalse
		reason := conditions.ReasonReconciling

		switch phase {
		case enums.PhaseRunning:
			status = metav1.ConditionTrue
			reason = conditions.ReasonDeploymentReady
		case enums.PhaseBlocked:
			reason = conditions.ReasonSchedulingFailed
		case enums.PhaseFailed:
			reason = conditions.ReasonExecutionFailed
		}

		cm.SetCondition(&i.Status.Conditions, conditions.ConditionReady, status, reason, message)
	})
}
