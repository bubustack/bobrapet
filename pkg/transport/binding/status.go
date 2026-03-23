package binding

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
)

// ReadyState inspects a TransportBinding's Ready condition and reports whether the
// binding is ready, if the current state is terminal, and the associated status message.
// Callers can reuse the shared interpretation across controllers so user-facing
// diagnostics remain consistent.
//
// Arguments:
//   - binding *transportv1alpha1.TransportBinding: binding whose status should be interpreted.
//
// Returns:
//   - ready bool: true when ConditionReady is true.
//   - terminal bool: true when the Ready condition reason indicates a non-recoverable failure.
//   - message string: trimmed status or reason text suitable for logs/events/conditions.
func ReadyState(binding *transportv1alpha1.TransportBinding) (bool, bool, string) {
	if binding == nil {
		return false, false, "binding is nil"
	}

	cond := conditions.GetCondition(binding.Status.Conditions, conditions.ConditionReady)
	if cond == nil {
		return false, false, "ready condition not reported"
	}

	if cond.Status == metav1.ConditionTrue {
		return true, false, strings.TrimSpace(cond.Message)
	}

	msg := strings.TrimSpace(cond.Message)
	if msg == "" {
		msg = strings.TrimSpace(cond.Reason)
	}
	if msg == "" {
		msg = fmt.Sprintf("condition %s=%s", conditions.ConditionReady, string(cond.Status))
	}

	terminal := cond.Reason == conditions.ReasonTransportFailed ||
		cond.Reason == conditions.ReasonValidationFailed ||
		cond.Reason == conditions.ReasonInvalidConfiguration

	return false, terminal, msg
}

// PatchReadyCondition ensures the TransportBinding Ready condition matches the
// supplied ready/reason/message tuple using kubeutil.RetryableStatusPatch so
// controllers and background workers can keep condition semantics aligned.
//
// Arguments:
//   - ctx context.Context: propagated to the patch call.
//   - c client.Client: Kubernetes client used to submit the status kubeutil.
//   - binding *transportv1alpha1.TransportBinding: supplies the binding name/namespace.
//   - ready bool: desired Ready condition status.
//   - reason string / message string: Ready condition metadata.
//
// Returns:
//   - error: result of the status kubeutil.
func PatchReadyCondition(
	ctx context.Context,
	c client.Client,
	binding *transportv1alpha1.TransportBinding,
	ready bool,
	reason,
	message string,
) error {
	if binding == nil {
		return fmt.Errorf("binding is nil")
	}

	target := &transportv1alpha1.TransportBinding{}
	target.Name = binding.Name
	target.Namespace = binding.Namespace

	msg := strings.TrimSpace(message)

	return kubeutil.RetryableStatusPatch(ctx, c, target, func(obj client.Object) {
		tb := obj.(*transportv1alpha1.TransportBinding)
		cm := conditions.NewConditionManager(tb.Generation)
		cm.SetReadyCondition(&tb.Status.Conditions, ready, reason, msg)
	})
}
