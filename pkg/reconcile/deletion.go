package reconcile

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ShortCircuitDeletion checks the object's DeletionTimestamp and, when set,
// executes deleteFn to perform controller-specific cleanup before bubbling the
// returned ctrl.Result/error back to the caller. It reports whether deletion
// handling ran so reconcilers can skip the rest of their pipelines.
func ShortCircuitDeletion(
	ctx context.Context,
	obj client.Object,
	deleteFn func(context.Context) (ctrl.Result, error),
) (bool, ctrl.Result, error) {
	if obj == nil {
		return false, ctrl.Result{}, nil
	}
	ts := obj.GetDeletionTimestamp()
	if ts == nil || ts.IsZero() {
		return false, ctrl.Result{}, nil
	}
	res, err := deleteFn(ctx)
	return true, res, err
}
