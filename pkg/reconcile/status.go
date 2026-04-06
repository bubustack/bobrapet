// File: pkg/controllerx/status/status.go
package reconcile

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bubustack/bobrapet/pkg/kubeutil"
)

// PatchStatusIfChanged applies mutate() and updates status only if it changed.
// Returns (changed, err).
//
// This prevents self-reconcile storms caused by unconditional status writes.
func PatchStatusIfChanged[T client.Object](
	ctx context.Context,
	c client.Client,
	obj T,
	mutate func(T),
) (bool, error) {
	before := obj.DeepCopyObject().(T)
	mutate(obj)

	if apiequality.Semantic.DeepEqual(before, obj) {
		return false, nil
	}

	if err := kubeutil.RetryableStatusPatch(ctx, c, obj, func(o client.Object) {
		mutate(o.(T))
	}); err != nil {
		return false, err
	}

	return true, nil
}
