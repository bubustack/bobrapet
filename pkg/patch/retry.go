package patch

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// UpdateFunc is a function that mutates an object.
type UpdateFunc func(obj client.Object)

// RetryableStatusPatch applies a status patch to an object with retry-on-conflict logic.
// It fetches the latest version of the object before applying the update.
func RetryableStatusPatch(ctx context.Context, cl client.Client, obj client.Object, updateFunc UpdateFunc) error {
	return retry.OnError(retry.DefaultRetry, errors.IsConflict, func() error {
		// Get the latest version of the object.
		key := client.ObjectKeyFromObject(obj)
		if err := cl.Get(ctx, key, obj); err != nil {
			return err
		}

		// Apply the update logic to the fresh object.
		patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
		updateFunc(obj)

		// Attempt to patch the status.
		return cl.Status().Patch(ctx, obj, patch)
	})
}
