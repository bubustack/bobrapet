/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubeutil

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// UpdateFunc is a function that mutates an object.
type UpdateFunc func(obj client.Object)

// RetryableStatusPatch patches the /status subresource of obj using a merge patch,
// retrying on optimistic-concurrency conflicts.
//
// The function:
//  1. GETs the latest object by key,
//  2. deep-copies it as the patch base,
//  3. applies updateFunc to the fresh copy,
//  4. PATCHes status with client.MergeFrom(original).
//
// Arguments:
//   - ctx context.Context: context for the operations.
//   - cl client.Client: Kubernetes client.
//   - obj client.Object: the object whose status to patch.
//   - updateFunc UpdateFunc: function that mutates status fields only.
//
// Returns:
//   - error: non-conflict failures or retry budget exhaustion.
//
// Behavior:
//   - updateFunc must only mutate status fields, not spec/metadata.
//   - Works on fresh copies to avoid mutating caller's in-memory objects.
func RetryableStatusPatch(ctx context.Context, cl client.Client, obj client.Object, updateFunc UpdateFunc) error {
	key := client.ObjectKeyFromObject(obj)
	return retry.OnError(retry.DefaultRetry, errors.IsConflict, func() error {
		current := obj.DeepCopyObject().(client.Object)
		if err := cl.Get(ctx, key, current); err != nil {
			return err
		}

		original := current.DeepCopyObject().(client.Object)
		updateFunc(current)

		return cl.Status().Patch(ctx, current, client.MergeFrom(original))
	})
}

// RetryableStatusUpdate updates an object's status using optimistic concurrency.
//
// Arguments:
//   - ctx context.Context: context for the operations.
//   - cl client.Client: Kubernetes client.
//   - obj client.Object: the object whose status to update.
//   - updateFunc UpdateFunc: function that mutates status fields.
//
// Returns:
//   - error: non-conflict failures or retry budget exhaustion.
func RetryableStatusUpdate(ctx context.Context, cl client.Client, obj client.Object, updateFunc UpdateFunc) error {
	key := client.ObjectKeyFromObject(obj)
	return retry.OnError(retry.DefaultRetry, errors.IsConflict, func() error {
		current := obj.DeepCopyObject().(client.Object)
		if err := cl.Get(ctx, key, current); err != nil {
			return err
		}
		updateFunc(current)
		return cl.Status().Update(ctx, current)
	})
}
