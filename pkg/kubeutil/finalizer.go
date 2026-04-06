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
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// EnsureFinalizer adds the supplied finalizer to obj (if missing) using a merge patch.
//
// Arguments:
//   - ctx context.Context: context for the patch operation.
//   - c client.Client: Kubernetes client.
//   - obj client.Object: the object to add the finalizer to.
//   - finalizer string: the finalizer string to add.
//
// Returns:
//   - bool: true when the finalizer was added, false if already present.
//   - error: patch error or nil on success.
//
// Behavior:
//   - Returns (false, nil) if the finalizer is already present.
//   - Uses merge patch for atomic finalizer addition.
func EnsureFinalizer(ctx context.Context, c client.Client, obj client.Object, finalizer string) (bool, error) {
	if controllerutil.ContainsFinalizer(obj, finalizer) {
		return false, nil
	}

	copyObj, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return false, fmt.Errorf("deep copy of %T does not implement client.Object", obj)
	}

	controllerutil.AddFinalizer(obj, finalizer)
	if err := c.Patch(ctx, obj, client.MergeFrom(copyObj)); err != nil {
		return false, err
	}
	return true, nil
}
