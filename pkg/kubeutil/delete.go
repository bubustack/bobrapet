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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DeleteLogger captures the logging interface required by DeleteIfExists.
type DeleteLogger interface {
	Info(msg string, keysAndValues ...any)
	Error(err error, msg string, keysAndValues ...any)
}

// DeleteIfExists deletes the provided object using background propagation and
// treats IsNotFound as success so controllers can safely converge deletes.
//
// Arguments:
//   - ctx context.Context: context for the delete operation.
//   - c client.Client: Kubernetes client.
//   - obj client.Object: the object to delete.
//   - logger DeleteLogger: optional logger for operation details.
//
// Returns:
//   - error: nil on success (including NotFound), or the delete error.
func DeleteIfExists(ctx context.Context, c client.Client, obj client.Object, logger DeleteLogger) error {
	if obj == nil {
		return nil
	}
	if !obj.GetDeletionTimestamp().IsZero() {
		if logger != nil {
			logger.Info("Object is already deleting; skipping delete", objectLogFields(obj)...)
		}
		return nil
	}
	if err := c.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		if apierrors.IsNotFound(err) {
			if logger != nil {
				logger.Info("Object already absent; skipping delete", objectLogFields(obj)...)
			}
			return nil
		}
		if logger != nil {
			logger.Error(err, "Failed to delete object", objectLogFields(obj)...)
		}
		return err
	}
	if logger != nil {
		logger.Info("Deleted object", objectLogFields(obj)...)
	}
	return nil
}

func objectLogFields(obj client.Object) []any {
	gvk := obj.GetObjectKind().GroupVersionKind().String()
	if gvk == "" {
		gvk = fmt.Sprintf("%T", obj)
	}
	return []any{
		"kind", gvk,
		"name", obj.GetName(),
		"namespace", obj.GetNamespace(),
	}
}
