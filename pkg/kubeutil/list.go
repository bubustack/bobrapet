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

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ListByLabels fetches objects in a namespace that match the provided labels.
//
// Arguments:
//   - ctx context.Context: context for the list operation.
//   - c client.Client: Kubernetes client.
//   - namespace string: namespace to list in; empty string lists cluster-wide.
//   - labels map[string]string: label selector; empty map matches all labels.
//   - list client.ObjectList: the list object to populate with results.
//
// Returns:
//   - error: list error or nil on success.
//
// Behavior:
//   - Applies namespace filter when namespace is non-empty.
//   - Applies label matching when labels is non-empty.
func ListByLabels(
	ctx context.Context,
	c client.Client,
	namespace string,
	labels map[string]string,
	list client.ObjectList,
) error {
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if len(labels) > 0 {
		opts = append(opts, client.MatchingLabels(labels))
	}
	return c.List(ctx, list, opts...)
}
