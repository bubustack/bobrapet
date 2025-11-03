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

package refs

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NamespacedReferencer surfaces the ToNamespacedName helper implemented by the
// ObjectReference types embedded in refs.StoryReference, refs.EngramReference, etc.
// The Story and Impulse controllers rely on this interface so the loader helpers
// can derive stable cache keys without duplicating namespace resolution logic.
//
// +kubebuilder:object:generate=false
type NamespacedReferencer interface {
	ToNamespacedName(client.Object) types.NamespacedName
}

// NamespacedRuntimeObject represents API objects that can be fetched via
// controller-runtime's client and support DeepCopyObject for safe caching.
//
// +kubebuilder:object:generate=false
type NamespacedRuntimeObject interface {
	client.Object
	runtime.Object
}

// LoadNamespacedReference fetches the object identified by referencer, defaulting
// namespaces using the referencing object when the reference omits one.
//
// Behavior:
//   - Returns cached copies when cache already contains the resolved key.
//   - Uses factory to allocate a fresh object before calling reader.Get.
//   - Stores a DeepCopy inside cache when provided so callers can reuse the same
//     validation results across multiple steps.
//
// Returns:
//   - object: the referenced API object (or zero value on error).
//   - key: the namespaced name derived from the reference (useful for error text).
//   - error: nil on success; otherwise the client.Get error or validation failures.
func LoadNamespacedReference[T NamespacedRuntimeObject](
	ctx context.Context,
	reader client.Reader,
	referencing client.Object,
	referencer NamespacedReferencer,
	factory func() T,
	cache map[types.NamespacedName]T,
) (T, types.NamespacedName, error) {
	var zero T
	if referencer == nil {
		return zero, types.NamespacedName{}, fmt.Errorf("reference is nil")
	}

	key := referencer.ToNamespacedName(referencing)
	if cache != nil {
		if cached, ok := cache[key]; ok {
			return cached, key, nil
		}
	}

	if factory == nil {
		return zero, key, fmt.Errorf("factory is nil for %s", key.String())
	}

	obj := factory()
	if err := reader.Get(ctx, key, obj); err != nil {
		return zero, key, err
	}

	if cache != nil {
		// Store a DeepCopy so future callers can mutate cached pointers safely.
		cache[key] = obj.DeepCopyObject().(T)
	}

	return obj, key, nil
}

// LoadClusterObject fetches a cluster-scoped object (no namespace) by name using
// the provided factory to allocate the target type.
//
// Behavior:
//   - Requires a non-empty name; callers receive an error if the name is blank.
//   - Uses factory() to allocate a new object before issuing reader.Get.
//   - Returns the fetched object or the error from client.Get.
func LoadClusterObject[T NamespacedRuntimeObject](
	ctx context.Context,
	reader client.Reader,
	name string,
	factory func() T,
) (T, error) {
	var zero T
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return zero, fmt.Errorf("reference name is empty")
	}
	if factory == nil {
		return zero, fmt.Errorf("factory is nil for %s", trimmed)
	}

	obj := factory()
	if err := reader.Get(ctx, client.ObjectKey{Name: trimmed}, obj); err != nil {
		return zero, err
	}
	return obj, nil
}
