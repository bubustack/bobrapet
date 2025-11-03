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

package workload

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// EnsureOptions provides the knobs required to reconcile a controller-owned workload.
type EnsureOptions[T client.Object] struct {
	// Client is the controller-runtime client used for Get/Create/Patch operations.
	Client client.Client
	// Scheme is used when setting the controller reference on newly created workloads.
	Scheme *runtime.Scheme
	// Owner becomes the controller reference for the workload when it is created.
	Owner client.Object
	// NamespacedName identifies the workload to fetch or create.
	NamespacedName types.NamespacedName
	// Logger receives creation/update messages; optional but recommended.
	Logger logr.Logger
	// WorkloadType appears in log/error messages to aid debugging (e.g., "Deployment").
	WorkloadType string
	// NewEmpty returns a zero-value object used as the Get target.
	NewEmpty func() T
	// BuildDesired returns the desired workload state for comparison/creation.
	BuildDesired func() (T, error)
	// NeedsUpdate determines whether the live object should be patched.
	NeedsUpdate func(current, desired T) bool
	// ApplyUpdate mutates the live object so it matches the desired state prior to patching.
	ApplyUpdate func(current, desired T)
}

// Ensure fetches the workload identified by opts.NamespacedName, creating it if missing or
// patching controlled fields when drift is detected.
func Ensure[T client.Object](ctx context.Context, opts EnsureOptions[T]) (T, error) {
	var zero T
	if err := validateOptions(opts); err != nil {
		return zero, err
	}

	log := opts.Logger
	workloadLabel := strings.ToLower(opts.WorkloadType)
	current := opts.NewEmpty()
	if err := opts.Client.Get(ctx, opts.NamespacedName, current); err != nil {
		if errors.IsNotFound(err) {
			desired, derr := opts.BuildDesired()
			if derr != nil {
				return zero, fmt.Errorf("build desired %s: %w", workloadLabel, derr)
			}
			if err := controllerutil.SetControllerReference(opts.Owner, desired, opts.Scheme); err != nil {
				return zero, fmt.Errorf("set controller reference on %s: %w", workloadLabel, err)
			}
			if err := opts.Client.Create(ctx, desired); err != nil {
				return zero, fmt.Errorf("create %s: %w", workloadLabel, err)
			}
			if log.GetSink() != nil {
				log.Info(fmt.Sprintf("Created %s", opts.WorkloadType),
					"name", opts.NamespacedName.Name, "namespace", opts.NamespacedName.Namespace)
			}
			return desired, nil
		}
		return zero, fmt.Errorf("get %s: %w", workloadLabel, err)
	}

	desired, err := opts.BuildDesired()
	if err != nil {
		return zero, fmt.Errorf("build desired %s: %w", workloadLabel, err)
	}

	if opts.NeedsUpdate(current, desired) {
		original := current.DeepCopyObject().(T)
		opts.ApplyUpdate(current, desired)
		if err := opts.Client.Patch(ctx, current, client.MergeFrom(original)); err != nil {
			return zero, fmt.Errorf("patch %s: %w", workloadLabel, err)
		}
		if log.GetSink() != nil {
			log.Info(fmt.Sprintf("Updated %s", opts.WorkloadType),
				"name", opts.NamespacedName.Name, "namespace", opts.NamespacedName.Namespace)
		}
	}

	return current, nil
}

func validateOptions[T client.Object](opts EnsureOptions[T]) error {
	switch {
	case opts.Client == nil:
		return fmt.Errorf("workload.Ensure missing client")
	case opts.Scheme == nil:
		return fmt.Errorf("workload.Ensure missing scheme")
	case opts.Owner == nil:
		return fmt.Errorf("workload.Ensure missing owner")
	case opts.NewEmpty == nil:
		return fmt.Errorf("workload.Ensure missing NewEmpty factory")
	case opts.BuildDesired == nil:
		return fmt.Errorf("workload.Ensure missing BuildDesired function")
	case opts.NeedsUpdate == nil:
		return fmt.Errorf("workload.Ensure missing NeedsUpdate function")
	case opts.ApplyUpdate == nil:
		return fmt.Errorf("workload.Ensure missing ApplyUpdate function")
	case opts.NamespacedName == (types.NamespacedName{}):
		return fmt.Errorf("workload.Ensure missing workload key")
	case opts.WorkloadType == "":
		return fmt.Errorf("workload.Ensure missing workload type label")
	default:
		return nil
	}
}
