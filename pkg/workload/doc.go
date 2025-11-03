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

// Package workload provides generic utilities for reconciling controller-owned workloads.
//
// This package contains a generic [Ensure] function that handles the common pattern
// of fetching, creating, and patching Kubernetes workloads (Deployments, Jobs,
// StatefulSets, etc.) with consistent owner reference and drift detection.
//
// # Ensuring Workloads
//
// The [Ensure] function provides a generic reconcile pattern for any client.Object:
//
//	deployment, err := workload.Ensure(ctx, workload.EnsureOptions[*appsv1.Deployment]{
//	    Client:         client,
//	    Scheme:         scheme,
//	    Owner:          impulse,
//	    NamespacedName: types.NamespacedName{Name: "my-dep", Namespace: ns},
//	    Logger:         log,
//	    WorkloadType:   "Deployment",
//	    NewEmpty:       func() *appsv1.Deployment { return &appsv1.Deployment{} },
//	    BuildDesired:   func() (*appsv1.Deployment, error) { return buildDeployment() },
//	    NeedsUpdate:    deploymentNeedsUpdate,
//	    ApplyUpdate:    applyDeploymentUpdate,
//	})
//
// Behavior:
//   - Creates the workload if not found, setting controller owner reference.
//   - Patches existing workloads when NeedsUpdate returns true.
//   - Logs creation/update operations when logger is provided.
//   - Validates all required options before proceeding.
//
// # Options
//
// The [EnsureOptions] struct provides all knobs for reconciliation:
//   - Client, Scheme, Owner: Required controller-runtime dependencies
//   - NamespacedName: Identifies the workload to reconcile
//   - WorkloadType: Label for log messages (e.g., "Deployment")
//   - NewEmpty: Factory for zero-value target object
//   - BuildDesired: Constructs the desired workload state
//   - NeedsUpdate: Determines if patching is needed
//   - ApplyUpdate: Mutates current to match desired before patching
package workload
