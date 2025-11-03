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

// Package kubeutil provides low-level Kubernetes utilities for controllers.
//
// This package consolidates utility functions used across BubuStack controllers
// for common Kubernetes operations including:
//
//   - Environment variable construction
//   - Event recording
//   - Finalizer management
//   - Resource listing and deletion
//   - Status patching with retry
//   - DNS-1123 name generation
//   - Duration configuration
//   - Retry and periodic execution loops
//   - JSON merge utilities
//   - RBAC management
//   - Service management
//   - Validation status computation
//
// # Environment Variables
//
// Build config environment variables for pods:
//
//	envVars := kubeutil.ConfigEnvVars(jsonBytes)
//	kubeutil.AppendConfigEnvVar(&envVars, jsonBytes)
//
// # Event Recording
//
// Record Kubernetes events with nil-safe logging fallback:
//
//	kubeutil.RecordEvent(recorder, logger, obj, corev1.EventTypeWarning, "Failed", "error message")
//
// # Finalizer Management
//
// Ensure finalizers are present on objects:
//
//	added, err := kubeutil.EnsureFinalizer(ctx, client, obj, "my.finalizer.io")
//
// # Resource Listing and Deletion
//
// List resources with label selectors:
//
//	err := kubeutil.ListByLabels(ctx, client, namespace, labels, &podList)
//
// Delete resources with NotFound handling:
//
//	err := kubeutil.DeleteIfExists(ctx, client, obj, logger)
//
// # Status Patching
//
// Retry status patches on conflicts:
//
//	err := kubeutil.RetryableStatusPatch(ctx, client, obj, func(o client.Object) {
//	    o.(*v1alpha1.StepRun).Status.Phase = enums.PhaseRunning
//	})
//
// # Name Generation
//
// Generate DNS-1123 compliant names:
//
//	name := kubeutil.ComposeName("storyrun", "my-story", "step-1")
//
// # Duration Configuration
//
// Resolve durations with fallbacks:
//
//	timeout := kubeutil.PositiveDurationOrDefault(userTimeout, 30*time.Second)
//	timeout := kubeutil.FirstPositiveDuration(step, story, default)
//
// # Retry and Periodic Execution
//
// Retry with exponential backoff:
//
//	err := kubeutil.Retry(ctx, kubeutil.RetryConfig{MaxAttempts: 3}, fn, retryable)
//
// Run periodic tasks:
//
//	err := kubeutil.Periodic(ctx, 10*time.Second, fn)
//
// # JSON Merge
//
// Merge runtime.RawExtension with-blocks:
//
//	merged, err := kubeutil.MergeWithBlocks(base, overlay)
//
// Merge string maps:
//
//	labels := kubeutil.MergeStringMaps(base, override)
//
// # RBAC Management
//
// Ensure Roles and RoleBindings:
//
//	result, err := kubeutil.EnsureRole(ctx, client, scheme, owner, name, ns, rules, style)
//	result, err := kubeutil.EnsureRoleBinding(ctx, client, scheme, owner, name, ns, roleRef, subjects, style)
//
// # Service Management
//
// Ensure Services with immutable field preservation:
//
//	svc, result, err := kubeutil.EnsureService(ctx, client, scheme, owner, desired)
//
// # Validation Status
//
// Compute Ready conditions from validation/binding states:
//
//	outcome := kubeutil.ComputeValidationOutcome(kubeutil.ValidationParams{
//	    ValidationErrors: errs,
//	    SuccessReason:    "Valid",
//	})
package kubeutil
