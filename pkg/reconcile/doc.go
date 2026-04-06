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

// Package reconcile provides shared reconciliation utilities for BubuStack controllers.
//
// This package contains helpers for common reconciliation patterns including
// timeout handling, requeue logic, deletion processing, status updates, and
// template validation.
//
// # Controller Reconcile
//
// The [StartControllerReconcile] function sets up reconcile context with timeout
// and metrics:
//
//	ctx, finish := reconcile.StartControllerReconcile(ctx, "StoryRun", timeout)
//	defer finish(err)
//
// # Timeout Handling
//
// The [WithTimeout] function applies a timeout to the reconcile context:
//
//	ctx, cancel := reconcile.WithTimeout(ctx, 5*time.Minute)
//	defer cancel()
//
// # Requeue Logic
//
// Requeue helpers for common patterns:
//
//	reconcile.RequeueAfter(30*time.Second)  // Fixed delay
//	reconcile.RequeueBackoff(attempt)       // Exponential backoff
//	reconcile.NoRequeue()                   // Terminal state
//
// # Deletion Handling
//
// The [HandleDeletion] function provides a standard deletion flow:
//
//	if result, done := reconcile.HandleDeletion(ctx, obj, finalizer, cleanupFn); done {
//	    return result, nil
//	}
//
// # Template Validation
//
// Shared template validation helpers:
//
//	result, err := reconcile.ValidateTemplateRef(ctx, templateName, errEmptyRef, loader)
//	reconcile.ApplyTemplateCondition(cm, &conditions, result)
//	reconcile.EmitValidationEvent(recorder, logger, obj, result, prevCondition)
//
// # Status Updates
//
// Status update utilities:
//
//	reconcile.PatchStatus(ctx, client, obj)
//	reconcile.MarkDirty(ctx)  // Flag for deferred status update
//
// This package is designed to reduce boilerplate in controller reconcile loops
// and ensure consistent behavior across all BubuStack controllers.
package reconcile
