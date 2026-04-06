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

// Package conditions provides standard Kubernetes condition management for BubuStack resources.
//
// This package defines standard condition types and reasons used across all BubuStack
// resources, along with utilities for managing condition transitions. It implements
// the Kubernetes condition conventions for status reporting.
//
// # Condition Types
//
// Standard condition types are defined as constants:
//   - ConditionReady: Resource is ready for use
//   - ConditionProgressing: Resource is progressing towards ready state
//   - ConditionDegraded: Resource is degraded but operational
//   - ConditionValidated: Template/spec validation passed
//   - ConditionTemplateResolved: Template reference resolved successfully
//
// # Condition Reasons
//
// Standard reasons for condition transitions:
//   - ReasonValidationPassed, ReasonValidationFailed
//   - ReasonTemplateResolved, ReasonTemplateNotFound
//   - ReasonCompleted, ReasonExecutionFailed
//
// # Condition Manager
//
// The [ConditionManager] provides atomic condition updates:
//
//	cm := conditions.NewConditionManager(obj.Generation)
//	cm.SetReadyCondition(&obj.Status.Conditions, true, ReasonCompleted, "ready")
//	cm.TransitionConditions(&obj.Status.Conditions, "Ready", ReasonCompleted, "done")
//
// # Condition Queries
//
// Helper functions for querying conditions:
//
//	if conditions.IsReady(obj.Status.Conditions) {
//	    // Resource is ready
//	}
//	cond := conditions.GetCondition(obj.Status.Conditions, ConditionValidated)
//
// This package follows Kubernetes API conventions for condition management and
// integrates with the standard metav1.Condition type.
package conditions
