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

// Package validation provides shared validation utilities for BubuStack resources.
//
// This package contains validation helpers used by controllers and webhooks,
// including error aggregation, reason extraction, and JSON schema validation.
//
// # Error Aggregation
//
// The [Aggregator] collects multiple validation errors for bulk status updates:
//
//	agg := validation.NewAggregator()
//	agg.AddFieldError("spec.steps[0].ref", conditions.ReasonEngramReferenceInvalid, "engram not found")
//	agg.AddFieldError("spec.steps[1].transport", conditions.ReasonTransportReferenceInvalid, "transport not found")
//	if agg.HasErrors() {
//	    return nil, agg.ToFieldErrors() // For webhooks
//	    // OR
//	    result := agg.ToValidationResult("success") // For controllers
//	}
//
// # Reason Errors
//
// The [WrapWithReason] function attaches a structured reason to an error:
//
//	err := validation.WrapWithReason(conditions.ReasonTemplateNotFound, fmt.Errorf("template not found"))
//	reason := validation.ExtractReason(err) // Returns conditions.ReasonTemplateNotFound
//
// # JSON Schema Validation
//
// The [ValidateJSONSchema] function validates that bytes represent valid JSON:
//
//	if err := validation.ValidateJSONSchema(schemaBytes); err != nil {
//	    return fmt.Errorf("invalid schema: %w", err)
//	}
//
// This package is designed for use in admission webhooks and controller
// reconcile loops that need to collect and report multiple validation errors.
package validation
