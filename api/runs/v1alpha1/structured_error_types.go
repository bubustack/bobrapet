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

package v1alpha1

import "k8s.io/apimachinery/pkg/runtime"

// StructuredErrorVersionV1 is the current version for StepRun.status.error payloads.
const StructuredErrorVersionV1 = "v1"

// StructuredErrorType categorizes the failure in a machine-readable way.
type StructuredErrorType string

const (
	StructuredErrorTypeTimeout        StructuredErrorType = "timeout"
	StructuredErrorTypeStorage        StructuredErrorType = "storage_error"
	StructuredErrorTypeSerialization  StructuredErrorType = "serialization_error"
	StructuredErrorTypeValidation     StructuredErrorType = "validation_error"
	StructuredErrorTypeInitialization StructuredErrorType = "initialization_error"
	StructuredErrorTypeExecution      StructuredErrorType = "execution_error"
	StructuredErrorTypeUnknown        StructuredErrorType = "unknown"
)

// StructuredError defines the versioned error contract stored in StepRun.status.error.
// It is emitted by SDKs and consumed by controllers/CLIs for diagnostics.
type StructuredError struct {
	// Version identifies the schema version for this error payload.
	Version string `json:"version"`

	// Type is a machine-readable error category.
	Type StructuredErrorType `json:"type"`

	// Message is the human-readable error message.
	Message string `json:"message"`

	// Retryable indicates whether a retry may succeed for this error.
	// Omitted when unknown.
	Retryable *bool `json:"retryable,omitempty"`

	// ExitCode is the container exit code, when known.
	ExitCode *int32 `json:"exitCode,omitempty"`

	// ExitClass mirrors the platform retry classification (success|retry|terminal|rateLimited).
	ExitClass string `json:"exitClass,omitempty"`

	// Code is an optional, component-specific error code.
	Code string `json:"code,omitempty"`

	// Details carries optional structured data for debugging.
	// +kubebuilder:pruning:PreserveUnknownFields
	Details *runtime.RawExtension `json:"details,omitempty"`
}
