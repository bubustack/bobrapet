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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=step
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=.status.phase
type StepRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StepRunSpec   `json:"spec,omitempty"`
	Status StepRunStatus `json:"status,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="size(self.storyRunRef) > 0",message="storyRunRef is required"
// +kubebuilder:validation:XValidation:rule="self.storyRunRef.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="storyRunRef must be valid DNS-1123 label"
// +kubebuilder:validation:XValidation:rule="size(self.stepId) > 0",message="stepId is required"
// +kubebuilder:validation:XValidation:rule="!has(self.engramRef) || self.engramRef.matches('^([a-z0-9]([-a-z0-9]*[a-z0-9])?/)?[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="engramRef must be valid name or ns/name format"
type StepRunSpec struct {
	StoryRunRef string `json:"storyRunRef"`
	StepID      string `json:"stepId"`

	// Engram execution details
	EngramRef string                `json:"engramRef,omitempty"` // reference to Engram CRD
	Image     string                `json:"image,omitempty"`     // OCI image (fallback if no EngramRef)
	Input     *runtime.RawExtension `json:"input,omitempty"`     // JSON input for the Engram

	// ABI contract environment
	Deadline         string `json:"deadline,omitempty"` // RFC3339 timestamp
	IdempotencyToken string `json:"idempotencyToken,omitempty"`

	// Execution configuration
	Mode string `json:"mode,omitempty"` // container|pooled|grpc|wasi
	// +kubebuilder:validation:XValidation:rule="size(self) == 0 || self.matches('^([0-9]+(\\\\.[0-9]+)?(ms|s|m|h))+$')",message="timeout must be a valid duration"
	Timeout     string       `json:"timeout,omitempty"` // duration string
	RetryPolicy *RetryPolicy `json:"retryPolicy,omitempty"`
}

type StepRunStatus struct {
	Phase string `json:"phase,omitempty"` // Pending|Running|Succeeded|Failed|Canceled|Paused

	// Execution metadata
	StartedAt  string `json:"startedAt,omitempty"`  // RFC3339 timestamp
	FinishedAt string `json:"finishedAt,omitempty"` // RFC3339 timestamp
	ExitCode   int32  `json:"exitCode,omitempty"`   // Actual exit code from container
	ExitClass  string `json:"exitClass,omitempty"`  // success|retry|terminal|rateLimited
	Retries    int    `json:"retries,omitempty"`
	PodName    string `json:"podName,omitempty"`

	// ABI contract outputs - Enhanced for large data handling
	Output    *runtime.RawExtension `json:"output,omitempty"`    // inline output if <1MB
	Artifacts []ArtifactRef         `json:"artifacts,omitempty"` // references to artifacts in store
	Error     *runtime.RawExtension `json:"error,omitempty"`     // structured error from stderr

	// Observability
	LogsURI string `json:"logsURI,omitempty"`
	TraceID string `json:"traceID,omitempty"` // OpenTelemetry trace ID

	// Dependencies and triggering (simplified communication pattern)
	Dependencies []string `json:"dependencies,omitempty"` // StepRuns this step depends on
	Triggers     []string `json:"triggers,omitempty"`     // StepRuns triggered by this step

	// Pause information
	PauseInfo *PauseInfo `json:"pauseInfo,omitempty"`
}

type RetryPolicy struct {
	MaxRetries int    `json:"maxRetries,omitempty"`
	Backoff    string `json:"backoff,omitempty"`   // exponential|linear|fixed
	BaseDelay  string `json:"baseDelay,omitempty"` // duration string
	MaxDelay   string `json:"maxDelay,omitempty"`  // duration string

	// Circuit breaker configuration
	CircuitBreaker *CircuitBreakerConfig `json:"circuitBreaker,omitempty"`
}

// CircuitBreakerConfig defines circuit breaker behavior for resilient execution
type CircuitBreakerConfig struct {
	// Failure threshold before opening circuit
	FailureThreshold int32 `json:"failureThreshold,omitempty"` // default: 5

	// Success threshold to close circuit from half-open
	SuccessThreshold int32 `json:"successThreshold,omitempty"` // default: 3

	// Time window to evaluate failures
	TimeWindow string `json:"timeWindow,omitempty"` // default: "1m"

	// Reset timeout when circuit is open
	ResetTimeout string `json:"resetTimeout,omitempty"` // default: "30s"

	// Fallback action when circuit is open
	FallbackAction *FallbackAction `json:"fallbackAction,omitempty"`

	// What constitutes a failure (HTTP codes, exit codes, etc.)
	FailureConditions []FailureCondition `json:"failureConditions,omitempty"`
}

// FallbackAction defines what to do when circuit is open
type FallbackAction struct {
	Type string `json:"type"` // skip|fail|engram|value

	// For type=engram: reference to fallback engram
	EngramRef string `json:"engramRef,omitempty"`

	// For type=value: static fallback value
	Value *runtime.RawExtension `json:"value,omitempty"`
}

// FailureCondition defines what constitutes a failure
type FailureCondition struct {
	Type string `json:"type"` // httpStatus|exitCode|exception|timeout

	// For httpStatus: HTTP status codes (e.g., "5xx", ">=400")
	HTTPStatusPattern string `json:"httpStatusPattern,omitempty"`

	// For exitCode: container exit codes (e.g., "!=0")
	ExitCodePattern string `json:"exitCodePattern,omitempty"`

	// For exception: exception patterns
	ExceptionPattern string `json:"exceptionPattern,omitempty"`

	// For timeout: consider timeouts as failures
	IncludeTimeouts bool `json:"includeTimeouts,omitempty"`
}

type ArtifactRef struct {
	Name   string `json:"name"`          // artifact name
	URI    string `json:"uri"`           // S3/MinIO URI
	SHA256 string `json:"sha256"`        // content hash
	Size   int64  `json:"size"`          // size in bytes
	TTL    string `json:"ttl,omitempty"` // time to live
}

// +kubebuilder:object:root=true
type StepRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StepRun `json:"items"`
}

// PauseInfo contains information about a paused step
type PauseInfo struct {
	// When the step was paused
	PausedAt *metav1.Time `json:"pausedAt,omitempty"`

	// Reason for pausing
	Reason string `json:"reason,omitempty"`

	// Expected resume time (for timeout-based resume)
	ResumeAt *metav1.Time `json:"resumeAt,omitempty"`

	// External trigger status
	ExternalTriggerStatus string `json:"externalTriggerStatus,omitempty"`
}

func init() {
	SchemeBuilder.Register(&StepRun{}, &StepRunList{})
}
