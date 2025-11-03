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

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
)

// StepRun represents the execution of a single step within a StoryRun
//
// Think of the relationship like this:
// - Story = A recipe (defines the cooking process)
// - StoryRun = Actually cooking the dish (following the recipe)
// - StepRun = Each individual cooking step (chopping vegetables, heating oil, etc.)
//
// StepRuns are the atomic units of execution. They represent:
// - Running a single Engram with specific inputs
// - Executing a built-in primitive action (condition, loop, etc.)
// - Launching a sub-story
//
// StepRuns provide detailed execution tracking:
// - Exactly when each step started and finished
// - What inputs were provided and outputs produced
// - Resource usage and performance metrics
// - Detailed error information for debugging
// - Retry attempts and circuit breaker status
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=step,categories={bubu,ai,runs}
// +kubebuilder:printcolumn:name="StoryRun",type=string,JSONPath=.spec.storyRunRef.name
// +kubebuilder:printcolumn:name="Step",type=string,JSONPath=.spec.stepId
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=.status.phase
// +kubebuilder:printcolumn:name="Retries",type=integer,JSONPath=.status.retries
// +kubebuilder:printcolumn:name="Duration",type=string,JSONPath=.status.duration
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;watch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns/status,verbs=patch;update
type StepRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StepRunSpec   `json:"spec,omitempty"`
	Status StepRunStatus `json:"status,omitempty"`
}

// StepRunSpec defines exactly how to execute this specific step
type StepRunSpec struct {
	// Which StoryRun does this step belong to?
	// Used to track the parent execution and coordinate step ordering
	// +kubebuilder:validation:Required
	StoryRunRef refs.StoryRunReference `json:"storyRunRef"`

	// Which step in the Story is this executing?
	// References the step name/ID from the original Story definition
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	StepID string `json:"stepId"`

	// What should this step execute?
	// Always references an Engram (for custom logic) or uses built-in primitive type
	// Primitive types: condition, loop, parallel, sleep, stop, switch, filter, transform, etc.
	EngramRef *refs.EngramReference `json:"engramRef,omitempty"` // Reference to an Engram for custom logic

	// What data should be passed to this step?
	// This is the resolved input after CEL evaluation and data flow from previous steps
	// Examples:
	// - HTTP request: {"url": "https://api.example.com", "method": "GET", "headers": {...}}
	// - OpenAI call: {"prompt": "Summarize this text: ...", "model": "gpt-4", "temperature": 0.7}
	// - Condition: {"expression": "{{ steps.http_check.output.status == 200 }}"}
	// - Loop: {"items": ["file1.txt", "file2.txt"], "concurrency": 3}
	// +kubebuilder:pruning:PreserveUnknownFields
	Input *runtime.RawExtension `json:"input,omitempty"`

	// Execution timing constraints (inherited from Story → Engram → Template hierarchy)
	// How long to wait before considering this step failed?
	// Examples: "30s", "5m", "1h"
	// Priority: StepRun > Story.steps[].execution.timeout > Engram.execution.timeout > Template.execution.timeout
	Timeout string `json:"timeout,omitempty"`

	// What to do if this step fails?
	// Priority: StepRun > Story.steps[].execution.retry > Engram.execution.retry > Template.execution.retry
	Retry *bubuv1alpha1.RetryPolicy `json:"retry,omitempty"`

	// Last-minute overrides for special situations
	// This has the highest priority in the configuration hierarchy
	ExecutionOverrides *StepExecutionOverrides `json:"executionOverrides,omitempty"`

	// DownstreamTargets instructs the Engram's SDK what to do with the output.
	// This is populated by the storyrun-controller to enable direct Engram-to-Engram communication.
	// Can be a list to support fanning out to multiple parallel steps.
	// +optional
	DownstreamTargets []DownstreamTarget `json:"downstreamTargets,omitempty"`

	// RequestedManifest lists the metadata fields the controller expects the SDK
	// to materialize alongside the offloaded output. These are derived from CEL expressions
	// that reference this step's outputs (e.g., len(steps.foo.output.bar)).
	// +optional
	RequestedManifest []ManifestRequest `json:"requestedManifest,omitempty"`
}

// DownstreamTarget defines the destination for an Engram's output in real-time execution mode.
// Exactly one of the fields must be set.
type DownstreamTarget struct {
	// GRPCTarget specifies the connection details for the next Engram in the chain.
	// +optional
	GRPCTarget *GRPCTarget `json:"grpc,omitempty"`

	// Terminate indicates that this is the last step in the flow.
	// +optional
	Terminate *TerminateTarget `json:"terminate,omitempty"`
}

// GRPCTarget provides the endpoint for a downstream gRPC peer.
type GRPCTarget struct {
	// Endpoint is the address of the downstream service (e.g., "engram-b.default.svc:9000").
	// +kubebuilder:validation:Required
	Endpoint string `json:"endpoint"`
}

// TerminateTarget specifies how a terminal step should conclude the workflow.
type TerminateTarget struct {
	// StopMode defines the final phase of the StoryRun (e.g., success, failure).
	// +kubebuilder:validation:Required
	StopMode enums.StopMode `json:"stopMode"`
}

// ManifestOperation enumerates the metadata operations supported for step manifests.
type ManifestOperation string

const (
	// ManifestOperationExists records whether the referenced field exists/non-nil.
	ManifestOperationExists ManifestOperation = "exists"
	// ManifestOperationLength records the length of the referenced field when it is an array, map, or string.
	ManifestOperationLength ManifestOperation = "length"
)

// ManifestRequest describes a single output field and the metadata operations required for it.
type ManifestRequest struct {
	// Path is the dot/bracket notation path relative to the step output root.
	// Examples: "result.items", "tools", "items[0].id".
	// +kubebuilder:validation:MinLength=1
	Path string `json:"path"`
	// Operations lists the metadata operations that should be computed for this path.
	// Defaults to ["exists"] when omitted.
	// +optional
	Operations []ManifestOperation `json:"operations,omitempty"`
}

// StepManifestData captures the metadata emitted by the SDK for a single manifest path.
type StepManifestData struct {
	// Exists indicates whether the referenced field was present and non-nil.
	// +optional
	Exists *bool `json:"exists,omitempty"`
	// Length contains the computed length when requested and applicable.
	// +optional
	Length *int64 `json:"length,omitempty"`
	// Truncated signals that the SDK could not compute the full metadata due to limits.
	// +optional
	Truncated bool `json:"truncated,omitempty"`
	// Error contains a warning message emitted by the SDK when it cannot honour the manifest request.
	// +optional
	Error string `json:"error,omitempty"`
	// Sample holds an optional representative slice of the data (implementation-defined).
	// +optional
	Sample *runtime.RawExtension `json:"sample,omitempty"`
}

// StepRunStatus tracks the detailed execution state of this individual step
// +kubebuilder:validation:XValidation:message="status.conditions reason field must be <= 64 characters",rule="!has(self.conditions) || self.conditions.all(c, !has(c.reason) || size(c.reason) <= 64)"
// +kubebuilder:validation:XValidation:message="status.conditions message field must be <= 2048 characters",rule="!has(self.conditions) || self.conditions.all(c, !has(c.message) || size(c.message) <= 2048)"
type StepRunStatus struct {
	// observedGeneration is the most recent generation observed for this StepRun. It corresponds to the
	// StepRun's generation, which is updated on mutation by the API Server.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Current execution phase
	// - Pending: StepRun created but not yet started (waiting for dependencies)
	// - Running: Step is actively executing
	// - Succeeded: Step completed successfully
	// - Failed: Step failed and will not be retried
	// - Canceled: Step was canceled by user or system
	// - Paused: Step is paused waiting for external input or approval
	Phase enums.Phase `json:"phase,omitempty"`

	// Execution timing and metadata
	StartedAt  *metav1.Time `json:"startedAt,omitempty"`  // When did this step start?
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"` // When did this step finish?
	Duration   string       `json:"duration,omitempty"`   // How long did execution take? (calculated field)

	// Process execution details
	ExitCode  int32           `json:"exitCode,omitempty"`  // What exit code did the container return? (0 = success)
	ExitClass enums.ExitClass `json:"exitClass,omitempty"` // How should we interpret this exit? (success|retry|terminal|rateLimited)
	PodName   string          `json:"podName,omitempty"`   // Which Kubernetes pod executed this step?

	// Retry tracking (aligned with our Story/Engram/Template retry design)
	Retries        int32        `json:"retries,omitempty"`        // How many times has this step been retried?
	NextRetryAt    *metav1.Time `json:"nextRetryAt,omitempty"`    // When will the next retry happen?
	LastFailureMsg string       `json:"lastFailureMsg,omitempty"` // Most recent failure message for debugging

	// What did this step produce?
	// For small outputs (< 1MB), stored inline here
	// For large outputs, automatically stored in shared storage (if enabled) and path referenced here
	// This gets validated against the Engram's outputSchema (if applicable)
	// Examples:
	// - HTTP client: {"status": 200, "body": "...", "headers": {...}, "responseTime": 150}
	// - OpenAI: {"response": "Summary text...", "usage": {"tokens": 250}, "model": "gpt-4"}
	// - Large file: {"result": "success", "outputPath": "/shared/storage/story-123/step-fetch/response.json"}
	// +kubebuilder:pruning:PreserveUnknownFields
	Output *runtime.RawExtension `json:"output,omitempty"`

	// If the step failed, what was the error?
	// Contains structured error information for debugging
	// Examples:
	// - HTTP error: {"type": "http_error", "status": 404, "message": "Not Found", "url": "..."}
	// - Timeout: {"type": "timeout", "duration": "30s", "stage": "execution"}
	// - Validation: {"type": "validation_error", "field": "url", "message": "Invalid URL format"}
	// +kubebuilder:pruning:PreserveUnknownFields
	Error *runtime.RawExtension `json:"error,omitempty"`

	// Conditions provide a standard way to convey the state of the StepRun.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Step coordination - which steps must complete before this one can start
	// Uses the same "needs" terminology as our Story API for consistency
	Needs []string `json:"needs,omitempty"` // StepRun names that must complete first

	// Manifest contains metadata captured for this step's output that enables CEL expressions
	// to execute without hydrating large blobs from storage.
	// The map key matches the ManifestRequest path.
	// +optional
	Manifest map[string]StepManifestData `json:"manifest,omitempty"`

	// ManifestWarnings contains any warnings produced while computing manifest data (e.g., unsupported operations).
	// +optional
	ManifestWarnings []string `json:"manifestWarnings,omitempty"`
}

// +kubebuilder:object:root=true
type StepRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StepRun `json:"items"`
}

// StepExecutionOverrides allows overriding execution parameters at the StepRun level
// This has the highest priority in the configuration hierarchy: StepRun > Story > Engram > Template
type StepExecutionOverrides struct {
	// Resource overrides - only the essentials
	CPURequest    *string `json:"cpuRequest,omitempty"`    // Override CPU request (e.g., "100m", "0.5")
	CPULimit      *string `json:"cpuLimit,omitempty"`      // Override CPU limit (e.g., "500m", "1")
	MemoryRequest *string `json:"memoryRequest,omitempty"` // Override memory request (e.g., "128Mi", "1Gi")
	MemoryLimit   *string `json:"memoryLimit,omitempty"`   // Override memory limit (e.g., "256Mi", "2Gi")

	// Job behavior overrides
	BackoffLimit            *int32  `json:"backoffLimit,omitempty"`            // Override job backoff limit
	TTLSecondsAfterFinished *int32  `json:"ttlSecondsAfterFinished,omitempty"` // Override job cleanup time
	RestartPolicy           *string `json:"restartPolicy,omitempty"`           // Override restart policy (Never, OnFailure)
}

func init() {
	SchemeBuilder.Register(&StepRun{}, &StepRunList{})
}
