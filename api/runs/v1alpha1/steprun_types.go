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

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
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
// - Executing a built-in primitive action (condition, parallel, etc.)
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
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;watch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns/status,verbs=patch;update
// +kubebuilder:selectablefield:JSONPath=".spec.storyRunRef.name"
// +kubebuilder:selectablefield:JSONPath=".spec.stepId"
// +kubebuilder:selectablefield:JSONPath=".status.phase"
type StepRun struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of StepRun
	// +required
	Spec StepRunSpec `json:"spec"`

	// status defines the observed state of StepRun
	// +optional
	Status StepRunStatus `json:"status,omitzero"`
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

	// IdempotencyKey provides a stable identifier for step side effects.
	// External systems should use this to dedupe effects across retries.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	IdempotencyKey string `json:"idempotencyKey,omitempty"`

	// What should this step execute?
	// Always references an Engram (for custom logic) or uses built-in primitive type
	// Primitive types: condition, parallel, sleep, stop, wait, executeStory, gate.
	EngramRef *refs.EngramReference `json:"engramRef,omitempty"` // Reference to an Engram for custom logic

	// TemplateGeneration records the EngramTemplate metadata.generation at the time
	// this StepRun was created. Enables auditing and replay against the exact template version.
	// +optional
	TemplateGeneration int64 `json:"templateGeneration,omitempty"`

	// What data should be passed to this step?
	// This is the resolved input after template evaluation and data flow from previous steps
	// Examples:
	// - HTTP request: {"url": "https://api.example.com", "method": "GET", "headers": {...}}
	// - OpenAI call: {"prompt": "Summarize this text: ...", "model": "gpt-4", "temperature": 0.7}
	// - Condition: {"expression": "{{ eq steps.http_check.output.status 200 }}"}
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

// HandoffPhase captures lifecycle cutover progress for streaming workloads.
// +kubebuilder:validation:Enum=pending;draining;cutover;ready
type HandoffPhase string

const (
	HandoffPhasePending  HandoffPhase = "pending"
	HandoffPhaseDraining HandoffPhase = "draining"
	HandoffPhaseCutover  HandoffPhase = "cutover"
	HandoffPhaseReady    HandoffPhase = "ready"
)

// HandoffStatus reports streaming upgrade/handoff progress for a StepRun.
type HandoffStatus struct {
	// Strategy reflects the lifecycle strategy in effect for this streaming workload.
	// +optional
	Strategy *transportv1alpha1.TransportUpgradeStrategy `json:"strategy,omitempty"`

	// Phase indicates the current handoff stage.
	// +optional
	Phase HandoffPhase `json:"phase,omitempty"`

	// Message provides a human-readable explanation for the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// UpdatedAt records when the handoff status was last updated.
	// +optional
	UpdatedAt *metav1.Time `json:"updatedAt,omitempty"`
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

	// LastOutputAt records when the step's output was last written.
	// Useful for staleness detection — downstream consumers can compare this
	// against their own scheduling time.
	// +optional
	LastOutputAt *metav1.Time `json:"lastOutputAt,omitempty"`

	// Trace captures the OpenTelemetry trace context for this StepRun.
	// +optional
	Trace *TraceInfo `json:"trace,omitempty"`

	// Process execution details
	ExitCode  int32           `json:"exitCode,omitempty"`  // What exit code did the container return? (0 = success)
	ExitClass enums.ExitClass `json:"exitClass,omitempty"` // How should we interpret this exit? (success|retry|terminal|rateLimited)
	PodName   string          `json:"podName,omitempty"`   // Which Kubernetes pod executed this step?

	// Retry tracking (aligned with our Story/Engram/Template retry design)
	Retries        int32        `json:"retries"`                  // How many times has this step been retried?
	NextRetryAt    *metav1.Time `json:"nextRetryAt,omitempty"`    // When will the next retry happen?
	LastFailureMsg string       `json:"lastFailureMsg,omitempty"` // Most recent failure message for debugging

	// InputSchemaRef identifies the schema used to validate resolved step inputs.
	// +optional
	InputSchemaRef *SchemaReference `json:"inputSchemaRef,omitempty"`

	// OutputSchemaRef identifies the schema expected for step outputs.
	// +optional
	OutputSchemaRef *SchemaReference `json:"outputSchemaRef,omitempty"`

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

	// Optional logs reference emitted by the SDK/Engram.
	// When storage is enabled, the SDK may upload logs and store a storage ref map here.
	// +kubebuilder:pruning:PreserveUnknownFields
	Logs *runtime.RawExtension `json:"logs,omitempty"`

	// If the step failed, what was the error?
	// Contains structured error information for debugging.
	// Must conform to the StructuredError v1 contract (https://bubustack.io/docs/api/errors).
	// Examples:
	// - HTTP error: {"type": "http_error", "status": 404, "message": "Not Found", "url": "..."}
	// - Timeout: {"type": "timeout", "duration": "30s", "stage": "execution"}
	// - Validation: {"type": "validation_error", "field": "url", "message": "Invalid URL format"}
	Error *StructuredError `json:"error,omitempty"`

	// Conditions provide the canonical lifecycle status. Phase/Message are summaries.
	// The Ready condition reason holds stable reason codes for state transitions.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Handoff captures streaming upgrade/cutover state for realtime workloads.
	// +optional
	Handoff *HandoffStatus `json:"handoff,omitempty"`

	// Step coordination - which steps must complete before this one can start
	// Uses the same "needs" terminology as our Story API for consistency
	Needs []string `json:"needs,omitempty"` // StepRun names that must complete first

	// Signals captures lightweight, controller-friendly metadata emitted by running Engrams.
	// These values are intended for template expressions and branching logic (e.g., last transcript text).
	// Payloads should remain small (<8 KiB) to avoid bloating status objects.
	// +optional
	Signals map[string]runtime.RawExtension `json:"signals,omitempty"`

	// SignalEvents captures an append-only sequence of emitted signals for replay.
	// Payloads should remain small (<8 KiB) to avoid bloating status objects.
	// +optional
	// +kubebuilder:validation:MaxItems=256
	// +kubebuilder:validation:ListType=map
	// +kubebuilder:validation:ListMapKey=seq
	SignalEvents []SignalEvent `json:"signalEvents,omitempty"`

	// Effects records external side effects emitted by the step.
	// This ledger is append-only and intended for idempotency checks.
	// Payloads should remain small to avoid bloating status objects.
	// +optional
	// +kubebuilder:validation:MaxItems=256
	// +kubebuilder:validation:ListType=map
	// +kubebuilder:validation:ListMapKey=seq
	Effects []EffectRecord `json:"effects,omitempty"`
}

// +kubebuilder:object:root=true
type StepRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
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

	// Placement overrides pod scheduling constraints (node selector, tolerations, affinity).
	// +optional
	Placement *bubuv1alpha1.PlacementPolicy `json:"placement,omitempty"`

	// Cache overrides output caching behavior.
	// +optional
	Cache *bubuv1alpha1.CachePolicy `json:"cache,omitempty"`

	// Debug toggles component-level debug logging.
	Debug *bool `json:"debug,omitempty"`

	// Job behavior overrides
	BackoffLimit            *int32  `json:"backoffLimit,omitempty"`            // Override job backoff limit
	TTLSecondsAfterFinished *int32  `json:"ttlSecondsAfterFinished,omitempty"` // Override job cleanup time
	RestartPolicy           *string `json:"restartPolicy,omitempty"`           // Override restart policy (Never, OnFailure)
}

// EffectRecord captures a single external side effect for idempotency.
type EffectRecord struct {
	// Seq is a monotonically increasing sequence number for this StepRun.
	Seq uint64 `json:"seq"`
	// Key identifies the external effect (idempotency key or effect ID).
	Key string `json:"key"`
	// Status captures the outcome (e.g., succeeded, failed).
	// +optional
	Status string `json:"status,omitempty"`
	// EmittedAt records when the effect was emitted.
	// +optional
	EmittedAt *metav1.Time `json:"emittedAt,omitempty"`
	// Details carries optional structured metadata about the effect.
	// +optional
	Details *runtime.RawExtension `json:"details,omitempty"`
}

// SignalEvent captures a single emitted signal for replay.
type SignalEvent struct {
	// Seq is a monotonically increasing sequence number for this StepRun.
	Seq uint64 `json:"seq"`
	// Key identifies the signal name.
	Key string `json:"key"`
	// EmittedAt records when the signal was emitted.
	// +optional
	EmittedAt *metav1.Time `json:"emittedAt,omitempty"`
	// Payload carries the signal payload.
	// +optional
	Payload *runtime.RawExtension `json:"payload,omitempty"`
}

func init() {
	SchemeBuilder.Register(&StepRun{}, &StepRunList{})
}
