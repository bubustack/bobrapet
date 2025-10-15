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

// Package enums provides typed string enumerations used across BubuStack APIs.
//
// These enums provide type safety, validation, and consistency across all
// BubuStack resources. Each enum type includes Kubebuilder validation markers
// to ensure only valid values are accepted by the Kubernetes API server.
package enums

// Phase represents the current execution phase of a resource.
// This enum is used consistently across all execution resources (Stories, StoryRuns, StepRuns)
// to provide a unified view of execution state.
//
// The phase progression typically follows:
// Pending → Running → {Succeeded|Failed|Canceled|Compensated|Paused|Blocked|Scheduling|Timeout|Aborted}
//
// Some resources may also support Paused for manual intervention scenarios.
// nolint:lll
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Canceled;Compensated;Paused;Blocked;Scheduling;Timeout;Aborted
type Phase string

const (
	// PhasePending indicates that the resource is created but not yet started.
	// This is the initial state for most execution resources.
	// Reasons for staying in Pending:
	// - Waiting for dependencies to become ready
	// - Waiting for resource quota/capacity
	// - Waiting for scheduling
	PhasePending Phase = "Pending"

	// PhaseRunning indicates that the resource is actively executing.
	// This means the controller has begun processing and execution is in progress.
	// For workflow resources, this means steps are being executed.
	PhaseRunning Phase = "Running"

	// PhaseSucceeded indicates that the resource completed successfully.
	// This is a terminal state - the resource will not transition to other phases.
	// All required operations completed without errors.
	PhaseSucceeded Phase = "Succeeded"

	// PhaseFailed indicates that the resource failed and cannot proceed.
	// This is a terminal state - the resource will not automatically recover.
	// Manual intervention or recreation may be required.
	PhaseFailed Phase = "Failed"

	// PhaseCanceled indicates that the resource execution was canceled.
	// This is a terminal state that can result from:
	// - User-initiated cancellation
	// - System-initiated cancellation (e.g., policy violations)
	// - Parent resource cancellation
	PhaseCanceled Phase = "Canceled"

	// PhaseCompensated indicates that the resource has been compensated using saga pattern.
	// This is a terminal state used in distributed transaction scenarios.
	// The resource was rolled back or compensated for due to failures in related operations.
	PhaseCompensated Phase = "Compensated"

	// PhasePaused indicates that the resource is paused waiting for external input.
	// This is typically used for manual approval gates or human-in-the-loop scenarios.
	// The resource can transition back to Running when conditions are met.
	PhasePaused Phase = "Paused"

	// PhaseBlocked indicates that the resource is blocked waiting for dependencies.
	// This is not a terminal state. The resource can transition back to Pending/Running
	// once its dependencies (e.g., an EngramTemplate) are available.
	PhaseBlocked Phase = "Blocked"

	// PhaseScheduling indicates that the resource is waiting to be scheduled on a node.
	PhaseScheduling Phase = "Scheduling"

	// PhaseTimeout indicates that the resource has timed out.
	PhaseTimeout Phase = "Timeout"

	// PhaseAborted indicates that the resource execution was aborted.
	PhaseAborted Phase = "Aborted"

	// PhaseSkipped indicates that a step was skipped due to a condition.
	PhaseSkipped Phase = "Skipped"
)

// IsTerminal returns true if the Phase is a terminal state.
func (p Phase) IsTerminal() bool {
	switch p {
	case PhaseSucceeded, PhaseFailed, PhaseCanceled, PhaseCompensated, PhaseTimeout, PhaseAborted, PhaseSkipped:
		return true
	default:
		return false
	}
}

// StopMode defines how a story or workflow should stop execution.
// This enum is used in stop primitives and control flow to specify the desired outcome.
//
// +kubebuilder:validation:Enum=success;failure;cancel
type StopMode string

const (
	// StopModeSuccess indicates successful completion.
	// The workflow should terminate with a Succeeded phase.
	StopModeSuccess StopMode = "success"

	// StopModeFailure indicates failure termination.
	// The workflow should terminate with a Failed phase.
	StopModeFailure StopMode = "failure"

	// StopModeCancel indicates cancellation.
	// The workflow should terminate with a Canceled phase.
	StopModeCancel StopMode = "cancel"
)

// StepType defines the type of step in a story workflow.
// BubuStack provides built-in primitives for common workflow patterns,
// reducing the need for custom code in many scenarios.
// nolint:lll
// +kubebuilder:validation:Enum=condition;loop;parallel;sleep;stop;switch;filter;transform;wait;throttle;batch;executeStory;setData;mergeData;gate
type StepType string

const (
	// StepTypeCondition represents if/then/else logic.
	// Enables conditional branching based on data or previous step results.
	// Example: Check if user input is valid before proceeding.
	StepTypeCondition StepType = "condition"

	// StepTypeLoop represents iteration logic.
	// Enables processing arrays of data or repeating operations.
	// Supports parallel execution and early termination.
	StepTypeLoop StepType = "loop"

	// StepTypeParallel represents parallel execution.
	// Enables running multiple operations concurrently with optional synchronization.
	// Supports different completion policies (all, any, majority).
	StepTypeParallel StepType = "parallel"

	// StepTypeSleep represents a delay/wait.
	// Enables adding delays for rate limiting, polling intervals, or workflow timing.
	StepTypeSleep StepType = "sleep"

	// StepTypeStop represents workflow termination.
	// Enables early termination with specified outcome (success, failure, cancel).
	StepTypeStop StepType = "stop"

	// StepTypeSwitch represents multi-way branching.
	// Enables complex branching logic similar to switch/case statements.
	StepTypeSwitch StepType = "switch"

	// StepTypeFilter represents array/object filtering.
	// Enables data filtering and transformation without custom code.
	StepTypeFilter StepType = "filter"

	// StepTypeTransform represents data transformation.
	// Enables data manipulation, format conversion, and structure changes.
	StepTypeTransform StepType = "transform"

	// StepTypeWait represents waiting for external conditions.
	// Enables waiting for external events, webhooks, or time-based conditions.
	StepTypeWait StepType = "wait"

	// StepTypeThrottle represents rate limiting.
	// Enables controlling execution rate and implementing backpressure.
	StepTypeThrottle StepType = "throttle"

	// StepTypeBatch represents grouping operations.
	// Enables batching multiple operations for efficiency.
	StepTypeBatch StepType = "batch"

	// StepTypeExecuteStory represents sub-workflow execution.
	// Enables composing larger workflows from smaller, reusable stories.
	StepTypeExecuteStory StepType = "executeStory"

	// StepTypeSetData represents setting variables.
	// Enables storing intermediate results and setting workflow variables.
	StepTypeSetData StepType = "setData"

	// StepTypeMergeData represents merging data from multiple sources.
	// Enables combining results from parallel operations or multiple inputs.
	StepTypeMergeData StepType = "mergeData"

	// StepTypeGate represents waiting for a manual approval.
	// Enables waiting for a manual approval before continuing the workflow.
	StepTypeGate StepType = "gate"
)

// WorkloadMode defines how a workload should be executed in Kubernetes.
// This determines the fundamental execution pattern and lifecycle management.
//
// +kubebuilder:validation:Enum=job;deployment;statefulset
type WorkloadMode string

const (
	// WorkloadModeJob runs as a one-shot Kubernetes Job.
	// Best for: Batch processing, one-time executions, step-by-step workflows.
	// Characteristics: Runs to completion, automatic cleanup, resource efficient.
	WorkloadModeJob WorkloadMode = "job"

	// WorkloadModeDeployment runs as an always-on stateless Deployment.
	// Best for: HTTP services, webhooks, always-on triggers, stateless processing.
	// Characteristics: High availability, horizontal scaling, rolling updates.
	WorkloadModeDeployment WorkloadMode = "deployment"

	// WorkloadModeStatefulSet runs as an always-on stateful StatefulSet.
	// Best for: Stateful services, persistent storage, ordered deployment.
	// Characteristics: Stable network identity, persistent storage, ordered scaling.
	WorkloadModeStatefulSet WorkloadMode = "statefulset"
)

// StreamingStrategy defines the deployment strategy for long-running engrams in a streaming story.
type StreamingStrategy string

const (
	// StreamingStrategyPerStory creates a single, shared set of long-running engrams for the Story.
	StreamingStrategyPerStory StreamingStrategy = "PerStory"
	// StreamingStrategyPerStoryRun creates a dedicated set of long-running engrams for each StoryRun.
	StreamingStrategyPerStoryRun StreamingStrategy = "PerStoryRun"
)

// BackoffStrategy defines retry backoff strategies for failed operations.
// These strategies control how delays between retry attempts are calculated.
//
// +kubebuilder:validation:Enum=exponential;linear;constant
type BackoffStrategy string

const (
	// BackoffStrategyExponential doubles delay each time.
	// Example: 1s → 2s → 4s → 8s → 16s
	// Best for: Most retry scenarios, prevents overwhelming downstream services.
	BackoffStrategyExponential BackoffStrategy = "exponential"

	// BackoffStrategyLinear increases delay by base amount each time.
	// Example: 1s → 2s → 3s → 4s → 5s
	// Best for: Scenarios where exponential growth is too aggressive.
	BackoffStrategyLinear BackoffStrategy = "linear"

	// BackoffStrategyConstant uses same delay each time.
	// Example: 1s → 1s → 1s → 1s → 1s
	// Best for: Testing, scenarios where consistent timing is important.
	BackoffStrategyConstant BackoffStrategy = "constant"
)

// UpdateStrategyType defines how workload updates should be performed.
// This controls the rollout behavior when updating deployments or statefulsets.
//
// +kubebuilder:validation:Enum=RollingUpdate;Recreate
type UpdateStrategyType string

const (
	// UpdateStrategyRollingUpdate gradually replaces old pods with new ones.
	// Provides zero-downtime updates with configurable surge and unavailability.
	// Best for: Production services that need high availability.
	UpdateStrategyRollingUpdate UpdateStrategyType = "RollingUpdate"

	// UpdateStrategyRecreate deletes all old pods before creating new ones.
	// Results in downtime but ensures clean state transitions.
	// Best for: Development, testing, or services that can't run multiple versions.
	UpdateStrategyRecreate UpdateStrategyType = "Recreate"
)

// ValidationStatus defines template validation states.
// Templates undergo validation to ensure they're properly configured and secure.
//
// +kubebuilder:validation:Enum=valid;invalid;unknown;pending
type ValidationStatus string

const (
	// ValidationStatusValid indicates template passed validation.
	// The template is safe to use and meets all requirements.
	ValidationStatusValid ValidationStatus = "valid"

	// ValidationStatusInvalid indicates template failed validation.
	// The template has issues that must be resolved before use.
	ValidationStatusInvalid ValidationStatus = "invalid"

	// ValidationStatusUnknown indicates validation status is unknown.
	// This may occur during system startup or after validation system changes.
	ValidationStatusUnknown ValidationStatus = "unknown"

	// ValidationStatusPending indicates validation is in progress.
	// The template is being processed by the validation system.
	ValidationStatusPending ValidationStatus = "pending"
)

// ExitClass defines how container exit codes should be interpreted.
// This enables intelligent retry logic based on the type of failure.
//
// +kubebuilder:validation:Enum=success;retry;terminal;rateLimited
type ExitClass string

const (
	// ExitClassSuccess indicates successful completion.
	// The operation completed successfully and should not be retried.
	ExitClassSuccess ExitClass = "success"

	// ExitClassRetry indicates the operation should be retried.
	// This represents transient failures that may succeed on retry.
	// Examples: Network timeouts, temporary resource unavailability.
	ExitClassRetry ExitClass = "retry"

	// ExitClassTerminal indicates permanent failure, don't retry.
	// This represents failures that will not improve with retries.
	// Examples: Invalid input, authentication failures, programming errors.
	ExitClassTerminal ExitClass = "terminal"

	// ExitClassRateLimited indicates rate limiting, retry with backoff.
	// This represents failures due to rate limiting that require longer delays.
	// Examples: API rate limits, quota exhaustion, throttling.
	ExitClassRateLimited ExitClass = "rateLimited"
)

// SecretMountType defines how secrets should be made available to the workload
// +kubebuilder:validation:Enum=env;file;both
type SecretMountType string

const (
	// Mount secret as environment variables (SECRETNAME_KEY format)
	SecretMountTypeEnv SecretMountType = "env"
	// Mount secret as files in a directory
	SecretMountTypeFile SecretMountType = "file"
	// Mount secret as both environment variables AND files
	SecretMountTypeBoth SecretMountType = "both"
)

// StoryPattern defines the execution pattern for a Story.
type StoryPattern string

const (
	// BatchPattern executes the story as a series of discrete, ephemeral jobs.
	// This is the default and is suitable for traditional, asynchronous workflows.
	BatchPattern StoryPattern = "batch"

	// StreamingPattern executes the story as a persistent pipeline of long-running services.
	// This is suitable for low-latency, real-time streaming workflows.
	StreamingPattern StoryPattern = "streaming"
)
