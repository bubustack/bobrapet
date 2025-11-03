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

	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
)

// Story represents a workflow definition - a sequence of steps that accomplish a business goal
//
// Stories are the main workflows in your system. Think of them like:
// - GitHub Actions workflows
// - Jenkins pipelines
// - Zapier automations
// - Any business process you want to automate
//
// Stories orchestrate Engrams (the workers) to create powerful workflows:
// - CI/CD pipeline: checkout → test → build → deploy
// - Data processing: extract → transform → load → notify
// - E-commerce: validate order → charge payment → ship → update inventory
// - Content moderation: analyze → classify → approve/reject → notify
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=story,categories={bubu,ai,workflows}
// +kubebuilder:printcolumn:name="Steps",type=integer,JSONPath=".status.stepsTotal"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type Story struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StorySpec   `json:"spec,omitempty"`
	Status StoryStatus `json:"status,omitempty"`
}

// StorySpec defines what the workflow does and how it should run
// +kubebuilder:validation:XValidation:rule="self.steps.all(step, has(step.ref) != has(step.type))",message="each step must set exactly one of ref or type"
// +kubebuilder:validation:XValidation:rule="self.steps.all(step, self.steps.exists_one(other, other.name == step.name))",message="step names must be unique"
type StorySpec struct {
	// Pattern specifies the execution model for the Story.
	// "batch" stories are run to completion via a StoryRun.
	// "streaming" stories create long-running workloads that process data continuously.
	// +kubebuilder:validation:Enum=batch;streaming
	// +kubebuilder:default=batch
	Pattern enums.StoryPattern `json:"pattern,omitempty"`

	// StreamingStrategy defines the deployment strategy for long-running engrams in a streaming story.
	// "PerStory" creates a single, shared set of long-running engrams for the Story.
	// "PerStoryRun" creates a dedicated set of long-running engrams for each StoryRun.
	// This field is only applicable when `pattern` is "streaming".
	// +kubebuilder:validation:Enum=PerStory;PerStoryRun
	// +kubebuilder:default=PerStory
	// +optional
	StreamingStrategy enums.StreamingStrategy `json:"streamingStrategy,omitempty"`

	// InputsSchema defines the schema for the data required to start a StoryRun.
	// +optional
	InputsSchema *runtime.RawExtension `json:"inputsSchema,omitempty"`

	// OutputsSchema defines the schema for the data this Story is expected to produce.
	// +optional
	OutputsSchema *runtime.RawExtension `json:"outputsSchema,omitempty"`

	// Output defines a template for the Story's final output.
	// This CEL expression is evaluated upon successful completion of the story,
	// and the result is stored in the StoryRun's status.output field.
	// It has access to 'inputs' and 'steps' contexts.
	// e.g., {"final_message": "processed " + steps.load_data.outputs.count + " records"}
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Output *runtime.RawExtension `json:"output,omitempty"`

	// The actual workflow steps - this is where the magic happens!
	// Steps run in sequence unless you use parallel/condition logic
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=100
	Steps []Step `json:"steps"`

	// How should this Story behave? (timeouts, retries, storage, etc.)
	Policy *StoryPolicy `json:"policy,omitempty"`
}

// Step defines a single unit of work within a Story.
// Steps are the building blocks of stories, defining everything from custom logic
// to control flow like loops, conditions, and parallel execution.
type Step struct {
	// Name is a human-readable identifier for the step.
	// It's used to reference this step's outputs in other steps.
	// e.g., '{{ steps.my-step-name.outputs.some_field }}'
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Name string `json:"name"`

	// ID is an optional, unique identifier for a step instance.
	// While 'name' identifies the step in logs and outputs, 'id' can be used
	// programmatically to reference a specific node in the workflow graph.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	// +optional
	ID string `json:"id,omitempty"`

	// Needs explicitly defines a list of step names that must be completed before this step can run.
	// This is used to create an execution order for steps that don't have an implicit data dependency.
	// +optional
	Needs []string `json:"needs,omitempty"`

	// Type determines the kind of operation this step performs.
	// Use built-in types for common patterns. If omitted and 'ref' is present, the step is an 'engram' step.
	//
	// Built-in primitive types:
	// - condition: if/then/else logic
	// - loop: iterate over arrays or repeat N times
	// - parallel: run multiple steps simultaneously
	// - sleep: wait for a specified duration
	// - stop: halt story execution (success or failure)
	// - switch: multi-way branching like switch/case
	// - filter: filter arrays or objects based on conditions
	// - transform: modify data structure or format
	// - executeStory: run another story as a sub-story
	// - setData: set variables or update context
	// - mergeData: combine data from multiple sources
	// - wait: wait for external conditions
	// - throttle: rate limiting
	// - batch: group operations
	// - gate: wait for a manual approval
	// +optional
	Type enums.StepType `json:"type,omitempty"`

	// If provides a condition for executing this step.
	// The step is only executed if the CEL expression evaluates to true.
	// +optional
	If *string `json:"if,omitempty"`

	// Ref points to an Engram to execute.
	// This is a shortcut for a step of type 'engram'.
	// If 'ref' is used, 'type' should be omitted.
	// +optional
	Ref *refs.EngramReference `json:"ref,omitempty"`

	// With provides the configuration for the step. The expected structure of this block
	// is determined by the step 'type'. For example:
	// - for 'engram': the inputs to the engram.
	// - for 'executeStory': contains 'storyRef', 'waitForCompletion', and 'with'.
	// - for 'loop': contains 'items' and 'template'.
	// The structure is validated at runtime by the controller.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	With *runtime.RawExtension `json:"with,omitempty"`

	// Secrets maps template secret definitions to actual Kubernetes secrets for this step.
	// This is only applicable to 'engram' steps and overrides any secret mappings on the Engram itself.
	// Example: {"apiKey": "openai-credentials", "database": "postgres-creds"}
	// +optional
	Secrets map[string]string `json:"secrets,omitempty"`

	// Override execution settings for this specific step
	// Use sparingly - most configuration should be at the story or engram level
	Execution *ExecutionOverrides `json:"execution,omitempty"`
}

// Simplified StoryPolicy focused on essential workflow controls
type StoryPolicy struct {
	// Timeouts
	Timeouts *StoryTimeouts `json:"timeouts,omitempty"`

	// With provides the inputs for the sub-story, mirroring the 'with' field in an Engram step.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	With *runtime.RawExtension `json:"with,omitempty"`
	// Retry behavior
	Retries *StoryRetries `json:"retries,omitempty"`

	// Concurrency limits
	Concurrency *int32 `json:"concurrency,omitempty"`

	// Storage configuration
	Storage *StoragePolicy `json:"storage,omitempty"`

	// Default execution settings (can be overridden at step level)
	Execution *ExecutionPolicy `json:"execution,omitempty"`
}

type StoryTimeouts struct {
	// Total time for the entire story
	Story *string `json:"story,omitempty"`

	// Default timeout for individual steps
	Step *string `json:"step,omitempty"`
}

type StoryRetries struct {
	// Default retry policy for steps
	StepRetryPolicy *RetryPolicy `json:"stepRetryPolicy,omitempty"`

	// Whether to continue story on step failure
	ContinueOnStepFailure *bool `json:"continueOnStepFailure,omitempty"`
}

// StoryStatus defines the observed state of a Story.
type StoryStatus struct {
	// ObservedGeneration is the most recent generation observed for this Story.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Standard Kubernetes conditions for detailed status tracking (e.g., "Ready", "Validated")
	// This will indicate if the Story's syntax is valid and all referenced Engrams exist.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// StepsTotal reflects the total number of steps defined in the story's specification.
	// This provides a quick way to understand the complexity of the story.
	// +optional
	StepsTotal int32 `json:"stepsTotal,omitempty"`
}

// +kubebuilder:object:root=true
type StoryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Story `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Story{}, &StoryList{})
}
