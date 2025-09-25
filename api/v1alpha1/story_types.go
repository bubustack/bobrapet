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
type StorySpec struct {
	// Pattern defines the execution strategy for this Story.
	// 'batch' (default): Runs the story as a series of ephemeral jobs. Good for asynchronous tasks.
	// 'streaming': Deploys the story as a persistent pipeline of services for real-time, low-latency workloads.
	// +kubebuilder:validation:Enum=batch;streaming
	// +kubebuilder:default=batch
	Pattern enums.StoryPattern `json:"pattern,omitempty"`

	// What inputs does this Story expect?
	// Define the data structure that triggers (Impulses) or users need to provide
	// Example: {"repository": "string", "branch": "string", "commit": "string"}
	// +kubebuilder:pruning:PreserveUnknownFields
	InputsSchema *runtime.RawExtension `json:"inputsSchema,omitempty"`

	// What outputs does this Story produce?
	// Define the data structure this workflow will return upon completion
	// Example: {"buildArtifact": "string", "testResults": "object", "deploymentUrl": "string"}
	// +kubebuilder:pruning:PreserveUnknownFields
	OutputSchema *runtime.RawExtension `json:"outputSchema,omitempty"`

	// The actual workflow steps - this is where the magic happens!
	// Steps run in sequence unless you use parallel/condition logic
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=100
	Steps []Step `json:"steps"`

	// How should this Story behave? (timeouts, retries, storage, etc.)
	Policy *StoryPolicy `json:"policy,omitempty"`
}

// Step represents a single action in a story
// Steps are the building blocks of stories - each step does one specific thing
type Step struct {
	// Human-readable name for this step
	// Examples: "fetch-data", "process-payment", "send-notification"
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Name string `json:"name"`

	// Optional unique identifier for referencing this step's outputs
	// Useful for complex stories where later steps need data from specific earlier steps
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	ID string `json:"id,omitempty"`

	// What type of action should this step perform?
	// Use built-in types for common patterns, or reference an Engram for custom logic
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
	// - wait: wait for external conditions or events
	// - throttle: rate limiting and flow control
	// - batch: group operations for efficiency
	// - executeStory: run another story as a sub-story
	// - setData: set variables or update context
	// - mergeData: combine data from multiple sources
	Type *enums.StepType `json:"type,omitempty"`

	// Reference to an Engram for custom logic (alternative to Type)
	// Use this when you need functionality that's not covered by built-in types
	Ref *refs.EngramReference `json:"ref,omitempty"`

	// When should this step run? (CEL expression)
	// Examples: "inputs.environment == 'production'", "steps.validate.outputs.success == true"
	If *string `json:"if,omitempty"`

	// How to configure this step (works for both built-in types and Engrams)
	// The structure depends on what Type or Ref you're using
	// +kubebuilder:pruning:PreserveUnknownFields
	With *runtime.RawExtension `json:"with,omitempty"`

	// Maps template secret definitions to actual Kubernetes secrets for this step
	// Example: {"apiKey": "openai-credentials", "database": "postgres-creds"}
	Secrets map[string]string `json:"secrets,omitempty"`

	// Launch another story as a sub-story (using same 'with' pattern for arguments)
	// Useful for breaking complex stories into smaller, reusable pieces
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=^([a-z0-9]([-a-z0-9]*[a-z0-9])?/)?[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	StoryRef *string `json:"storyRef,omitempty"`

	// Override execution settings for this specific step
	// Use sparingly - most configuration should be at the story or engram level
	Execution *ExecutionOverrides `json:"execution,omitempty"`
}

// Simplified StoryPolicy focused on essential workflow controls
type StoryPolicy struct {
	// Timeouts
	Timeouts *StoryTimeouts `json:"timeouts,omitempty"`

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

type StoryStatus struct {
	// observedGeneration is the most recent generation observed for this Story. It corresponds to the
	// Story's generation, which is updated on mutation by the API Server.
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
