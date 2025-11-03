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

// Story defines a workflow that coordinates Engrams and primitive steps.
// It captures orchestration logic, dependency ordering, and configuration that is
// shared across every StoryRun derived from the Story.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=story,categories={bubu,ai,workflows}
// +kubebuilder:printcolumn:name="Steps",type=integer,JSONPath=".status.stepsTotal"
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=".status.validationStatus"
// +kubebuilder:printcolumn:name="Usage",type=integer,JSONPath=".status.usageCount"
// +kubebuilder:printcolumn:name="Triggers",type=integer,JSONPath=".status.triggers"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type Story struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StorySpec   `json:"spec,omitempty"`
	Status StoryStatus `json:"status,omitempty"`
}

// StorySpec captures the desired workflow topology and policy.
// +kubebuilder:validation:XValidation:rule="self.steps.all(step, has(step.ref) != has(step.type))",message="each step must set exactly one of ref or type"
// +kubebuilder:validation:XValidation:rule="self.steps.all(step, self.steps.exists_one(other, other.name == step.name))",message="step names must be unique"
type StorySpec struct {
	// Pattern selects the execution model. Batch stories produce short-lived StoryRuns;
	// streaming stories maintain long-lived streaming topologies.
	// +kubebuilder:validation:Enum=batch;streaming
	// +kubebuilder:default=batch
	Pattern enums.StoryPattern `json:"pattern,omitempty"`

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

	// Steps enumerates the workflow graph. Control-flow primitives and Engram
	// references are represented here.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=100
	Steps []Step `json:"steps"`

	// Policy provides optional story-wide defaults such as timeouts and storage.
	Policy *StoryPolicy `json:"policy,omitempty"`

	// Transports declares named media/stream transports that steps can publish to
	// or subscribe from. Controllers surface these to StoryRuns so engrams know
	// whether to keep payloads on the transport ("hot") or fall back to storage.
	// +optional
	Transports []StoryTransport `json:"transports,omitempty"`
}

// Step defines a node in the workflow graph. Nodes may refer to Engrams or to
// built-in primitives such as condition, loop, parallel, or executeStory.
type Step struct {
	// Name uniquely identifies the step within the Story. Outputs and dependencies
	// are referenced by this name.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Name string `json:"name"`

	// ID optionally provides a stable identifier distinct from Name. It is useful
	// for programmatic processing of the workflow graph.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	// +optional
	ID string `json:"id,omitempty"`

	// Needs lists predecessor step names that must complete before this step starts.
	// +optional
	Needs []string `json:"needs,omitempty"`

	// Type selects a built-in primitive. When omitted and Ref is set, the step
	// executes an Engram.
	// +optional
	Type enums.StepType `json:"type,omitempty"`

	// If gates execution on a CEL expression that evaluates to true.
	// +optional
	If *string `json:"if,omitempty"`

	// Ref points to the Engram executed by this step.
	// +optional
	Ref *refs.EngramReference `json:"ref,omitempty"`

	// With carries step configuration and parameters for the Engram.
	// Evaluation timing depends on execution mode:
	//
	// Batch/Job Mode:
	//   - Evaluated ONCE when the job starts
	//   - Has access to story inputs (trigger.*) and outputs from completed predecessor steps (steps.*)
	//   - Passed as BUBU_STEP_CONFIG environment variable
	//
	// Realtime/Streaming Mode:
	//   - Evaluated ONCE at deployment creation (static configuration)
	//   - Has access to story inputs (trigger.*) only
	//   - Step outputs are NOT available (they're per-packet, use 'runtime' field instead)
	//   - Passed as BUBU_STEP_CONFIG environment variable
	//
	// Examples:
	//   Batch: with: {inputFile: "{{ steps.fetch.outputs.filename }}", config: "{{ trigger.settings }}"}
	//   Realtime: with: {model: "gpt-4o-mini", systemPrompt: "{{ trigger.prompt }}"}
	//
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	With *runtime.RawExtension `json:"with,omitempty"`

	// Runtime carries dynamic per-packet configuration for realtime/streaming steps.
	// This field is ONLY used in realtime mode and is evaluated by the hub for each packet.
	//
	// CEL expressions can reference:
	//   - upstream.*: Outputs from predecessor steps (per-packet data)
	//   - trigger.*: Story trigger inputs (static)
	//   - packet.*: Current packet metadata
	//
	// The hub evaluates this configuration for each packet and passes the result
	// in StreamMessage.Inputs to the engram.
	//
	// Batch Mode: This field is ignored (use 'with' field instead)
	//
	// Examples:
	//   runtime: {userPrompt: "{{ upstream.transcribe.text }}", speakerId: "{{ packet.identity }}"}
	//
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Runtime *runtime.RawExtension `json:"runtime,omitempty"`

	// Transport selects a named transport declared in Story.spec.transports. Streaming
	// steps use this to bind to the correct connector.
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	// +optional
	Transport string `json:"transport,omitempty"`

	// Secrets overrides template secret bindings for Engram steps.
	// +optional
	Secrets map[string]string `json:"secrets,omitempty"`

	// Execution carries per-step execution overrides. Prefer story- or template-level
	// configuration when possible.
	Execution *ExecutionOverrides `json:"execution,omitempty"`
}

// StoryPolicy aggregates optional defaults applied across the Story.
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

	// Transports mirrors the declared transports with lightweight readiness info.
	// +optional
	Transports []StoryTransportStatus `json:"transports,omitempty"`

	// ValidationStatus indicates whether this Story's specification passed validation.
	ValidationStatus enums.ValidationStatus `json:"validationStatus,omitempty"`
	// ValidationErrors captures human-readable validation problems, when present.
	ValidationErrors []string `json:"validationErrors,omitempty"`

	// UsageCount reports how many Impulses reference this Story.
	// +optional
	UsageCount int32 `json:"usageCount"`

	// Triggers reports how many StoryRuns currently reference this Story.
	// This approximates how frequently the story has been executed.
	// +optional
	Triggers int64 `json:"triggers"`
}

// +kubebuilder:object:root=true
type StoryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Story `json:"items"`
}

// StoryTransport describes a named transport binding available to steps.
type StoryTransport struct {
	// Name references the transport from steps (e.g. "realtime-audio").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Name string `json:"name"`
	// TransportRef references the Transport CR providing this functionality.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	TransportRef string `json:"transportRef"`
	// Description provides optional human-readable context.
	// +optional
	Description string `json:"description,omitempty"`
	// Settings contain provider-specific overrides evaluated per StoryRun.
	// +optional
	Settings *runtime.RawExtension `json:"settings,omitempty"`
}

// StoryTransportStatus mirrors declared transports in status.
type StoryTransportStatus struct {
	Name         string              `json:"name"`
	TransportRef string              `json:"transportRef"`
	Mode         enums.TransportMode `json:"mode,omitempty"`
	// ModeReason explains why the controller selected the effective mode (e.g., "declarative-default", "requires-primitives").
	// +optional
	ModeReason string `json:"modeReason,omitempty"`
}

func init() {
	SchemeBuilder.Register(&Story{}, &StoryList{})
}
