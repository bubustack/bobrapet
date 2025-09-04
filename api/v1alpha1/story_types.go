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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

//
// Story
//

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=story
// +kubebuilder:printcolumn:name="Steps",type=integer,JSONPath=.spec.stepsCount
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
type Story struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StorySpec   `json:"spec,omitempty"`
	Status StoryStatus `json:"status,omitempty"`
}

// Enforce policy.concurrency at the spec level to avoid cross-struct references
// +kubebuilder:validation:XValidation:rule="!has(self.policy) || !has(self.policy.concurrency) || (self.policy.concurrency >= 1 && self.policy.concurrency <= 1000)",message="policy.concurrency must be 1-1000"
// +kubebuilder:validation:XValidation:rule="!has(self.policy) || !has(self.policy.timeouts) || !has(self.policy.timeouts.story) || self.policy.timeouts.story.matches('^[0-9]+(\\\\.[0-9]+)?(ns|us|µs|ms|s|m|h)$')",message="policy.timeouts.story must be valid duration"
// +kubebuilder:validation:XValidation:rule="!has(self.policy) || !has(self.policy.timeouts) || !has(self.policy.timeouts.step) || self.policy.timeouts.step.matches('^[0-9]+(\\\\.[0-9]+)?(ns|us|µs|ms|s|m|h)$')",message="policy.timeouts.step must be valid duration"
type StorySpec struct {
	// Optional JSON schema strings for inputs/outputs (opaque for MVP)
	InputsSchema *string `json:"inputsSchema,omitempty"`
	OutputSchema *string `json:"outputSchema,omitempty"`

	// Minimal step list: either a built-in Type or a Ref (image)
	Steps []Step `json:"steps"`

	// Simple policy placeholder (MVP)
	Policy *StoryPolicy `json:"policy,omitempty"`
}

// StoryPolicy captures workflow-wide behavior knobs for a Story
type StoryPolicy struct {
	Timeouts     *Timeouts     `json:"timeouts,omitempty"`
	Retries      *Retries      `json:"retries,omitempty"`
	ShortCircuit *ShortCircuit `json:"shortCircuit,omitempty"`

	// Story-level concurrency cap (max in-flight steps)
	Concurrency *int32 `json:"concurrency,omitempty"`
	// Dead-letter handling target (e.g., step name or sink ref)
	DLQ *string `json:"dlq,omitempty"`
	// Enable effectively-once semantics when possible
	Idempotency *bool `json:"idempotency,omitempty"`
	// Artifact store policy and TTL
	Artifacts *ArtifactPolicy `json:"artifacts,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="(has(self.type) && size(self.type) > 0) != (has(self.ref) && size(self.ref) > 0)",message="Exactly one of type or ref must be set"
type Step struct {
	Name string `json:"name"`
	ID   string `json:"id,omitempty"` // Step ID for GitHub Actions-style references

	// Exactly one of Type or Ref must be set
	// +kubebuilder:validation:Enum=condition;loop;parallel;sleep;stop;switch;filter;transform;wait;throttle;batch;executeStory;gate;setData;mergeData
	Type *string `json:"type,omitempty"` // built-ins: condition|loop|parallel|sleep|stop|switch|filter|transform|wait|throttle|batch|executeStory|gate|setData|mergeData
	Ref  *string `json:"ref,omitempty"`  // external Engram reference

	// Control flow with nested structures (natural workflow definition)
	If       *string               `json:"if,omitempty"`       // CEL expression for condition
	Then     *apiextensionsv1.JSON `json:"then,omitempty"`     // steps to execute if condition is true (flexible schema)
	Else     *apiextensionsv1.JSON `json:"else,omitempty"`     // steps to execute if condition is false (flexible schema)
	Items    *string               `json:"items,omitempty"`    // CEL expression for loop items
	Body     *apiextensionsv1.JSON `json:"body,omitempty"`     // steps to execute in loop body (flexible schema)
	Branches *apiextensionsv1.JSON `json:"branches,omitempty"` // parallel branches (flexible schema)

	// Gate specific
	Gate *GateConfig `json:"gate,omitempty"`

	// Policies
	BranchPolicy *BranchPolicy `json:"branchPolicy,omitempty"` // for parallel execution

	// Pause/Resume control
	PausePolicy *PausePolicy `json:"pausePolicy,omitempty"` // when to pause execution

	// Stop specific
	Mode   *string               `json:"mode,omitempty"`   // return|fail|cancel|break|continue
	Output *runtime.RawExtension `json:"output,omitempty"` // output for stop

	// Switch specific (multi-way branching)
	Cases   []SwitchCase          `json:"cases,omitempty"`   // conditions and their associated steps
	Default *apiextensionsv1.JSON `json:"default,omitempty"` // default case for switch (flexible schema)

	// Filter/Transform specific
	Filter    *string `json:"filter,omitempty"`    // CEL expression for filtering
	Transform *string `json:"transform,omitempty"` // CEL expression for transformation

	// Wait specific
	Event     *string `json:"event,omitempty"`     // event type to wait for
	Duration  *string `json:"duration,omitempty"`  // duration to wait
	Condition *string `json:"condition,omitempty"` // condition to wait for

	// Throttle specific
	Rate   *string `json:"rate,omitempty"`   // requests per time period
	Burst  *int    `json:"burst,omitempty"`  // burst capacity
	Window *string `json:"window,omitempty"` // time window

	// Batch specific
	Size         *int    `json:"size,omitempty"`         // batch size
	BatchTimeout *string `json:"batchTimeout,omitempty"` // batch timeout
	MaxWait      *string `json:"maxWait,omitempty"`      // max wait for batch

	// Sub-workflow execution
	StoryRef  *string               `json:"storyRef,omitempty"`  // reference to another story
	SubInputs *runtime.RawExtension `json:"subInputs,omitempty"` // inputs for sub-story

	// Generic step configuration
	With *runtime.RawExtension `json:"with,omitempty"`

	// Step-level policies (override Story defaults)
	Retry   *Retries `json:"retry,omitempty"`
	Timeout *string  `json:"timeout,omitempty"`

	// Continue on error (GitHub Actions inspiration)
	ContinueOnError bool `json:"continueOnError,omitempty"`

	// Dependencies (GitHub Actions needs)
	Needs []string `json:"needs,omitempty"` // step dependencies

	// Parallel execution hints
	MaxConcurrency *int32 `json:"maxConcurrency,omitempty"` // Max parallel instances of this step
}

// GateConfig defines metadata for manual approvals
type GateConfig struct {
	Approvers []string `json:"approvers,omitempty"`
	SLA       string   `json:"sla,omitempty"`
}

// SwitchCase represents a case in a switch statement
type SwitchCase struct {
	When  string                `json:"when"`  // CEL expression for this case
	Steps *apiextensionsv1.JSON `json:"steps"` // steps to execute if when condition is true (flexible schema)
}

// PausePolicy defines when and how a step should pause
type PausePolicy struct {
	// +kubebuilder:validation:Enum=onFailure;onCondition;manual
	// +kubebuilder:default="onFailure"
	Trigger string `json:"trigger,omitempty"` // onFailure|onCondition|manual

	// CEL expression for conditional pausing (when trigger=onCondition)
	Condition *string `json:"condition,omitempty"`

	// How to resume: manual, timeout, or external trigger
	// +kubebuilder:validation:Enum=manual;timeout;external
	// +kubebuilder:default="manual"
	ResumeMode string `json:"resumeMode,omitempty"`

	// Timeout for automatic resume (when resumeMode=timeout)
	ResumeTimeout *string `json:"resumeTimeout,omitempty"`

	// External trigger configuration (when resumeMode=external)
	ExternalTrigger *ExternalTrigger `json:"externalTrigger,omitempty"`
}

// ExternalTrigger defines external resume triggers
type ExternalTrigger struct {
	// Reference to an Engram that handles the external trigger
	EngramRef string `json:"engramRef"`

	// Configuration passed to the external trigger engram
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// InputSchema defines the schema for story inputs with type support
type InputSchema struct {
	// Schema properties for different input types
	Properties map[string]InputProperty `json:"properties,omitempty"`

	// Required input fields
	Required []string `json:"required,omitempty"`
}

// InputProperty defines a single input property with type information
type InputProperty struct {
	// Data type: text, voice, image, file, binary, json, number, boolean
	Type string `json:"type"`

	// Human-readable description
	Description string `json:"description,omitempty"`

	// Whether this input is required
	Required bool `json:"required,omitempty"`

	// MIME types accepted (for file/voice/image inputs)
	AcceptedTypes []string `json:"acceptedTypes,omitempty"`

	// Maximum size for file inputs (in bytes)
	MaxSize int64 `json:"maxSize,omitempty"`

	// Processing hints for the input
	Processing *InputProcessing `json:"processing,omitempty"`
}

// InputProcessing defines how inputs should be processed
type InputProcessing struct {
	// For voice inputs: transcription settings
	Transcription *TranscriptionConfig `json:"transcription,omitempty"`

	// For image inputs: vision processing settings
	Vision *VisionConfig `json:"vision,omitempty"`

	// For file inputs: parsing settings
	FileProcessing *FileProcessingConfig `json:"fileProcessing,omitempty"`
}

// TranscriptionConfig for voice input processing
type TranscriptionConfig struct {
	// Provider: openai, deepgram, assemblyai
	Provider string `json:"provider,omitempty"`

	// Language hint
	Language string `json:"language,omitempty"`

	// Include timestamps
	Timestamps bool `json:"timestamps,omitempty"`
}

// VisionConfig for image input processing
type VisionConfig struct {
	// Provider: openai, anthropic, google
	Provider string `json:"provider,omitempty"`

	// Analysis type: describe, ocr, analyze, custom
	AnalysisType string `json:"analysisType,omitempty"`

	// Custom prompt for vision analysis
	CustomPrompt string `json:"customPrompt,omitempty"`
}

// FileProcessingConfig for file input processing
type FileProcessingConfig struct {
	// Auto-detect file type and parse accordingly
	AutoParse bool `json:"autoParse,omitempty"`

	// Specific parser: pdf, docx, csv, excel, json, xml
	Parser string `json:"parser,omitempty"`

	// Extract text content
	ExtractText bool `json:"extractText,omitempty"`
}

type StoryStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Derived count for UX (moved from spec)
	StepsCount int `json:"stepsCount,omitempty"`

	// Compilation results
	CompiledAt    *metav1.Time `json:"compiledAt,omitempty"`
	CompiledSteps int          `json:"compiledSteps,omitempty"`
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

// BranchPolicy defines behavior for parallel branches
type BranchPolicy struct {
	// +kubebuilder:default={"return","fail"}
	On []string `json:"on,omitempty"` // Which outcomes trigger the action
	// +kubebuilder:default="cancelSiblings"
	Action string `json:"action,omitempty"` // cancelSiblings|waitAll|ignore
}

// ShortCircuit controls early termination behavior
type ShortCircuit struct {
	// +kubebuilder:default={"return","fail"}
	On []string `json:"on,omitempty"`
	// +kubebuilder:default="cancelInFlight"
	Action string `json:"action,omitempty"` // cancelInFlight|waitAll|ignore
}

// ArtifactPolicy configures artifact persistence behavior
type ArtifactPolicy struct {
	// Store name or class (e.g., s3, minio, gcs)
	Store string `json:"store,omitempty"`
	// Time-to-live for artifacts (e.g., "24h")
	TTL string `json:"ttl,omitempty"`
	// Max inline size for outputs kept in status (e.g., "256Ki")
	MaxInlineSize string `json:"maxInlineSize,omitempty"`
}

type Timeouts struct {
	// +kubebuilder:default="30m"
	Story string `json:"story,omitempty"`
	// +kubebuilder:default="5m"
	Step string `json:"step,omitempty"`
}

type Retries struct {
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MaxRetries int `json:"maxRetries,omitempty"`
	// +kubebuilder:default="exponential"
	// +kubebuilder:validation:Enum=exponential;linear;fixed
	Backoff string `json:"backoff,omitempty"`
	// +kubebuilder:default="2s"
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h)$`
	BaseDelay string `json:"baseDelay,omitempty"`
	// +kubebuilder:default="1m"
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h)$`
	MaxDelay string `json:"maxDelay,omitempty"`
}
