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

// StoryRun represents an actual execution instance of a Story
//
// Think of the relationship like this:
// - Story = A movie script (defines what should happen)
// - StoryRun = Actually filming the movie (executing the script with specific actors and locations)
//
// Every time a Story is triggered (by an Impulse, manual trigger, or another Story),
// a new StoryRun is created to track that specific execution.
//
// StoryRuns are the "run history" of your automation - they show:
// - When each story execution started and finished
// - What inputs were provided
// - Which steps succeeded or failed
// - What outputs were produced
// - Any errors that occurred
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=srun,categories={bubu,ai,runs}
// +kubebuilder:printcolumn:name="Story",type=string,JSONPath=.spec.storyRef.name
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=.status.phase
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=.status.startedAt
// +kubebuilder:printcolumn:name="Duration",type=string,JSONPath=.status.duration
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
type StoryRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StoryRunSpec   `json:"spec,omitempty"`
	Status StoryRunStatus `json:"status,omitempty"`
}

// StoryRunSpec defines what Story to run and with what inputs
type StoryRunSpec struct {
	// Which Story to execute
	// References can be same-namespace (name only) or cross-namespace with explicit namespace
	// Examples: "ci-pipeline", cross-ns: {"name": "deploy-app", "namespace": "production"}
	// +kubebuilder:validation:Required
	StoryRef refs.StoryReference `json:"storyRef"`

	// Which Impulse triggered this StoryRun, if any
	// This provides a clear audit trail from an event trigger to its corresponding workflow execution
	// +optional
	ImpulseRef *refs.ImpulseReference `json:"impulseRef,omitempty"`

	// Input data for this specific story execution
	// This data gets validated against the Story's inputsSchema
	// Examples:
	// - CI pipeline: {"repository": "my-app", "branch": "main", "commit": "abc123"}
	// - Data processing: {"filename": "data.csv", "format": "csv", "destination": "warehouse"}
	// - Deployment: {"environment": "production", "version": "v1.2.3", "rollback": false}
	// +kubebuilder:pruning:PreserveUnknownFields
	Inputs *runtime.RawExtension `json:"inputs,omitempty"`
}

// StoryRunStatus tracks the current state and results of this story execution
type StoryRunStatus struct {
	// observedGeneration is the most recent generation observed for this StoryRun. It corresponds to the
	// StoryRun's generation, which is updated on mutation by the API Server.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Current execution phase
	// - Pending: StoryRun created but not yet started
	// - Running: Story is actively executing steps
	// - Succeeded: All steps completed successfully
	// - Failed: A step failed and the story cannot continue
	// - Canceled: Execution was canceled by user or system
	Phase enums.Phase `json:"phase,omitempty"`

	// Human-readable message about the story's status
	Message string `json:"message,omitempty"`

	// List of steps that are currently executing
	Active []string `json:"active,omitempty"`

	// List of steps that have completed successfully
	Completed []string `json:"completed,omitempty"`

	// Tracks the child StepRuns created by primitive steps (e.g., parallel branches, loop iterations)
	// Key: Name of the parent primitive step (e.g., "my-parallel-step")
	// Value: List of child StepRun names created by that primitive
	// +optional
	PrimitiveChildren map[string][]string `json:"primitiveChildren,omitempty"`

	// Execution timing
	StartedAt  *metav1.Time `json:"startedAt,omitempty"`
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`

	// How long did this execution take? (calculated field)
	Duration string `json:"duration,omitempty"`

	// What data did this story produce upon completion?
	// For small outputs (< 1MB), stored inline here
	// For large outputs, automatically stored in shared storage (if enabled) and path referenced here
	// This gets validated against the Story's outputSchema
	// Examples:
	// - CI pipeline: {"buildArtifact": "app-v1.2.3.tar.gz", "testResults": {...}, "deploymentUrl": "https://..."}
	// - Data processing: {"processedRows": 1000, "outputFile": "processed-data.json", "errors": 5}
	// - Large output: {"result": "success", "outputPath": "/shared/storage/story-123/final-output.json"}
	// +kubebuilder:pruning:PreserveUnknownFields
	Output *runtime.RawExtension `json:"output,omitempty"`

	// StepStates provides a detailed, real-time status for each step in the Story.
	// The key is the step name.
	// +optional
	StepStates map[string]StepState `json:"stepStates,omitempty"`

	// If the story failed, what was the error?
	// Contains structured error information for debugging
	// Examples:
	// - Step failure: {"failedStep": "deploy", "reason": "timeout", "details": "..."}
	// - Validation error: {"type": "input_validation", "field": "url", "message": "Invalid URL"}
	// +kubebuilder:pruning:PreserveUnknownFields
	Error *runtime.RawExtension `json:"error,omitempty"`

	// Standard Kubernetes conditions for detailed status tracking
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Step execution summary for quick overview
	StepsTotal    int32 `json:"stepsTotal,omitempty"`    // Total number of steps in the story
	StepsComplete int32 `json:"stepsComplete,omitempty"` // Number of steps that completed successfully
	StepsFailed   int32 `json:"stepsFailed,omitempty"`   // Number of steps that failed
	StepsSkipped  int32 `json:"stepsSkipped,omitempty"`  // Number of steps that were skipped
}

// StepState holds the detailed status of a single step within a StoryRun.
type StepState struct {
	// Phase is the current execution phase of the step.
	Phase enums.Phase `json:"phase"`
	// Message provides a human-readable summary of the step's status.
	// +optional
	Message string `json:"message,omitempty"`
	// For 'executeStory' steps, this tracks the name of the created sub-StoryRun.
	// +optional
	SubStoryRunName string `json:"subStoryRunName,omitempty"`
}

// +kubebuilder:object:root=true
type StoryRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoryRun `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StoryRun{}, &StoryRunList{})
}
