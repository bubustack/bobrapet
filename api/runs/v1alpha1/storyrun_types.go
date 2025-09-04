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
// +kubebuilder:resource:scope=Namespaced,shortName=srun
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=.status.phase
type StoryRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StoryRunSpec   `json:"spec,omitempty"`
	Status StoryRunStatus `json:"status,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="size(self.storyRef) > 0",message="storyRef is required"
// +kubebuilder:validation:XValidation:rule="self.storyRef.matches('^([a-z0-9]([-a-z0-9]*[a-z0-9])?/)?[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="storyRef must be valid name or ns/name format"
// +kubebuilder:validation:XValidation:rule="size(self.storyRef) <= 253",message="storyRef must be <= 253 characters"
type StoryRunSpec struct {
	StoryRef string                `json:"storyRef"`
	Inputs   *runtime.RawExtension `json:"inputs,omitempty"`
}

type StoryRunStatus struct {
	Phase     string   `json:"phase,omitempty"` // Pending|Running|Succeeded|Failed|Canceled
	Message   string   `json:"message,omitempty"`
	Completed []string `json:"completed,omitempty"`
	Active    []string `json:"active,omitempty"`

	// Execution timing
	StartedAt  *metav1.Time `json:"startedAt,omitempty"`
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`

	// Final outputs and artifacts
	Output    *runtime.RawExtension `json:"output,omitempty"`    // inline output if <256 KiB
	Artifacts []ArtifactRef         `json:"artifacts,omitempty"` // references to artifacts in store
	Error     *runtime.RawExtension `json:"error,omitempty"`     // structured error from execution

	// Status conditions
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
