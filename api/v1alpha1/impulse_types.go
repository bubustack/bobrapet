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
// +kubebuilder:resource:scope=Namespaced,shortName=imp
type Impulse struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImpulseSpec   `json:"spec,omitempty"`
	Status ImpulseStatus `json:"status,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="size(self.storyRef) > 0",message="storyRef is required"
// +kubebuilder:validation:XValidation:rule="!has(self.engine) || size(self.engine.templateRef) > 0",message="engine.templateRef is required when engine is set"
// +kubebuilder:validation:XValidation:rule="self.storyRef.matches('^([a-z0-9]([-a-z0-9]*[a-z0-9])?/)?[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="storyRef must be valid name or ns/name format"
// +kubebuilder:validation:XValidation:rule="size(self.storyRef) <= 253",message="storyRef must be <= 253 characters"
type ImpulseSpec struct {
	// Core impulse behavior - trigger a story; StoryRef may be "ns/name" for cross-namespace
	StoryRef string `json:"storyRef"`

	// Optional mapping from trigger context to story inputs
	Mapping *runtime.RawExtension `json:"contextMapping,omitempty"`

	// Execution engine configuration - how to run this impulse (optional; may default from template)
	Engine *EngineConfig `json:"engine,omitempty"`

	// Schema definitions (optional, for validation and tooling)
	ContextSchema *runtime.RawExtension `json:"contextSchema,omitempty"` // Schema for incoming trigger context
	MappingSchema *runtime.RawExtension `json:"mappingSchema,omitempty"` // Schema for context mapping
	ConfigSchema  *runtime.RawExtension `json:"configSchema,omitempty"`  // Schema for 'with' configuration

	// Configuration/parameters for this Impulse instance (validated by the template/schema)
	With *runtime.RawExtension `json:"with,omitempty"`

	// Retry and timeout policies
	Retry   *RetryPolicy `json:"retry,omitempty"`
	Timeout string       `json:"timeout,omitempty"`

	// Resource requirements
	Resources *WorkloadResources `json:"resources,omitempty"`

	// Security configuration
	Security *WorkloadSecurity `json:"security,omitempty"`
}

// All shared types (EngineConfig, RetryPolicy, WorkloadResources, WorkloadSecurity, etc.)
// are defined in workload_types.go to ensure consistency between Engrams and Impulses
type ImpulseStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type ImpulseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Impulse `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Impulse{}, &ImpulseList{})
}
