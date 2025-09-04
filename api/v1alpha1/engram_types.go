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
// +kubebuilder:resource:scope=Namespaced,shortName=engram
type Engram struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EngramSpec   `json:"spec,omitempty"`
	Status EngramStatus `json:"status,omitempty"`
}

type EngramSpec struct {
	// Execution engine configuration - how to run this engram (optional; may default from template)
	// +kubebuilder:validation:XValidation:rule="has(self.templateRef) && size(self.templateRef) > 0",message="engine.templateRef is required"
	// +kubebuilder:validation:XValidation:rule="has(self.templateRef) ? size(self.templateRef) <= 253 : true",message="templateRef must be valid DNS name (max 253 chars)"
	// +kubebuilder:validation:XValidation:rule="has(self.templateRef) ? self.templateRef.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$') : true",message="templateRef must be valid DNS-1123 label"
	Engine *EngineConfig `json:"engine,omitempty"`

	// ABI schema definitions (optional, for validation and tooling)
	InputSchema  *runtime.RawExtension `json:"inputSchema,omitempty"`
	OutputSchema *runtime.RawExtension `json:"outputSchema,omitempty"`
	ConfigSchema *runtime.RawExtension `json:"configSchema,omitempty"`

	// Generic configuration / parameters for this Engram instance (validated by template/schema)
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

type EngramStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type EngramList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Engram `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Engram{}, &EngramList{})
}
