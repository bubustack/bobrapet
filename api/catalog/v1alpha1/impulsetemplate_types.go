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
// +kubebuilder:resource:scope=Cluster,shortName=itpl
// +kubebuilder:printcolumn:name="Category",type=string,JSONPath=.spec.uiHints.category
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=.spec.version
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
type ImpulseTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImpulseTemplateSpec   `json:"spec,omitempty"`
	Status ImpulseTemplateStatus `json:"status,omitempty"`
}

type ImpulseTemplateSpec struct {
	// Shared template fields
	TemplateSpec `json:",inline"`

	// Impulse-specific schema fields
	// JSON Schema for trigger context/payload (optional, for validation and tooling)
	ContextSchema *runtime.RawExtension `json:"contextSchema,omitempty"`

	// JSON Schema for context mapping configuration (optional, for validation and tooling)
	MappingSchema *runtime.RawExtension `json:"mappingSchema,omitempty"`
}

// All shared types (UIHints, Example, ResourceHints, SecurityHints, TemplateStatus)
// are defined in template_types.go to ensure consistency between EngramTemplate and ImpulseTemplate

type ImpulseTemplateStatus struct {
	TemplateStatus `json:",inline"`
}

// +kubebuilder:object:root=true
type ImpulseTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImpulseTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ImpulseTemplate{}, &ImpulseTemplateList{})
}
