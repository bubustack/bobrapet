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
// +kubebuilder:resource:scope=Cluster,shortName=etpl
// +kubebuilder:printcolumn:name="Category",type=string,JSONPath=.spec.uiHints.category
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=.spec.version
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
type EngramTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EngramTemplateSpec   `json:"spec,omitempty"`
	Status EngramTemplateStatus `json:"status,omitempty"`
}

type EngramTemplateSpec struct {

	// Shared template fields
	TemplateSpec `json:",inline"`

	// Engram-specific schema fields
	// JSON Schema for inputs
	InputSchema *runtime.RawExtension `json:"inputSchema,omitempty"`

	// JSON Schema for outputs
	OutputSchema *runtime.RawExtension `json:"outputSchema,omitempty"`
}

// All shared types (UIHints, Example, ResourceHints, SecurityHints, TemplateStatus)
// are defined in template_types.go to ensure consistency between EngramTemplate and ImpulseTemplate

type EngramTemplateStatus struct {
	TemplateStatus `json:",inline"`
}

// +kubebuilder:object:root=true
type EngramTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EngramTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EngramTemplate{}, &EngramTemplateList{})
}
