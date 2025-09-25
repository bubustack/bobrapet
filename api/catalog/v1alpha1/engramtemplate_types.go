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

// EngramTemplate defines a reusable "worker" component for stories
//
// Think of EngramTemplates as specialized tools in a toolbox:
// - "http-client": Makes HTTP requests with retry and timeout support
// - "openai-chat": Integrates with OpenAI's GPT models
// - "postgres-query": Executes SQL queries against PostgreSQL
// - "slack-notify": Sends messages to Slack channels
// - "image-resize": Processes and resizes images
//
// Templates are cluster-scoped because they're meant to be shared across teams and namespaces.
// They define WHAT can be done, while Engrams define HOW to configure and run them.
//
// The relationship is:
// EngramTemplate (defines capabilities) → Engram (configured instance) → Used in Story steps
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=etpl,categories={bubu,ai,catalog}
// +kubebuilder:printcolumn:name="Description",type=string,JSONPath=.spec.description
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=.spec.version
// +kubebuilder:printcolumn:name="Usage",type=integer,JSONPath=.status.usageCount
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=.status.validationStatus
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
type EngramTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EngramTemplateSpec   `json:"spec,omitempty"`
	Status EngramTemplateStatus `json:"status,omitempty"`
}

// EngramTemplateSpec defines the capabilities and contract of a worker component
type EngramTemplateSpec struct {
	// Common template fields (version, description, image, etc.)
	TemplateSpec `json:",inline"`

	// What data does this engram expect as input?
	// This defines the contract for the data flowing INTO this component
	// Examples:
	// - HTTP client: {"url": "string", "method": "string", "headers": "object"}
	// - OpenAI: {"prompt": "string", "model": "string", "temperature": "number"}
	// - Database: {"query": "string", "parameters": "object"}
	// +kubebuilder:pruning:PreserveUnknownFields
	InputSchema *runtime.RawExtension `json:"inputSchema,omitempty"`

	// What data does this engram produce as output?
	// This defines the contract for the data flowing OUT of this component
	// Examples:
	// - HTTP client: {"status": "integer", "body": "string", "headers": "object"}
	// - OpenAI: {"response": "string", "usage": "object", "model": "string"}
	// - Database: {"rows": "array", "rowCount": "integer", "duration": "number"}
	// +kubebuilder:pruning:PreserveUnknownFields
	OutputSchema *runtime.RawExtension `json:"outputSchema,omitempty"`
}

// EngramTemplateStatus defines the observed state of EngramTemplate
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
