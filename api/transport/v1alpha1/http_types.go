/*
Copyright 2025.

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
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// SchemaField defines a field in the request schema
type SchemaField struct {
	// Name of the field
	Name string `json:"name"`
	// Type of the field (text, image, audio, video, object, array)
	Type string `json:"type"`
	// Description of the field
	// +optional
	Description string `json:"description,omitempty"`
	// Format supported for media types (e.g., jpeg,png for images)
	// +optional
	Format string `json:"format,omitempty"`
	// Whether this field is required
	// +optional
	Required bool `json:"required,omitempty"`
}

// NodeDestination defines a destination node where requests should be forwarded
type NodeDestination struct {
	// Type of node (LLM, ImageProcessor, etc.)
	NodeType string `json:"nodeType"`
	// Name of the specific node
	NodeName string `json:"nodeName"`
	// Mapping of source fields to destination node input fields
	// +optional
	InputMapping map[string]string `json:"inputMapping,omitempty"`
	// Mapping of destination output fields to response fields
	// +optional
	OutputMapping map[string]string `json:"outputMapping,omitempty"`
}

// HTTPSpec defines the desired state of HTTP transport
type HTTPSpec struct {
	// Path where the HTTP endpoint will be exposed
	Path string `json:"path"`

	// Schema defines the expected request structure
	// +optional
	Schema []SchemaField `json:"schema,omitempty"`
	// NodeDestinations where requests should be forwarded
	Destinations []NodeDestination `json:"destinations"`
	// Image for the HTTP transport deployment
	// +optional
	Image string `json:"image,omitempty"`
	// Image pull policy for the deployment
	// +optional
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`
	// Number of replicas for the deployment
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
}

// HTTPStatus defines the observed state of HTTP
type HTTPStatus struct {
	// Conditions represent the latest available observations
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Endpoint where the HTTP transport is accessible
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Endpoint",type="string",JSONPath=".status.endpoint"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// HTTP is the Schema for the https API
type HTTP struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HTTPSpec   `json:"spec,omitempty"`
	Status HTTPStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HTTPList contains a list of HTTP
type HTTPList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HTTP `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HTTP{}, &HTTPList{})
}
