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
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ReferenceGrantSpec defines the desired state of ReferenceGrant.
// The grant is namespaced and allows references *to* objects in the grant's
// namespace from objects in other namespaces.
type ReferenceGrantSpec struct {
	// From lists which resources are allowed to reference into this namespace.
	// +kubebuilder:validation:MinItems=1
	From []ReferenceGrantFrom `json:"from"`

	// To lists the target resource kinds (and optionally names) that can be referenced.
	// +kubebuilder:validation:MinItems=1
	To []ReferenceGrantTo `json:"to"`
}

// ReferenceGrantFrom identifies the referencing resource.
type ReferenceGrantFrom struct {
	// Group is the API group of the referencing object (e.g., "bubustack.io").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Group string `json:"group"`

	// Kind is the Kubernetes kind of the referencing object (e.g., "StoryRun").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Kind string `json:"kind"`

	// Namespace is the namespace of the referencing object.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Namespace string `json:"namespace"`
}

// ReferenceGrantTo identifies the target resource that can be referenced.
type ReferenceGrantTo struct {
	// Group is the API group of the target object (e.g., "bubustack.io").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Group string `json:"group"`

	// Kind is the Kubernetes kind of the target object (e.g., "Engram").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Kind string `json:"kind"`

	// Name optionally restricts the grant to a specific resource name.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Name *string `json:"name,omitempty"`
}

// ReferenceGrantStatus defines the observed state of ReferenceGrant.
type ReferenceGrantStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the ReferenceGrant resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=refgrant,categories={bubu,ai}

// ReferenceGrant is the Schema for the referencegrants API
type ReferenceGrant struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ReferenceGrant
	// +required
	Spec ReferenceGrantSpec `json:"spec"`

	// status defines the observed state of ReferenceGrant
	// +optional
	Status ReferenceGrantStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ReferenceGrantList contains a list of ReferenceGrant
type ReferenceGrantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ReferenceGrant `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ReferenceGrant{}, &ReferenceGrantList{})
}
