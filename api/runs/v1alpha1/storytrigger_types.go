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
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TriggerDeliveryIdentity captures the durable business identity for a trigger submission.
type TriggerDeliveryIdentity struct {
	// Mode records how the trigger identity was derived.
	// +optional
	Mode *bubuv1alpha1.TriggerDedupeMode `json:"mode,omitempty"`

	// Key is the stable business key used for token/key dedupe modes.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Key string `json:"key,omitempty"`

	// InputHash is the canonical sha256 of the resolved trigger inputs.
	// Required when Key is set.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	InputHash string `json:"inputHash,omitempty"`

	// SubmissionID identifies one logical trigger submission attempt chain.
	// Retries for the same submission must reuse this value.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	SubmissionID string `json:"submissionId"`
}

// +kubebuilder:validation:Enum=Pending;Created;Reused;Rejected
type StoryTriggerDecision string

const (
	StoryTriggerDecisionPending  StoryTriggerDecision = "Pending"
	StoryTriggerDecisionCreated  StoryTriggerDecision = "Created"
	StoryTriggerDecisionReused   StoryTriggerDecision = "Reused"
	StoryTriggerDecisionRejected StoryTriggerDecision = "Rejected"
)

// StoryTriggerSpec defines the desired state of StoryTrigger.
type StoryTriggerSpec struct {
	// StoryRef identifies the Story executed when the trigger is accepted.
	// +kubebuilder:validation:Required
	StoryRef refs.StoryReference `json:"storyRef"`

	// ImpulseRef identifies the Impulse that observed the external event.
	// +optional
	ImpulseRef *refs.ImpulseReference `json:"impulseRef,omitempty"`

	// Inputs is the resolved Story input payload for the requested run.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Inputs *runtime.RawExtension `json:"inputs,omitempty"`

	// DeliveryIdentity is the durable request identity used for dedupe and retry resolution.
	// +kubebuilder:validation:Required
	DeliveryIdentity TriggerDeliveryIdentity `json:"deliveryIdentity"`
}

// StoryTriggerStatus defines the observed state of StoryTrigger.
// +kubebuilder:validation:XValidation:message="status.conditions reason field must be <= 64 characters",rule="!has(self.conditions) || self.conditions.all(c, !has(c.reason) || size(c.reason) <= 64)"
// +kubebuilder:validation:XValidation:message="status.conditions message field must be <= 2048 characters",rule="!has(self.conditions) || self.conditions.all(c, !has(c.message) || size(c.message) <= 2048)"
type StoryTriggerStatus struct {
	// ObservedGeneration is the latest generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Decision is the controller-owned durable admission decision.
	// +optional
	Decision StoryTriggerDecision `json:"decision,omitempty"`

	// Reason is a stable machine-readable reason for the current decision.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message provides a human-readable description of the current decision.
	// +optional
	Message string `json:"message,omitempty"`

	// StoryRunRef points at the accepted or reused StoryRun once resolution completes.
	// +optional
	StoryRunRef *refs.StoryRunReference `json:"storyRunRef,omitempty"`

	// AcceptedAt records when the trigger was first accepted for controller processing.
	// +optional
	AcceptedAt *metav1.Time `json:"acceptedAt,omitempty"`

	// CompletedAt records when the controller finished resolving the trigger.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Conditions provide canonical Kubernetes lifecycle status for the trigger request.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=trig,categories={bubu,ai,runs}
// +kubebuilder:printcolumn:name="Story",type=string,JSONPath=.spec.storyRef.name
// +kubebuilder:printcolumn:name="Decision",type=string,JSONPath=.status.decision
// +kubebuilder:printcolumn:name="StoryRun",type=string,JSONPath=.status.storyRunRef.name
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp

// StoryTrigger is the Schema for the storytriggers API.
type StoryTrigger struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of StoryTrigger
	// +required
	Spec StoryTriggerSpec `json:"spec"`

	// status defines the observed state of StoryTrigger
	// +optional
	Status StoryTriggerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// StoryTriggerList contains a list of StoryTrigger.
type StoryTriggerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []StoryTrigger `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StoryTrigger{}, &StoryTriggerList{})
}
