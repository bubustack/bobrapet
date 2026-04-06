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
	"github.com/bubustack/bobrapet/pkg/refs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:validation:Enum=completed;released;abandoned
type EffectClaimCompletionStatus string

const (
	EffectClaimCompletionStatusCompleted EffectClaimCompletionStatus = "completed"
	EffectClaimCompletionStatusReleased  EffectClaimCompletionStatus = "released"
	EffectClaimCompletionStatusAbandoned EffectClaimCompletionStatus = "abandoned"
)

// +kubebuilder:validation:Enum=Reserved;Completed;Released;Abandoned
type EffectClaimPhase string

const (
	EffectClaimPhaseReserved  EffectClaimPhase = "Reserved"
	EffectClaimPhaseCompleted EffectClaimPhase = "Completed"
	EffectClaimPhaseReleased  EffectClaimPhase = "Released"
	EffectClaimPhaseAbandoned EffectClaimPhase = "Abandoned"
)

// EffectClaimSpec defines the desired state of EffectClaim.
type EffectClaimSpec struct {
	// StepRunRef identifies the StepRun that owns this effect claim.
	// +kubebuilder:validation:Required
	StepRunRef refs.StepRunReference `json:"stepRunRef"`

	// EffectKey identifies the stable external effect within the StepRun.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	EffectKey string `json:"effectKey"`

	// IdempotencyKey optionally copies the step-scoped business idempotency key.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	IdempotencyKey string `json:"idempotencyKey,omitempty"`

	// HolderIdentity identifies the current reservation owner.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	HolderIdentity string `json:"holderIdentity,omitempty"`

	// LeaseDurationSeconds bounds how long a reservation may remain unrenewed
	// before another worker may recover it.
	// +kubebuilder:validation:Minimum=1
	// +optional
	LeaseDurationSeconds int32 `json:"leaseDurationSeconds,omitempty"`

	// AcquireTime records when the current holder first acquired the reservation.
	// +optional
	AcquireTime *metav1.MicroTime `json:"acquireTime,omitempty"`

	// RenewTime records the latest observed successful renewal by the holder.
	// +optional
	RenewTime *metav1.MicroTime `json:"renewTime,omitempty"`

	// LeaseTransitions counts successful stale-takeover transitions.
	// +kubebuilder:validation:Minimum=0
	// +optional
	LeaseTransitions int32 `json:"leaseTransitions,omitempty"`

	// CompletionStatus records the terminal effect outcome once known.
	// +optional
	CompletionStatus EffectClaimCompletionStatus `json:"completionStatus,omitempty"`

	// CompletedAt records when the effect entered a terminal completion state.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Details carries compact structured metadata about the terminal effect outcome.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Details *runtime.RawExtension `json:"details,omitempty"`
}

// EffectClaimStatus defines the observed state of EffectClaim.
// +kubebuilder:validation:XValidation:message="status.conditions reason field must be <= 64 characters",rule="!has(self.conditions) || self.conditions.all(c, !has(c.reason) || size(c.reason) <= 64)"
// +kubebuilder:validation:XValidation:message="status.conditions message field must be <= 2048 characters",rule="!has(self.conditions) || self.conditions.all(c, !has(c.message) || size(c.message) <= 2048)"
type EffectClaimStatus struct {
	// ObservedGeneration is the latest generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase provides a canonical high-level lifecycle summary for the claim.
	// +optional
	Phase EffectClaimPhase `json:"phase,omitempty"`

	// Conditions provide canonical Kubernetes lifecycle status for the claim.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=eff,categories={bubu,ai,runs}
// +kubebuilder:printcolumn:name="StepRun",type=string,JSONPath=.spec.stepRunRef.name
// +kubebuilder:printcolumn:name="Effect",type=string,JSONPath=.spec.effectKey
// +kubebuilder:printcolumn:name="Holder",type=string,JSONPath=.spec.holderIdentity
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=.status.phase
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp

// EffectClaim is the Schema for the effectclaims API.
type EffectClaim struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of EffectClaim
	// +required
	Spec EffectClaimSpec `json:"spec"`

	// status defines the observed state of EffectClaim
	// +optional
	Status EffectClaimStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// EffectClaimList contains a list of EffectClaim.
type EffectClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []EffectClaim `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EffectClaim{}, &EffectClaimList{})
}
