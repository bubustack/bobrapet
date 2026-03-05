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
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=engram,categories={bubu,ai}
// +kubebuilder:printcolumn:name="Template",type=string,JSONPath=".spec.templateRef.name"
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=".spec.mode"
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=".status.validationStatus"
// +kubebuilder:printcolumn:name="Usage",type=integer,JSONPath=".status.usageCount"
// +kubebuilder:printcolumn:name="Triggers",type=integer,JSONPath=".status.triggers"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type Engram struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Engram
	// +required
	Spec EngramSpec `json:"spec,omitempty"`

	// status defines the observed state of Engram
	// +optional
	Status EngramStatus `json:"status,omitzero"`
}

// EngramSpec describes the desired behaviour for an Engram.
type EngramSpec struct {
	// Version identifies this Engram instance for pinning by Story steps.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	Version string `json:"version,omitempty"`

	// TemplateRef selects the EngramTemplate that provides defaults, validation,
	// and base artifacts for this instance.
	// +kubebuilder:validation:Required
	TemplateRef refs.EngramTemplateReference `json:"templateRef"`

	// Mode optionally overrides the workload controller selected by the template.
	// Supported values are job, deployment, and statefulset.
	// +kubebuilder:validation:Enum=job;deployment;statefulset
	Mode enums.WorkloadMode `json:"mode,omitempty"`

	// With carries template-specific configuration. The template schema determines
	// the structure and validation rules applied to this payload.
	// +kubebuilder:pruning:PreserveUnknownFields
	With *runtime.RawExtension `json:"with,omitempty"`

	// Secrets maps named secret inputs defined by the template to concrete Secret
	// resources in the tenant namespace.
	Secrets map[string]string `json:"secrets,omitempty"`

	// ExecutionPolicy captures the fully-resolved execution policy after hierarchical
	// merge. Template defaults feed into this value.
	// +optional
	ExecutionPolicy *ExecutionPolicy `json:"executionPolicy,omitempty"`

	// Overrides allows callers to apply instance-specific execution adjustments.
	Overrides *ExecutionOverrides `json:"overrides,omitempty"`

	// Transport carries transport-level overrides such as TLS configuration.
	// +optional
	Transport *EngramTransportSpec `json:"transport,omitempty"`
}

// EngramTransportSpec defines transport-related overrides for an Engram.
type EngramTransportSpec struct {
	// TLS configures TLS secrets for hybrid transports.
	// +optional
	TLS *EngramTLSSpec `json:"tls,omitempty"`
}

// EngramTLSSpec describes TLS secret resolution behavior.
type EngramTLSSpec struct {
	// SecretRef references a Secret containing tls.key/tls.crt data.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`

	// UseDefaultTLS requests the operator's default TLS secret when true.
	// Defaults to true when SecretRef is unset.
	// +optional
	UseDefaultTLS *bool `json:"useDefaultTLS,omitempty"`
}

// EngramStatus reports observed workload state.
type EngramStatus struct {
	// ObservedGeneration reflects the last reconciled generation.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions conveys per-condition status such as Ready.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Replicas and ReadyReplicas surface pod readiness information for controllers
	// that expose it (Deployments/StatefulSets). Jobs leave these fields at zero.
	Replicas      int32       `json:"replicas,omitempty"`
	ReadyReplicas int32       `json:"readyReplicas,omitempty"`
	Phase         enums.Phase `json:"phase,omitempty"`

	// ValidationStatus reflects whether the engram spec passed validation.
	ValidationStatus enums.ValidationStatus `json:"validationStatus,omitempty"`
	// ValidationErrors contains messages describing validation failures.
	ValidationErrors []string `json:"validationErrors,omitempty"`

	// UsageCount reports how many Stories reference this Engram.
	// +optional
	UsageCount int32 `json:"usageCount"`

	// Triggers reports how many StepRuns currently reference this Engram.
	// Useful for understanding runtime fan-out and utilisation.
	// +optional
	Triggers int64 `json:"triggers"`

	// LastExecutionTime captures the completion timestamp for the most recent run.
	LastExecutionTime *metav1.Time `json:"lastExecutionTime,omitempty"`
	// TotalExecutions counts the number of completed runs for job workloads.
	TotalExecutions int32 `json:"totalExecutions,omitempty"`
	// FailedExecutions counts runs that ended in a failure state.
	FailedExecutions int32 `json:"failedExecutions,omitempty"`
}

// +kubebuilder:object:root=true
type EngramList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Engram `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Engram{}, &EngramList{})
}
