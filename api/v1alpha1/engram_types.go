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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=engram,categories={bubu,ai}
// +kubebuilder:printcolumn:name="Template",type=string,JSONPath=".spec.templateRef.name"
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=".spec.mode"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type Engram struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EngramSpec   `json:"spec,omitempty"`
	Status EngramStatus `json:"status,omitempty"`
}

// EngramSpec defines the configuration and behavior of a specific engram instance
type EngramSpec struct {
	// Which template to use for this engram instance
	// This connects the specific configuration below to a reusable component
	// +kubebuilder:validation:Required
	TemplateRef refs.EngramTemplateReference `json:"templateRef"`

	// Mode specifies how the Engram should be run.
	// Supported values are "job", "deployment", "statefulset".
	// If not specified, it defaults to the template's supported mode or "job".
	// +kubebuilder:validation:Enum=job;deployment;statefulset
	Mode enums.WorkloadMode `json:"mode,omitempty"`

	// How to configure this specific engram instance
	// This data gets validated against the EngramTemplate's configSchema
	// +kubebuilder:pruning:PreserveUnknownFields
	With *runtime.RawExtension `json:"with,omitempty"`

	// Secrets maps template secret definitions to actual Kubernetes secrets.
	Secrets map[string]string `json:"secrets,omitempty"`

	// ExecutionPolicy defines the execution configuration for this Engram.
	// These settings override any defaults from the EngramTemplate.
	// +optional
	ExecutionPolicy *ExecutionPolicy `json:"executionPolicy,omitempty"`

	// Overrides allows for fine-tuning of execution behavior.
	Overrides *ExecutionOverrides `json:"overrides,omitempty"`
}

// EngramStatus defines the observed state of Engram
type EngramStatus struct {
	// ObservedGeneration is the most recent generation observed for this Engram.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// This will indicate if the Engram's syntax is valid and its template is resolved.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Current workload status - what's actually running.
	// These fields are ONLY populated for Engrams running as a Deployment or StatefulSet.
	// For job-based engrams, these will be empty.
	Replicas      int32       `json:"replicas,omitempty"`      // How many pods are currently running
	ReadyReplicas int32       `json:"readyReplicas,omitempty"` // How many pods are actually ready to serve traffic
	Phase         enums.Phase `json:"phase,omitempty"`         // Overall state: Pending, Running, Succeeded, Failed

	// Additional useful information for debugging and monitoring
	LastExecutionTime *metav1.Time `json:"lastExecutionTime,omitempty"` // When did this last run (for jobs)
	TotalExecutions   int32        `json:"totalExecutions,omitempty"`   // How many times has this run
	FailedExecutions  int32        `json:"failedExecutions,omitempty"`  // How many executions failed
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
