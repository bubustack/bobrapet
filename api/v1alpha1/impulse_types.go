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

	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
)

// Impulse defines an always-on trigger that listens for external events and
// launches Stories in response.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=imp,categories={bubu,ai}
// +kubebuilder:printcolumn:name="Template",type=string,JSONPath=.spec.templateRef.name
// +kubebuilder:printcolumn:name="Story",type=string,JSONPath=.spec.storyRef.name
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=.status.phase
// +kubebuilder:printcolumn:name="Triggers",type=integer,JSONPath=.status.triggersReceived
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;list;watch;patch
type Impulse struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImpulseSpec   `json:"spec,omitempty"`
	Status ImpulseStatus `json:"status,omitempty"`
}

// ImpulseSpec describes a trigger instance and the Story it activates.
type ImpulseSpec struct {
	// TemplateRef selects the ImpulseTemplate that implements the trigger mechanics.
	// +kubebuilder:validation:Required
	TemplateRef refs.ImpulseTemplateReference `json:"templateRef"`

	// StoryRef identifies the Story executed when the trigger fires.
	// +kubebuilder:validation:Required
	StoryRef refs.StoryReference `json:"storyRef"`

	// With carries template-specific configuration and is validated by the template.
	// +kubebuilder:pruning:PreserveUnknownFields
	With *runtime.RawExtension `json:"with,omitempty"`

	// Secrets maps template-defined secret inputs to concrete namespace Secrets.
	Secrets map[string]string `json:"secrets,omitempty"`

	// Mapping describes how an incoming event is converted into Story inputs.
	// +kubebuilder:pruning:PreserveUnknownFields
	Mapping *runtime.RawExtension `json:"mapping,omitempty"`

	// Workload controls the backing workload for the impulse (deployment or statefulset).
	Workload *WorkloadSpec `json:"workload,omitempty"`

	// Service controls instance-level exposure of the impulse via a Service.
	// Ports and health come from the template; this sets type/labels/annotations.
	// If omitted, defaults to ClusterIP with template-defined ports.
	Service *ServiceExposure `json:"service,omitempty"`

	// Execution provides instance-level execution overrides.
	Execution *ExecutionOverrides `json:"execution,omitempty"`
}

// All shared types moved to shared_types.go

// ImpulseStatus reports observed trigger state and counters.
type ImpulseStatus struct {
	// observedGeneration is the most recent generation observed for this Impulse. It corresponds to the
	// Impulse's generation, which is updated on mutation by the API Server.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions is the standard Kubernetes condition set (Ready, etc.).
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Replicas and ReadyReplicas surface readiness information reported by the backing workload.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// +optional
	Phase enums.Phase `json:"phase,omitempty"`

	// TriggersReceived counts all events seen by the impulse.
	// +optional
	TriggersReceived int64 `json:"triggersReceived"`
	// StoriesLaunched counts StoryRuns successfully started from events.
	// +optional
	StoriesLaunched int64 `json:"storiesLaunched"`
	// FailedTriggers counts events that failed validation or launch.
	// +optional
	FailedTriggers int64 `json:"failedTriggers"`
	// LastTrigger records the timestamp of the most recent event.
	// +optional
	LastTrigger *metav1.Time `json:"lastTrigger,omitempty"`
	// LastSuccess records the timestamp of the most recent successful Story launch.
	// +optional
	LastSuccess *metav1.Time `json:"lastSuccess,omitempty"`
}

// +kubebuilder:object:root=true
type ImpulseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Impulse `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Impulse{}, &ImpulseList{})
}
