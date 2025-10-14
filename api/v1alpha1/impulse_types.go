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

// Impulse represents a trigger that launches Stories when events occur
//
// Think of Impulses as the "event handlers" of your story system:
// - GitHub webhook → trigger CI/CD Story
// - File upload → trigger data processing Story
// - Schedule/timer → trigger backup Story
// - Queue message → trigger order processing Story
// - Slack mention → trigger support Story
//
// # Impulses are always-on services that listen for events and launch stories
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=imp,categories={bubu,ai}
// +kubebuilder:printcolumn:name="Template",type=string,JSONPath=.spec.templateRef
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

// ImpulseSpec defines how to set up an event trigger that launches Stories
//
// The flow is: External Event → Impulse (processes event) → Story (runs story)
// For example: GitHub push → webhook impulse → CI/CD story
type ImpulseSpec struct {
	// Which template to use for handling events
	// Examples: "github-webhook", "slack-handler", "file-watcher", "cron-scheduler"
	// +kubebuilder:validation:Required
	TemplateRef refs.ImpulseTemplateReference `json:"templateRef"`

	// Which Story to launch when an event is received
	// This is the story that will run in response to the trigger
	// +kubebuilder:validation:Required
	StoryRef refs.StoryReference `json:"storyRef"`

	// How to configure this specific trigger instance
	// Examples:
	// - Webhook: {"port": 8080, "path": "/webhook"}
	// - File watcher: {"bucket": "my-bucket", "prefix": "uploads/"}
	// - Scheduler: {"schedule": "0 2 * * *", "timezone": "UTC"}
	// +kubebuilder:pruning:PreserveUnknownFields
	With *runtime.RawExtension `json:"with,omitempty"`

	// Secrets configuration for this impulse instance
	// Maps template secret definitions to actual Kubernetes secrets
	// Example: {"authToken": "webhook-secret", "apiKey": "external-service-key"}
	Secrets map[string]string `json:"secrets,omitempty"`

	// How to transform the trigger event into Story inputs
	// You have complete freedom here - map any event data to any Story input
	// Examples:
	// - GitHub: {"repo": "{{ event.repository.name }}", "branch": "{{ event.ref }}"}
	// - File: {"filename": "{{ event.object.key }}", "size": "{{ event.object.size }}"}
	// +kubebuilder:pruning:PreserveUnknownFields
	Mapping *runtime.RawExtension `json:"mapping,omitempty"`

	// How to run this impulse (deployment or statefulset only - must be always-on)
	// Impulses can't be jobs because they need to continuously listen for events
	Workload *WorkloadSpec `json:"workload,omitempty"`

	// Service controls instance-level exposure of the impulse via a Service.
	// Ports and health come from the template; this sets type/labels/annotations.
	// If omitted, defaults to ClusterIP with template-defined ports.
	Service *ServiceExposure `json:"service,omitempty"`

	// Fine-tune execution behavior for special cases
	Execution *ExecutionOverrides `json:"execution,omitempty"`
}

// All shared types moved to shared_types.go

// ImpulseStatus shows the current state and activity of this trigger
type ImpulseStatus struct {
	// observedGeneration is the most recent generation observed for this Impulse. It corresponds to the
	// Impulse's generation, which is updated on mutation by the API Server.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Standard Kubernetes conditions (Ready, Available, etc.)
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Current workload status - how many trigger instances are running (managed by KEDA/HPA)
	Replicas      int32       `json:"replicas,omitempty"`      // How many trigger pods are currently running
	ReadyReplicas int32       `json:"readyReplicas,omitempty"` // How many trigger pods are ready to receive events
	Phase         enums.Phase `json:"phase,omitempty"`         // Overall state: Pending, Running, Failed

	// Event processing statistics - useful for monitoring and debugging
	TriggersReceived int64        `json:"triggersReceived,omitempty"` // Total events received since creation
	StoriesLaunched  int64        `json:"storiesLaunched,omitempty"`  // Total Stories successfully triggered
	FailedTriggers   int64        `json:"failedTriggers,omitempty"`   // Events that failed to trigger Stories
	LastTrigger      *metav1.Time `json:"lastTrigger,omitempty"`      // When did we last receive an event
	LastSuccess      *metav1.Time `json:"lastSuccess,omitempty"`      // When did we last successfully trigger a Story
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
