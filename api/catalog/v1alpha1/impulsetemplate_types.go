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

// ImpulseTemplate defines a reusable "trigger" component that launches stories
//
// Think of ImpulseTemplates as event listeners or sensors that know how to handle specific types of events:
// - "github-webhook": Handles GitHub webhook events (push, PR, release, etc.)
// - "slack-handler": Processes Slack events (mentions, slash commands, reactions)
// - "file-watcher": Monitors file uploads/changes in cloud storage
// - "cron-scheduler": Triggers stories on schedules (like GitHub Actions cron)
// - "kafka-consumer": Consumes messages from Kafka topics
//
// Templates are cluster-scoped because they're meant to be shared across teams and namespaces.
// They define WHAT events can be handled, while Impulses define HOW to configure and use them.
//
// The relationship is:
// ImpulseTemplate (defines trigger capabilities) → Impulse (configured trigger) → Launches Stories
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=itpl,categories={bubu,ai,catalog}
// +kubebuilder:printcolumn:name="Description",type=string,JSONPath=.spec.description
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=.spec.version
// +kubebuilder:printcolumn:name="Usage",type=integer,JSONPath=.status.usageCount
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=.status.validationStatus
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
type ImpulseTemplate struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ImpulseTemplate
	// +required
	Spec ImpulseTemplateSpec `json:"spec,omitempty"`

	// status defines the observed state of ImpulseTemplate
	// +optional
	Status ImpulseTemplateStatus `json:"status,omitzero"`
}

// ImpulseTemplateSpec defines the capabilities and contract of a trigger component
type ImpulseTemplateSpec struct {
	// Common template fields (version, description, image, etc.)
	TemplateSpec `json:",inline"`

	// What data does this trigger provide when events occur?
	// This defines the structure of the event context that will be available for mapping to Story inputs
	// Examples:
	// - GitHub webhook: {"repository": "object", "ref": "string", "commits": "array", "sender": "object"}
	// - File watcher: {"bucket": "string", "objectKey": "string", "size": "integer", "timestamp": "string"}
	// - Slack event: {"channel": "string", "user": "object", "message": "string", "timestamp": "string"}
	// - Cron trigger: {"scheduledTime": "string", "timezone": "string", "schedule": "string"}
	// +kubebuilder:pruning:PreserveUnknownFields
	ContextSchema *runtime.RawExtension `json:"contextSchema,omitempty"`

	// DeliveryPolicy defines default trigger delivery behavior (dedupe/retry) for Impulses.
	// +optional
	DeliveryPolicy *TriggerDeliveryPolicy `json:"deliveryPolicy,omitempty"`
}

type ImpulseTemplateStatus struct {
	TemplateStatus `json:",inline"`
}

// GetTemplateStatus returns a pointer to the embedded TemplateStatus.
// This enables shared status update helpers to operate on both template types.
func (t *ImpulseTemplate) GetTemplateStatus() *TemplateStatus {
	return &t.Status.TemplateStatus
}

// GetImage returns the template's container image.
func (t *ImpulseTemplate) GetImage() string {
	return t.Spec.Image
}

// GetVersion returns the template's version.
func (t *ImpulseTemplate) GetVersion() string {
	return t.Spec.Version
}

// +kubebuilder:object:root=true
type ImpulseTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ImpulseTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ImpulseTemplate{}, &ImpulseTemplateList{})
}
