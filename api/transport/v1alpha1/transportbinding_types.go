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

	"github.com/bubustack/bobrapet/pkg/refs"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// MediaDirection describes whether the engram publishes, subscribes, or does both for a media stream.
// +kubebuilder:validation:Enum=publish;subscribe;bidirectional
type MediaDirection string

const (
	MediaDirectionPublish       MediaDirection = "publish"
	MediaDirectionSubscribe     MediaDirection = "subscribe"
	MediaDirectionBidirectional MediaDirection = "bidirectional"
)

// AudioCodec describes a negotiated audio codec.
type AudioCodec struct {
	// name of the codec (e.g., pcm16, opus, g711-mu)
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=pcm16;pcm24;opus;g711-mu;g711-a;g722;aac;mp3;flac;raw
	Name string `json:"name"`

	// sampleRateHz indicates the sampling rate for this codec.
	// +optional
	SampleRateHz int32 `json:"sampleRateHz,omitempty"`

	// channels indicates the number of channels carried by this codec.
	// +optional
	Channels int32 `json:"channels,omitempty"`
}

// VideoCodec describes a negotiated video codec or raw pixel format.
type VideoCodec struct {
	// name of the codec (e.g., h264, vp8, raw-yuv420)
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=h264;vp8;vp9;av1;raw-yuv420;raw-rgb;raw-bgr
	Name string `json:"name"`

	// profile or format hint (e.g., baseline, high, rgb)
	// +optional
	Profile string `json:"profile,omitempty"`
}

// AudioBinding declares how audio should flow across the connector.
type AudioBinding struct {
	// direction indicates whether the owning engram publishes, subscribes, or both.
	// +kubebuilder:validation:Required
	Direction MediaDirection `json:"direction"`

	// codecs enumerates the preferred codecs ordered by priority.
	// If empty, defaults to PCM16.
	// +optional
	Codecs []AudioCodec `json:"codecs,omitempty"`
}

// VideoBinding declares how video should flow across the connector.
type VideoBinding struct {
	// direction indicates whether the owning engram publishes, subscribes, or both.
	// +kubebuilder:validation:Required
	Direction MediaDirection `json:"direction"`

	// codecs enumerates supported codecs. Encoded pass-through drivers should
	// declare the codecs they can forward.
	// +optional
	Codecs []VideoCodec `json:"codecs,omitempty"`

	// raw indicates whether raw frames are required for processing (e.g., CV tasks).
	// Drivers may use this hint to determine if decoding is necessary.
	// +optional
	Raw bool `json:"raw,omitempty"`
}

// BinaryBinding declares how generic binary payloads should flow across the connector.
type BinaryBinding struct {
	// direction indicates whether the owning engram publishes, subscribes, or both.
	// +kubebuilder:validation:Required
	Direction MediaDirection `json:"direction"`

	// mimeTypes enumerates supported MIME types (e.g., application/json, application/octet-stream).
	// +optional
	MimeTypes []string `json:"mimeTypes,omitempty"`
}

// TransportBindingSpec defines the desired state of TransportBinding.
type TransportBindingSpec struct {
	// transportRef references the Transport CR driving this binding.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	TransportRef string `json:"transportRef"`

	// storyRunRef identifies the StoryRun this binding belongs to when scoped to an individual run.
	// +optional
	StoryRunRef *refs.StoryRunReference `json:"storyRunRef,omitempty"`

	// stepName is the Story step that owns this binding.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	StepName string `json:"stepName"`

	// engramName is the run-specific engram backing the step.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	EngramName string `json:"engramName"`

	// driver indicates which connector driver should be used.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Driver string `json:"driver"`

	// audio describes the desired audio flow for this binding.
	// +optional
	Audio *AudioBinding `json:"audio,omitempty"`

	// video describes the desired video flow for this binding.
	// +optional
	Video *VideoBinding `json:"video,omitempty"`

	// binary describes the desired binary flow for this binding.
	// +optional
	Binary *BinaryBinding `json:"binary,omitempty"`

	// connectorEndpoint is the in-pod endpoint engrams should dial to reach the connector sidecar (e.g., 127.0.0.1:50051).
	// +optional
	ConnectorEndpoint string `json:"connectorEndpoint,omitempty"`

	// settings carries provider-specific configuration serialized as JSON.
	// +optional
	RawSettings *runtime.RawExtension `json:"rawSettings,omitempty"`
}

// TransportBindingStatus defines the observed state of TransportBinding.
type TransportBindingStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// negotiatedAudio captures the codec the connector selected for audio.
	// +optional
	NegotiatedAudio *AudioCodec `json:"negotiatedAudio,omitempty"`

	// negotiatedVideo captures the codec the connector selected for video.
	// +optional
	NegotiatedVideo *VideoCodec `json:"negotiatedVideo,omitempty"`

	// negotiatedBinary captures the MIME type the connector selected for binary payloads.
	// +optional
	NegotiatedBinary string `json:"negotiatedBinary,omitempty"`

	// endpoint is the connector endpoint (host:port or unix socket) for this binding.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// downstreamHost is the resolved downstream endpoint (host:port) for the engram owning this binding.
	// This is populated when the engram can accept direct P2P connections from upstream engrams.
	// +optional
	DownstreamHost string `json:"downstreamHost,omitempty"`

	// upstreamHost is the resolved upstream endpoint (host:port) that this engram should connect to.
	// This is populated when the connection can bypass the hub and use direct P2P routing.
	// +optional
	UpstreamHost string `json:"upstreamHost,omitempty"`

	// conditions represent the current state of the TransportBinding resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=tb;tbind,categories={bubu,transport}
// +kubebuilder:printcolumn:name="Transport",type=string,JSONPath=.spec.transportRef
// +kubebuilder:printcolumn:name="Step",type=string,JSONPath=.spec.stepName
// +kubebuilder:printcolumn:name="Driver",type=string,JSONPath=.spec.driver
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=.status.endpoint
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
// +kubebuilder:selectablefield:JSONPath=".spec.transportRef"
// +kubebuilder:selectablefield:JSONPath=".spec.storyRunRef.name"

// TransportBinding is the Schema for the transportbindings API
type TransportBinding struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of TransportBinding
	// +required
	Spec TransportBindingSpec `json:"spec"`

	// status defines the observed state of TransportBinding
	// +optional
	Status TransportBindingStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TransportBindingList contains a list of TransportBinding
type TransportBindingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TransportBinding `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TransportBinding{}, &TransportBindingList{})
}
