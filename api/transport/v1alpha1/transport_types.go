package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/bubustack/bobrapet/pkg/enums"
)

// TransportSpec defines a provider-neutral transport description.
type TransportSpec struct {
	// provider is the human-readable provider name (e.g., livekit, twilio, custom).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Provider string `json:"provider"`

	// driver is the connector driver identifier that should be used for bindings.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Driver string `json:"driver"`

	// connectorImage optionally declares the recommended connector image implementing this driver.
	// +optional
	ConnectorImage string `json:"connectorImage,omitempty"`

	// supportedAudio lists the audio codecs supported by this transport.
	// +optional
	SupportedAudio []AudioCodec `json:"supportedAudio,omitempty"`

	// supportedVideo lists the video codecs supported by this transport.
	// +optional
	SupportedVideo []VideoCodec `json:"supportedVideo,omitempty"`

	// supportedBinary lists MIME types supported for generic binary payloads.
	// +optional
	SupportedBinary []string `json:"supportedBinary,omitempty"`

	// streaming configures default streaming policies such as backpressure, flow control, and delivery.
	// +optional
	Streaming *TransportStreamingSettings `json:"streaming,omitempty"`

	// configSchema optionally captures a JSONSchema for provider-specific settings.
	// +optional
	ConfigSchema *runtime.RawExtension `json:"configSchema,omitempty"`

	// defaultSettings contains default provider-specific settings serialized as JSON.
	// +optional
	DefaultSettings *runtime.RawExtension `json:"defaultSettings,omitempty"`
}

// TransportStatus reports validation/usage state for a transport template.
type TransportStatus struct {
	ObservedGeneration int64                  `json:"observedGeneration,omitempty"`
	ValidationStatus   enums.ValidationStatus `json:"validationStatus,omitempty"`
	ValidationErrors   []string               `json:"validationErrors,omitempty"`
	UsageCount         int32                  `json:"usageCount,omitempty"`
	// negotiatedAudio lists the codecs the connector actually exposes/negotiates.
	// +optional
	AvailableAudio []AudioCodec `json:"availableAudio,omitempty"`
	// negotiatedVideo lists the video codecs the connector supports.
	// +optional
	AvailableVideo []VideoCodec `json:"availableVideo,omitempty"`
	// negotiatedBinary lists MIME types the connector supports for binary payloads.
	// +optional
	AvailableBinary []string           `json:"availableBinary,omitempty"`
	Conditions      []metav1.Condition `json:"conditions,omitempty"`
	// pendingBindings tracks how many bindings still await negotiation.
	// +optional
	PendingBindings int32 `json:"pendingBindings,omitempty"`
	// lastHeartbeat records when a binding last reported capabilities.
	// +optional
	LastHeartbeatTime *metav1.Time `json:"lastHeartbeatTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=transport,categories={bubu,ai}

// Transport defines a reusable transport configuration.
type Transport struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Transport
	// +required
	Spec TransportSpec `json:"spec"`

	// status defines the observed state of Transport
	// +optional
	Status TransportStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TransportList lists transports.
type TransportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Transport `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Transport{}, &TransportList{})
}
