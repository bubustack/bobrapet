package transport

import (
	"encoding/json"
	"strings"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"k8s.io/apimachinery/pkg/runtime"
)

// MergeSettings merges the default transport settings with overrides, returning a raw JSON blob.
func MergeSettings(base, overrides *runtime.RawExtension) ([]byte, error) {
	merged, err := kubeutil.MergeWithBlocks(base, overrides)
	if err != nil {
		return nil, err
	}
	if merged == nil || len(merged.Raw) == 0 {
		return nil, nil
	}
	return append([]byte(nil), merged.Raw...), nil
}

// MergeSettingsWithStreaming merges a raw settings block with structured streaming settings.
func MergeSettingsWithStreaming(base *runtime.RawExtension, streaming any) (*runtime.RawExtension, error) {
	streamingRaw, err := MarshalSettings(applyStreamingDefaults(streaming))
	if err != nil {
		return nil, err
	}
	merged, err := kubeutil.MergeWithBlocks(base, streamingRaw)
	if err != nil || merged == nil || len(merged.Raw) == 0 {
		return merged, err
	}
	return &runtime.RawExtension{Raw: append([]byte(nil), merged.Raw...)}, nil
}

// MarshalSettings encodes a settings struct into a RawExtension, returning nil for empty values.
func MarshalSettings(value any) (*runtime.RawExtension, error) {
	if value == nil {
		return nil, nil
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(string(bytes))
	if trimmed == "" || trimmed == "{}" || trimmed == "null" {
		return nil, nil
	}
	return &runtime.RawExtension{Raw: bytes}, nil
}

func applyStreamingDefaults(streaming any) any {
	switch typed := streaming.(type) {
	case *transportv1alpha1.TransportStreamingSettings:
		return applyTransportStreamingDefaults(typed)
	default:
		return streaming
	}
}

func applyTransportStreamingDefaults(in *transportv1alpha1.TransportStreamingSettings) *transportv1alpha1.TransportStreamingSettings { //nolint:lll
	if in == nil {
		out := &transportv1alpha1.TransportStreamingSettings{}
		applyFlowControlDefaultsTransport(out)
		applyRoutingDefaultsTransport(out)
		applyLaneDefaultsTransport(out)
		return out
	}
	out := in.DeepCopy()
	applyFlowControlDefaultsTransport(out)
	applyRoutingDefaultsTransport(out)
	applyLaneDefaultsTransport(out)
	return out
}

func applyFlowControlDefaultsTransport(settings *transportv1alpha1.TransportStreamingSettings) {
	if settings == nil {
		return
	}
	fc := settings.FlowControl
	if fc == nil {
		fc = &transportv1alpha1.TransportFlowControlSettings{}
		settings.FlowControl = fc
	}
	if shouldDefaultFlowControlTransport(fc) {
		fc.Mode = transportv1alpha1.FlowControlCredits
		fc.AckEvery = &transportv1alpha1.TransportFlowAckSettings{
			Messages: int32Ptr(10),
			MaxDelay: stringPtr("250ms"),
		}
		fc.PauseThreshold = &transportv1alpha1.TransportFlowThreshold{BufferPct: int32Ptr(90)}
		fc.ResumeThreshold = &transportv1alpha1.TransportFlowThreshold{BufferPct: int32Ptr(50)}
		return
	}
	if fc.Mode == transportv1alpha1.FlowControlCredits || fc.Mode == transportv1alpha1.FlowControlWindow {
		if fc.AckEvery == nil {
			fc.AckEvery = &transportv1alpha1.TransportFlowAckSettings{Messages: int32Ptr(10), MaxDelay: stringPtr("250ms")}
		}
		if fc.PauseThreshold == nil {
			fc.PauseThreshold = &transportv1alpha1.TransportFlowThreshold{BufferPct: int32Ptr(90)}
		}
		if fc.ResumeThreshold == nil {
			fc.ResumeThreshold = &transportv1alpha1.TransportFlowThreshold{BufferPct: int32Ptr(50)}
		}
	}
}

func applyRoutingDefaultsTransport(settings *transportv1alpha1.TransportStreamingSettings) {
	if settings == nil {
		return
	}
	if settings.Routing == nil {
		settings.Routing = &transportv1alpha1.TransportRoutingSettings{}
	}
	if settings.Routing.MaxDownstreams == nil {
		settings.Routing.MaxDownstreams = int32Ptr(32)
	}
}

func applyLaneDefaultsTransport(settings *transportv1alpha1.TransportStreamingSettings) {
	if settings == nil {
		return
	}
	if len(settings.Lanes) == 0 {
		settings.Lanes = defaultTransportLanes()
		return
	}
	for i := range settings.Lanes {
		applyLaneDefaultsTransportEntry(&settings.Lanes[i])
	}
}

func applyLaneDefaultsTransportEntry(lane *transportv1alpha1.TransportLane) {
	if lane == nil {
		return
	}
	if lane.MaxMessages == nil {
		lane.MaxMessages = int32Ptr(defaultLaneMaxMessages)
	}
	if lane.MaxBytes == nil {
		lane.MaxBytes = int32Ptr(defaultLaneMaxBytes)
	}
	if lane.Direction == "" {
		lane.Direction = transportv1alpha1.TransportLaneBidirectional
	}
}

func defaultTransportLanes() []transportv1alpha1.TransportLane {
	return []transportv1alpha1.TransportLane{
		{
			Name:        "audio",
			Kind:        transportv1alpha1.TransportLaneAudio,
			Direction:   transportv1alpha1.TransportLaneBidirectional,
			MaxMessages: int32Ptr(defaultLaneMaxMessages),
			MaxBytes:    int32Ptr(defaultLaneMaxBytes),
		},
		{
			Name:        "video",
			Kind:        transportv1alpha1.TransportLaneVideo,
			Direction:   transportv1alpha1.TransportLaneBidirectional,
			MaxMessages: int32Ptr(defaultLaneMaxMessages),
			MaxBytes:    int32Ptr(defaultLaneMaxBytes),
		},
		{
			Name:        "binary",
			Kind:        transportv1alpha1.TransportLaneBinary,
			Direction:   transportv1alpha1.TransportLaneBidirectional,
			MaxMessages: int32Ptr(defaultLaneMaxMessages),
			MaxBytes:    int32Ptr(defaultLaneMaxBytes),
		},
		{
			Name:        "payload",
			Kind:        transportv1alpha1.TransportLanePayload,
			Direction:   transportv1alpha1.TransportLaneBidirectional,
			MaxMessages: int32Ptr(defaultLaneMaxMessages),
			MaxBytes:    int32Ptr(defaultLaneMaxBytes),
		},
	}
}

func shouldDefaultFlowControlTransport(fc *transportv1alpha1.TransportFlowControlSettings) bool {
	if fc == nil {
		return true
	}
	if fc.Mode != "" {
		return false
	}
	return fc.InitialCredits == nil && fc.AckEvery == nil && fc.PauseThreshold == nil && fc.ResumeThreshold == nil
}

const (
	defaultLaneMaxMessages int32 = 100
	defaultLaneMaxBytes    int32 = 1 * 1024 * 1024
)

func int32Ptr(val int32) *int32 {
	return &val
}

func stringPtr(val string) *string {
	return &val
}
