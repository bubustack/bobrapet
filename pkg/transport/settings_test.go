package transport

import (
	"encoding/json"
	"testing"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestMergeSettingsWithStreaming(t *testing.T) {
	base := &runtime.RawExtension{Raw: []byte(`{"backpressure":{"buffer":{"maxMessages":10}},"provider":{"foo":"bar"}}`)}
	streaming := &transportv1alpha1.TransportStreamingSettings{
		Backpressure: &transportv1alpha1.TransportBackpressureSettings{
			Buffer: &transportv1alpha1.TransportBufferSettings{
				MaxMessages: ptrInt32(20),
				DropPolicy:  transportv1alpha1.BufferDropOldest,
			},
		},
		FlowControl: &transportv1alpha1.TransportFlowControlSettings{
			Mode: transportv1alpha1.FlowControlCredits,
		},
	}

	merged, err := MergeSettingsWithStreaming(base, streaming)
	require.NoError(t, err)
	require.NotNil(t, merged)

	var out map[string]any
	require.NoError(t, json.Unmarshal(merged.Raw, &out))
	require.Equal(t, map[string]any{"foo": "bar"}, out["provider"])

	backpressure := out["backpressure"].(map[string]any)
	buffer := backpressure["buffer"].(map[string]any)
	require.Equal(t, float64(20), buffer["maxMessages"])
	require.Equal(t, "drop_oldest", buffer["dropPolicy"])

	flow := out["flowControl"].(map[string]any)
	require.Equal(t, "credits", flow["mode"])
}

func ptrInt32(v int32) *int32 {
	return &v
}
