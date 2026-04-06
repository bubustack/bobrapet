package transport

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
)

func TestBindingEnvelopeRoundTripToConnector(t *testing.T) {
	t.Helper()

	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "binding-demo",
			Namespace: "default",
		},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef:      "transport-demo",
			StepName:          "step-authenticate",
			EngramName:        "engram-authenticate",
			Driver:            "demo-driver",
			ConnectorEndpoint: "127.0.0.1:50051",
			RawSettings: &runtime.RawExtension{
				Raw: []byte(`{"env":{"BINDING_OVERRIDE":"value","NUM":123}}`),
			},
			Audio: &transportv1alpha1.AudioBinding{
				Codecs: []transportv1alpha1.AudioCodec{{Name: "opus"}},
			},
		},
		Status: transportv1alpha1.TransportBindingStatus{
			Endpoint: "grpc://connector.default.svc.cluster.local:8443",
		},
	}

	envValue, err := EncodeBindingEnv(binding)
	require.NoError(t, err)
	require.NotEmpty(t, envValue)

	sanitized := coretransport.SanitizeBindingAnnotationValue(envValue)
	require.Equal(t, "default/binding-demo", sanitized)

	payload, err := coretransport.ParseBindingPayload(envValue)
	require.NoError(t, err)
	require.Equal(t, binding.Name, payload.Reference.Name)
	require.Equal(t, binding.Namespace, payload.Reference.Namespace)
	require.NotNil(t, payload.Info)

	overrides := coretransport.BindingEnvOverrides(payload.Info)
	require.Equal(t, map[string]string{
		"BINDING_OVERRIDE": "value",
		"NUM":              "123",
	}, overrides)

	envVars := coretransport.AppendTransportMetadataEnv(nil, payload.Info)
	envVars = coretransport.AppendBindingEnvOverrides(envVars, payload.Info)
	requireEnvVar(t, envVars, contracts.TransportDriverEnv, binding.Spec.Driver)
	requireEnvVar(t, envVars, contracts.TransportEndpointEnv, binding.Spec.ConnectorEndpoint)
	requireEnvVar(t, envVars, contracts.TransportAudioCodecsEnv, "opus")
}

func requireEnvVar(t *testing.T, envVars []corev1.EnvVar, name, value string) {
	t.Helper()
	for _, env := range envVars {
		if env.Name == name {
			require.Equal(t, value, env.Value, "unexpected value for env var %s", name)
			return
		}
	}
	t.Fatalf("env var %s not found in %#v", name, envVars)
}
