package transport

import (
	"testing"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestResolveTLSSecretName_Order(t *testing.T) {
	t.Parallel()

	withSpec := &bubuv1alpha1.Engram{
		Spec: bubuv1alpha1.EngramSpec{
			Transport: &bubuv1alpha1.EngramTransportSpec{
				TLS: &bubuv1alpha1.EngramTLSSpec{
					SecretRef: &corev1.LocalObjectReference{Name: "spec-secret"},
				},
			},
		},
	}
	require.Equal(t, "spec-secret", ResolveTLSSecretName(withSpec, "default"))

	withDefault := &bubuv1alpha1.Engram{}
	require.Equal(t, "default-secret", ResolveTLSSecretName(withDefault, "default-secret"))
}

func TestResolveTLSSecretName_DisableDefault(t *testing.T) {
	t.Parallel()

	useDefaultFalse := false
	engram := &bubuv1alpha1.Engram{
		Spec: bubuv1alpha1.EngramSpec{
			Transport: &bubuv1alpha1.EngramTransportSpec{
				TLS: &bubuv1alpha1.EngramTLSSpec{
					UseDefaultTLS: &useDefaultFalse,
				},
			},
		},
	}
	require.Equal(t, "", ResolveTLSSecretName(engram, "default"))
}

func TestExplicitTLSSecretName(t *testing.T) {
	t.Parallel()

	engram := &bubuv1alpha1.Engram{
		Spec: bubuv1alpha1.EngramSpec{
			Transport: &bubuv1alpha1.EngramTransportSpec{
				TLS: &bubuv1alpha1.EngramTLSSpec{
					SecretRef: &corev1.LocalObjectReference{Name: "spec-secret"},
				},
			},
		},
	}
	require.Equal(t, "spec-secret", ExplicitTLSSecretName(engram))

	engram.Spec.Transport.TLS.SecretRef = nil
	require.Equal(t, "", ExplicitTLSSecretName(engram))
}
