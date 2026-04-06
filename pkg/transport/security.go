package transport

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
)

// ApplyTransportSecurityEnv ensures pods advertise the effective transport security policy.
func ApplyTransportSecurityEnv(envVars []corev1.EnvVar) []corev1.EnvVar {
	envVars = coretransport.SetOrReplaceEnvVar(
		envVars,
		corev1.EnvVar{Name: contracts.TransportSecurityModeEnv, Value: contracts.TransportSecurityModeTLS},
	)
	return envVars
}
