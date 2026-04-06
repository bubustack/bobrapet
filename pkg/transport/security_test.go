package transport

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/bubustack/core/contracts"
)

func TestApplyTransportSecurityEnv(t *testing.T) {
	envs := ApplyTransportSecurityEnv([]corev1.EnvVar{
		{Name: "EXISTING", Value: "1"},
		{Name: contracts.TransportSecurityModeEnv, Value: "plaintext"},
	})
	if got := findEnv(envs, contracts.TransportSecurityModeEnv); got != contracts.TransportSecurityModeTLS {
		t.Fatalf("expected tls mode, got %q", got)
	}
	if got := findEnv(envs, "EXISTING"); got != "1" {
		t.Fatalf("unexpected change to unrelated env var, got %q", got)
	}
}

func findEnv(envs []corev1.EnvVar, name string) string {
	for _, env := range envs {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}
