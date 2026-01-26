package transport

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/bubustack/core/contracts"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
)

func TestAllowInsecureTransportFromSecurityMode(t *testing.T) {
	info := &transportpb.BindingInfo{
		Payload: []byte(fmt.Sprintf(
			`{"env":{"%s":"%s"}}`,
			contracts.TransportSecurityModeEnv,
			contracts.TransportSecurityModePlaintext,
		)),
	}
	if !AllowInsecureTransport(nil, info) {
		t.Fatalf("expected AllowInsecureTransport to honor plaintext security mode override")
	}

	info.Payload = []byte(fmt.Sprintf(
		`{"env":{"%s":"%s"}}`,
		contracts.TransportSecurityModeEnv,
		contracts.TransportSecurityModeTLS,
	))
	if AllowInsecureTransport(nil, info) {
		t.Fatalf("expected AllowInsecureTransport to enforce TLS security mode override")
	}
}

func TestApplyTransportSecurityEnv(t *testing.T) {
	envs := ApplyTransportSecurityEnv(nil, true)
	if got := findEnv(envs, contracts.TransportSecurityModeEnv); got != contracts.TransportSecurityModePlaintext {
		t.Fatalf("expected plaintext mode, got %q", got)
	}

	envs = ApplyTransportSecurityEnv([]corev1.EnvVar{{Name: "EXISTING", Value: "1"}}, false)
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
