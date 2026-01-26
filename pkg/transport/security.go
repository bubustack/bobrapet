package transport

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/transport/bindinginfo"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
)

// AllowInsecureTransport returns true when binding/env overrides explicitly request plaintext,
// or when the Engram spec disables operator-managed TLS.
func AllowInsecureTransport(engram *bubuv1alpha1.Engram, info *transportpb.BindingInfo) bool {
	if info != nil {
		if overrides := bindinginfo.EnvOverrides(info); len(overrides) > 0 {
			if mode := normalizeSecurityMode(overrides[contracts.TransportSecurityModeEnv]); mode != "" {
				return mode == contracts.TransportSecurityModePlaintext
			}
		}
	}
	return engramDisablesOperatorTLS(engram)
}

func engramDisablesOperatorTLS(engram *bubuv1alpha1.Engram) bool {
	if engram == nil || engram.Spec.Transport == nil || engram.Spec.Transport.TLS == nil {
		return false
	}
	if engram.Spec.Transport.TLS.SecretRef != nil && strings.TrimSpace(engram.Spec.Transport.TLS.SecretRef.Name) != "" {
		return false
	}
	if engram.Spec.Transport.TLS.UseDefaultTLS != nil {
		return !*engram.Spec.Transport.TLS.UseDefaultTLS
	}
	return false
}

// ApplyTransportSecurityEnv ensures pods advertise the effective transport security policy.
func ApplyTransportSecurityEnv(envVars []corev1.EnvVar, allowInsecure bool) []corev1.EnvVar {
	mode := contracts.TransportSecurityModeTLS
	if allowInsecure {
		mode = contracts.TransportSecurityModePlaintext
	}
	envVars = coretransport.SetOrReplaceEnvVar(
		envVars,
		corev1.EnvVar{Name: contracts.TransportSecurityModeEnv, Value: mode},
	)
	return envVars
}

func normalizeSecurityMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case contracts.TransportSecurityModePlaintext:
		return contracts.TransportSecurityModePlaintext
	case contracts.TransportSecurityModeTLS:
		return contracts.TransportSecurityModeTLS
	default:
		return ""
	}
}
