package transport

import (
	"strings"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
)

// ResolveTLSSecretName determines the TLS secret for an Engram, considering spec overrides
// and an operator-level default.
func ResolveTLSSecretName(engram *bubuv1alpha1.Engram, defaultSecret string) string {
	if secret := strings.TrimSpace(explicitSpecTLSSecret(engram)); secret != "" {
		return secret
	}
	if usesDefaultTLS(engram) {
		return strings.TrimSpace(defaultSecret)
	}
	return ""
}

// ExplicitTLSSecretName returns the concrete secret explicitly referenced by spec.
func ExplicitTLSSecretName(engram *bubuv1alpha1.Engram) string {
	if secret := strings.TrimSpace(explicitSpecTLSSecret(engram)); secret != "" {
		return secret
	}
	return ""
}

func explicitSpecTLSSecret(engram *bubuv1alpha1.Engram) string {
	if engram == nil || engram.Spec.Transport == nil || engram.Spec.Transport.TLS == nil {
		return ""
	}
	if ref := engram.Spec.Transport.TLS.SecretRef; ref != nil {
		return ref.Name
	}
	return ""
}

func usesDefaultTLS(engram *bubuv1alpha1.Engram) bool {
	if engram == nil || engram.Spec.Transport == nil || engram.Spec.Transport.TLS == nil {
		return true
	}
	if engram.Spec.Transport.TLS.SecretRef != nil && strings.TrimSpace(engram.Spec.Transport.TLS.SecretRef.Name) != "" {
		return false
	}
	if engram.Spec.Transport.TLS.UseDefaultTLS != nil {
		return *engram.Spec.Transport.TLS.UseDefaultTLS
	}
	return true
}
