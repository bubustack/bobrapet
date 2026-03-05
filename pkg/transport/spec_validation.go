package transport

import transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"

// HasDeclaredCapabilities reports whether the given transport spec declares at least one
// supported audio, video, or binary capability slice. Controllers and webhooks use this
// to enforce the shared "a transport must declare some capability" rule.
func HasDeclaredCapabilities(spec *transportv1alpha1.TransportSpec) bool {
	if spec == nil {
		return false
	}
	return len(spec.SupportedAudio) > 0 || len(spec.SupportedVideo) > 0 || len(spec.SupportedBinary) > 0
}
