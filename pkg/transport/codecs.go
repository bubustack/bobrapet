package transport

import (
	"fmt"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	transportvalidation "github.com/bubustack/bobrapet/pkg/transport/validation"
)

// PopulateBindingMedia ensures the binding's audio/video declarations are compatible with the Transport.
func PopulateBindingMedia(binding *transportv1alpha1.TransportBinding, transport *transportv1alpha1.Transport) error {
	if binding == nil {
		return fmt.Errorf("binding must not be nil")
	}
	binding.Spec.Audio = buildDefaultAudioBinding(transport)
	binding.Spec.Video = buildDefaultVideoBinding(transport)
	binding.Spec.Binary = buildDefaultBinaryBinding(transport)
	return nil
}

func buildDefaultAudioBinding(transport *transportv1alpha1.Transport) *transportv1alpha1.AudioBinding {
	if transport == nil || len(transport.Spec.SupportedAudio) == 0 {
		return nil
	}
	binding := &transportv1alpha1.AudioBinding{
		Direction: transportv1alpha1.MediaDirectionBidirectional,
		Codecs:    make([]transportv1alpha1.AudioCodec, len(transport.Spec.SupportedAudio)),
	}
	copy(binding.Codecs, transport.Spec.SupportedAudio)
	return binding
}

func buildDefaultVideoBinding(transport *transportv1alpha1.Transport) *transportv1alpha1.VideoBinding {
	if transport == nil || len(transport.Spec.SupportedVideo) == 0 {
		return nil
	}
	binding := &transportv1alpha1.VideoBinding{
		Direction: transportv1alpha1.MediaDirectionBidirectional,
		Codecs:    make([]transportv1alpha1.VideoCodec, len(transport.Spec.SupportedVideo)),
	}
	copy(binding.Codecs, transport.Spec.SupportedVideo)
	return binding
}

func buildDefaultBinaryBinding(transport *transportv1alpha1.Transport) *transportv1alpha1.BinaryBinding {
	if transport == nil || len(transport.Spec.SupportedBinary) == 0 {
		return nil
	}
	mimeTypes := make([]string, len(transport.Spec.SupportedBinary))
	copy(mimeTypes, transport.Spec.SupportedBinary)
	return &transportv1alpha1.BinaryBinding{
		Direction: transportv1alpha1.MediaDirectionBidirectional,
		MimeTypes: mimeTypes,
	}
}

// ValidateCodecSupport ensures binding codecs exist in the transport capability list.
func ValidateCodecSupport(binding *transportv1alpha1.TransportBinding, transport *transportv1alpha1.Transport) error {
	return transportvalidation.ValidateCodecSupport(binding, transport)
}
