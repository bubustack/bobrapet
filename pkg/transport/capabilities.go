package transport

import (
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
)

// DeriveNegotiatedCapabilities ensures the TransportBinding status reflects the currently requested codecs.
// It only fills missing negotiated fields so that connectors can later override them with the actual selection.
func DeriveNegotiatedCapabilities(
	binding *transportv1alpha1.TransportBinding,
	status *transportv1alpha1.TransportBindingStatus,
) {
	if binding == nil || status == nil {
		return
	}

	status.ObservedGeneration = binding.Generation

	if status.NegotiatedAudio == nil {
		if codec := firstAudioCodec(binding.Spec.Audio); codec != nil {
			cp := *codec
			status.NegotiatedAudio = &cp
		}
	}

	if status.NegotiatedVideo == nil {
		if codec := firstVideoCodec(binding.Spec.Video); codec != nil {
			cp := *codec
			status.NegotiatedVideo = &cp
		}
	}

	if status.NegotiatedBinary == "" {
		if mime := firstBinaryType(binding.Spec.Binary); mime != "" {
			status.NegotiatedBinary = mime
		}
	}
}

func firstAudioCodec(binding *transportv1alpha1.AudioBinding) *transportv1alpha1.AudioCodec {
	if binding == nil || len(binding.Codecs) == 0 {
		return nil
	}
	return &binding.Codecs[0]
}

func firstVideoCodec(binding *transportv1alpha1.VideoBinding) *transportv1alpha1.VideoCodec {
	if binding == nil || len(binding.Codecs) == 0 {
		return nil
	}
	return &binding.Codecs[0]
}

func firstBinaryType(binding *transportv1alpha1.BinaryBinding) string {
	if binding == nil || len(binding.MimeTypes) == 0 {
		return ""
	}
	return binding.MimeTypes[0]
}
