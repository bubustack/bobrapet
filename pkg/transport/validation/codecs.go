package validation

import (
	"fmt"
	"strings"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
)

// ValidateCodecSupport ensures binding codecs exist in the transport capability list.
func ValidateCodecSupport(binding *transportv1alpha1.TransportBinding, transport *transportv1alpha1.Transport) error {
	if binding == nil || transport == nil {
		return nil
	}
	if err := validateAudio(binding.Spec.Audio, transport.Spec.SupportedAudio); err != nil {
		return err
	}
	if err := validateVideo(binding.Spec.Video, transport.Spec.SupportedVideo); err != nil {
		return err
	}
	if err := validateBinary(binding.Spec.Binary, transport.Spec.SupportedBinary); err != nil {
		return err
	}
	return nil
}

func validateAudio(binding *transportv1alpha1.AudioBinding, supported []transportv1alpha1.AudioCodec) error {
	if binding == nil || len(binding.Codecs) == 0 {
		return nil
	}
	if len(supported) == 0 {
		return fmt.Errorf("transport does not support audio codecs")
	}
	for _, codec := range binding.Codecs {
		if !audioCodecSupported(codec, supported) {
			return fmt.Errorf("audio codec %s is not supported by transport", codec.Name)
		}
	}
	return nil
}

func validateVideo(binding *transportv1alpha1.VideoBinding, supported []transportv1alpha1.VideoCodec) error {
	if binding == nil || len(binding.Codecs) == 0 {
		return nil
	}
	if len(supported) == 0 {
		return fmt.Errorf("transport does not support video codecs")
	}
	for _, codec := range binding.Codecs {
		if !videoCodecSupported(codec, supported) {
			return fmt.Errorf("video codec %s is not supported by transport", codec.Name)
		}
	}
	return nil
}

func validateBinary(binding *transportv1alpha1.BinaryBinding, supported []string) error {
	if binding == nil || len(binding.MimeTypes) == 0 {
		return nil
	}
	if len(supported) == 0 {
		return fmt.Errorf("transport does not support binary payloads")
	}
	for _, mt := range binding.MimeTypes {
		if !binaryTypeSupported(mt, supported) {
			return fmt.Errorf("binary mime type %s is not supported by transport", mt)
		}
	}
	return nil
}

func audioCodecSupported(codec transportv1alpha1.AudioCodec, supported []transportv1alpha1.AudioCodec) bool {
	name := strings.ToLower(strings.TrimSpace(codec.Name))
	for _, allowed := range supported {
		if strings.EqualFold(allowed.Name, name) {
			return true
		}
	}
	return false
}

func videoCodecSupported(codec transportv1alpha1.VideoCodec, supported []transportv1alpha1.VideoCodec) bool {
	name := strings.ToLower(strings.TrimSpace(codec.Name))
	for _, allowed := range supported {
		if strings.EqualFold(allowed.Name, name) {
			return true
		}
	}
	return false
}

func binaryTypeSupported(mime string, supported []string) bool {
	name := strings.ToLower(strings.TrimSpace(mime))
	for _, allowed := range supported {
		if strings.EqualFold(strings.TrimSpace(allowed), name) {
			return true
		}
	}
	return false
}

// HasAnyLane returns true if the binding declares at least one media lane.
func HasAnyLane(binding *transportv1alpha1.TransportBinding) bool {
	if binding == nil {
		return false
	}
	return (binding.Spec.Audio != nil) ||
		(binding.Spec.Video != nil) ||
		(binding.Spec.Binary != nil)
}
