package transport

import (
	"testing"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
)

func TestPopulateBindingMediaDefaults(t *testing.T) {
	binding := &transportv1alpha1.TransportBinding{}
	if err := PopulateBindingMedia(binding, nil); err != nil {
		t.Fatalf("PopulateBindingMedia returned error: %v", err)
	}
	if binding.Spec.Audio == nil {
		t.Fatalf("expected audio binding to be initialized")
	}
	if got := binding.Spec.Audio.Codecs[0].Name; got != "pcm16" {
		t.Fatalf("expected default audio codec pcm16, got %s", got)
	}
	if binding.Spec.Video != nil {
		t.Fatalf("expected video binding to remain nil when no transport is provided")
	}
	if binding.Spec.Binary != nil {
		t.Fatalf("expected binary binding to remain nil when no transport is provided")
	}
}

func TestPopulateBindingMediaCopiesTransportCapabilities(t *testing.T) {
	transport := &transportv1alpha1.Transport{
		Spec: transportv1alpha1.TransportSpec{
			SupportedAudio:  []transportv1alpha1.AudioCodec{{Name: "opus"}},
			SupportedVideo:  []transportv1alpha1.VideoCodec{{Name: "h264"}},
			SupportedBinary: []string{"application/json"},
		},
	}
	binding := &transportv1alpha1.TransportBinding{}
	if err := PopulateBindingMedia(binding, transport); err != nil {
		t.Fatalf("PopulateBindingMedia returned error: %v", err)
	}
	if binding.Spec.Audio == nil ||
		len(binding.Spec.Audio.Codecs) != 1 ||
		binding.Spec.Audio.Codecs[0].Name != "opus" {
		t.Fatalf("expected audio codecs to match transport declaration")
	}
	if binding.Spec.Video == nil ||
		len(binding.Spec.Video.Codecs) != 1 ||
		binding.Spec.Video.Codecs[0].Name != "h264" {
		t.Fatalf("expected video codecs to match transport declaration")
	}
	if binding.Spec.Binary == nil ||
		len(binding.Spec.Binary.MimeTypes) != 1 ||
		binding.Spec.Binary.MimeTypes[0] != "application/json" {
		t.Fatalf("expected binary mime types to match transport declaration")
	}
}

func TestValidateCodecSupport(t *testing.T) {
	baseTransport := &transportv1alpha1.Transport{
		Spec: transportv1alpha1.TransportSpec{
			SupportedAudio:  []transportv1alpha1.AudioCodec{{Name: "opus"}},
			SupportedVideo:  []transportv1alpha1.VideoCodec{{Name: "h264"}},
			SupportedBinary: []string{"application/json"},
		},
	}
	t.Run("happy path", func(t *testing.T) {
		binding := &transportv1alpha1.TransportBinding{
			Spec: transportv1alpha1.TransportBindingSpec{
				Audio:  &transportv1alpha1.AudioBinding{Codecs: []transportv1alpha1.AudioCodec{{Name: "opus"}}},
				Video:  &transportv1alpha1.VideoBinding{Codecs: []transportv1alpha1.VideoCodec{{Name: "h264"}}},
				Binary: &transportv1alpha1.BinaryBinding{MimeTypes: []string{"application/json"}},
			},
		}
		if err := ValidateCodecSupport(binding, baseTransport); err != nil {
			t.Fatalf("expected codec validation to pass, got %v", err)
		}
	})

	t.Run("unsupported audio codec", func(t *testing.T) {
		binding := &transportv1alpha1.TransportBinding{
			Spec: transportv1alpha1.TransportBindingSpec{
				Audio: &transportv1alpha1.AudioBinding{Codecs: []transportv1alpha1.AudioCodec{{Name: "g711"}}},
			},
		}
		if err := ValidateCodecSupport(binding, baseTransport); err == nil {
			t.Fatalf("expected codec validation to fail for unsupported audio codec")
		}
	})

	t.Run("unsupported video codec", func(t *testing.T) {
		binding := &transportv1alpha1.TransportBinding{
			Spec: transportv1alpha1.TransportBindingSpec{
				Video: &transportv1alpha1.VideoBinding{Codecs: []transportv1alpha1.VideoCodec{{Name: "vp9"}}},
			},
		}
		if err := ValidateCodecSupport(binding, baseTransport); err == nil {
			t.Fatalf("expected codec validation to fail for unsupported video codec")
		}
	})

	t.Run("unsupported binary type", func(t *testing.T) {
		binding := &transportv1alpha1.TransportBinding{
			Spec: transportv1alpha1.TransportBindingSpec{
				Binary: &transportv1alpha1.BinaryBinding{MimeTypes: []string{"application/octet-stream"}},
			},
		}
		if err := ValidateCodecSupport(binding, baseTransport); err == nil {
			t.Fatalf("expected codec validation to fail for unsupported binary type")
		}
	})

	t.Run("audio codec sample rate mismatch", func(t *testing.T) {
		transport := &transportv1alpha1.Transport{
			Spec: transportv1alpha1.TransportSpec{
				SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "opus", SampleRateHz: 48000, Channels: 2}},
			},
		}
		binding := &transportv1alpha1.TransportBinding{
			Spec: transportv1alpha1.TransportBindingSpec{
				Audio: &transportv1alpha1.AudioBinding{Codecs: []transportv1alpha1.AudioCodec{{Name: "opus", SampleRateHz: 16000}}},
			},
		}
		if err := ValidateCodecSupport(binding, transport); err == nil {
			t.Fatalf("expected codec validation to fail for sample rate mismatch")
		}
	})

	t.Run("audio codec channel wildcard", func(t *testing.T) {
		transport := &transportv1alpha1.Transport{
			Spec: transportv1alpha1.TransportSpec{
				SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "opus", SampleRateHz: 48000, Channels: 2}},
			},
		}
		binding := &transportv1alpha1.TransportBinding{
			Spec: transportv1alpha1.TransportBindingSpec{
				Audio: &transportv1alpha1.AudioBinding{Codecs: []transportv1alpha1.AudioCodec{{Name: "opus"}}},
			},
		}
		if err := ValidateCodecSupport(binding, transport); err != nil {
			t.Fatalf("expected codec validation to pass with wildcard channels, got %v", err)
		}
	})

	t.Run("video profile mismatch", func(t *testing.T) {
		transport := &transportv1alpha1.Transport{
			Spec: transportv1alpha1.TransportSpec{
				SupportedVideo: []transportv1alpha1.VideoCodec{{Name: "h264", Profile: "baseline"}},
			},
		}
		binding := &transportv1alpha1.TransportBinding{
			Spec: transportv1alpha1.TransportBindingSpec{
				Video: &transportv1alpha1.VideoBinding{Codecs: []transportv1alpha1.VideoCodec{{Name: "h264", Profile: "high"}}},
			},
		}
		if err := ValidateCodecSupport(binding, transport); err == nil {
			t.Fatalf("expected codec validation to fail for profile mismatch")
		}
	})

	t.Run("video profile wildcard", func(t *testing.T) {
		transport := &transportv1alpha1.Transport{
			Spec: transportv1alpha1.TransportSpec{
				SupportedVideo: []transportv1alpha1.VideoCodec{{Name: "h264", Profile: "baseline"}},
			},
		}
		binding := &transportv1alpha1.TransportBinding{
			Spec: transportv1alpha1.TransportBindingSpec{
				Video: &transportv1alpha1.VideoBinding{Codecs: []transportv1alpha1.VideoCodec{{Name: "h264"}}},
			},
		}
		if err := ValidateCodecSupport(binding, transport); err != nil {
			t.Fatalf("expected codec validation to pass with wildcard profile, got %v", err)
		}
	})
}
