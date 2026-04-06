package transport

import (
	"testing"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
)

func TestDeriveNegotiatedCapabilitiesSetsDefaults(t *testing.T) {
	binding := &transportv1alpha1.TransportBinding{
		Spec: transportv1alpha1.TransportBindingSpec{
			Audio: &transportv1alpha1.AudioBinding{
				Codecs: []transportv1alpha1.AudioCodec{{Name: "pcm16", SampleRateHz: 16000}},
			},
			Video: &transportv1alpha1.VideoBinding{
				Codecs: []transportv1alpha1.VideoCodec{{Name: "h264"}},
			},
			Binary: &transportv1alpha1.BinaryBinding{
				MimeTypes: []string{"application/json"},
			},
		},
	}
	status := &transportv1alpha1.TransportBindingStatus{}

	DeriveNegotiatedCapabilities(binding, status)

	if status.NegotiatedAudio == nil || status.NegotiatedAudio.Name != "pcm16" {
		t.Fatalf("expected negotiated audio to default to first codec, got %#v", status.NegotiatedAudio)
	}
	if status.NegotiatedVideo == nil || status.NegotiatedVideo.Name != "h264" {
		t.Fatalf("expected negotiated video to default to first codec, got %#v", status.NegotiatedVideo)
	}
	if status.NegotiatedBinary != "application/json" {
		t.Fatalf("expected negotiated binary to default to first mime type, got %s", status.NegotiatedBinary)
	}
}

func TestDeriveNegotiatedCapabilitiesPreservesExisting(t *testing.T) {
	existingAudio := &transportv1alpha1.AudioCodec{Name: "opus"}
	existingVideo := &transportv1alpha1.VideoCodec{Name: "vp8"}
	status := &transportv1alpha1.TransportBindingStatus{
		NegotiatedAudio:  existingAudio,
		NegotiatedVideo:  existingVideo,
		NegotiatedBinary: "application/octet-stream",
	}
	binding := &transportv1alpha1.TransportBinding{
		Spec: transportv1alpha1.TransportBindingSpec{
			Audio: &transportv1alpha1.AudioBinding{
				Codecs: []transportv1alpha1.AudioCodec{{Name: "pcm16"}},
			},
			Video: &transportv1alpha1.VideoBinding{
				Codecs: []transportv1alpha1.VideoCodec{{Name: "h264"}},
			},
			Binary: &transportv1alpha1.BinaryBinding{
				MimeTypes: []string{"application/json"},
			},
		},
	}

	DeriveNegotiatedCapabilities(binding, status)

	if status.NegotiatedAudio != existingAudio {
		t.Fatalf("expected existing audio pointer preserved")
	}
	if status.NegotiatedVideo != existingVideo {
		t.Fatalf("expected existing video pointer preserved")
	}
	if status.NegotiatedBinary != "application/octet-stream" {
		t.Fatalf("expected existing binary preserved, got %s", status.NegotiatedBinary)
	}
}
