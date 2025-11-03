package v1alpha1

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
)

func TestTransportValidatorRequiresCapabilities(t *testing.T) {
	tv := &TransportValidator{}
	tr := &transportv1alpha1.Transport{
		Spec: transportv1alpha1.TransportSpec{
			Provider: "livekit",
			Driver:   "bobravoz-grpc",
		},
	}
	if err := tv.validateTransport(tr); err == nil {
		t.Fatalf("expected validation error when no capabilities declared")
	}
}

func TestTransportBindingValidatorRejectsMissingTransport(t *testing.T) {
	s := runtime.NewScheme()
	if err := transportv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	validator := &TransportBindingValidator{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
	}
	binding := &transportv1alpha1.TransportBinding{
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "missing",
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
			Audio: &transportv1alpha1.AudioBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
				Codecs:    []transportv1alpha1.AudioCodec{{Name: "opus"}},
			},
		},
	}
	if _, err := validator.validate(context.Background(), binding); err == nil {
		t.Fatalf("expected validation error for missing transport")
	} else if !apierrors.IsInvalid(err) {
		t.Fatalf("expected invalid error, got %v", err)
	}
}

func TestTransportBindingValidatorRejectsMismatchedDriver(t *testing.T) {
	s := runtime.NewScheme()
	if err := transportv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	transport := &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{
			Name: "livekit",
		},
		Spec: transportv1alpha1.TransportSpec{
			Provider:       "livekit",
			Driver:         "bobravoz-grpc",
			SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "opus"}},
		},
	}

	validator := &TransportBindingValidator{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(transport).Build(),
	}

	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "binding",
			Namespace: "default",
		},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "different",
			StepName:     "step-a",
			EngramName:   "engram-a",
			Audio: &transportv1alpha1.AudioBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
				Codecs:    []transportv1alpha1.AudioCodec{{Name: "opus"}},
			},
		},
	}

	if _, err := validator.validate(context.Background(), binding); err == nil {
		t.Fatalf("expected validation error for mismatched driver")
	} else if !apierrors.IsInvalid(err) {
		t.Fatalf("expected invalid error, got %v", err)
	}
}

func TestTransportBindingValidatorRejectsUnsupportedCodec(t *testing.T) {
	s := runtime.NewScheme()
	if err := transportv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	transport := &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{
			Name: "livekit",
		},
		Spec: transportv1alpha1.TransportSpec{
			Provider:       "livekit",
			Driver:         "bobravoz-grpc",
			SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "opus"}},
		},
	}

	validator := &TransportBindingValidator{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(transport).Build(),
	}

	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "binding",
			Namespace: "default",
		},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
			Audio: &transportv1alpha1.AudioBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
				Codecs:    []transportv1alpha1.AudioCodec{{Name: "pcm16"}},
			},
		},
	}

	if _, err := validator.validate(context.Background(), binding); err == nil {
		t.Fatalf("expected validation error for unsupported codec")
	} else if !apierrors.IsInvalid(err) {
		t.Fatalf("expected invalid error, got %v", err)
	}
}
