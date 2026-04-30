package transport

import (
	"context"
	"encoding/json"
	"testing"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/tractatus/envelope"
)

func TestMarshalRuntimeTransportDescriptorsEmitsTypedConfig(t *testing.T) {
	story := &bubuv1alpha1.Story{}
	story.Spec.Transports = []bubuv1alpha1.StoryTransport{{
		Name:         "rt",
		TransportRef: "livekit-default",
	}}
	story.Status.Transports = []bubuv1alpha1.StoryTransportStatus{{
		Name:         "rt",
		TransportRef: "livekit-default",
		Mode:         enums.TransportModeHot,
		ModeReason:   "streaming-default",
	}}

	raw, err := MarshalRuntimeTransportDescriptors(context.Background(), nil, story)
	if err != nil {
		t.Fatalf("MarshalRuntimeTransportDescriptors() error = %v", err)
	}
	var descriptors []envelope.TransportDescriptor
	if err := json.Unmarshal([]byte(raw), &descriptors); err != nil {
		t.Fatalf("unmarshal descriptors: %v", err)
	}
	if len(descriptors) != 1 {
		t.Fatalf("expected 1 descriptor, got %d", len(descriptors))
	}
	typed := descriptors[0].TypedConfig
	if typed == nil {
		t.Fatal("expected typed config")
	}
	if typed.TransportRef != "livekit-default" {
		t.Fatalf("expected transport ref livekit-default, got %q", typed.TransportRef)
	}
	if typed.ModeReason != "streaming-default" {
		t.Fatalf("expected mode reason streaming-default, got %q", typed.ModeReason)
	}
	var rawDescriptors []map[string]any
	if err := json.Unmarshal([]byte(raw), &rawDescriptors); err != nil {
		t.Fatalf("unmarshal raw descriptors: %v", err)
	}
	if _, ok := rawDescriptors[0]["config"]; ok {
		t.Fatal("runtime descriptor must not emit legacy config")
	}
}
