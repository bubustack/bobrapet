package naming

import "testing"

func TestComposeWithinLimit(t *testing.T) {
	name := Compose("story", "step")
	if name != "story-step" {
		t.Fatalf("expected story-step, got %s", name)
	}
	if len(name) > maxDNS1123Length {
		t.Fatalf("expected length <= %d, got %d", maxDNS1123Length, len(name))
	}
}

func TestComposeTruncatesAndHashes(t *testing.T) {
	longPart := "this-is-a-very-long-name-component-that-will-force-truncation"
	name := Compose(longPart, longPart, longPart)
	if len(name) > maxDNS1123Length {
		t.Fatalf("expected length <= %d, got %d", maxDNS1123Length, len(name))
	}
	// ensure hash suffix present (8 hex chars)
	if len(name) < 9 {
		t.Fatalf("expected hashed suffix, got %s", name)
	}
}
