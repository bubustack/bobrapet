/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubeutil

import "testing"

func TestComposeNameWithinLimit(t *testing.T) {
	name := ComposeName("story", "step")
	if name != "story-step" {
		t.Fatalf("expected story-step, got %s", name)
	}
	if len(name) > maxDNS1123Length {
		t.Fatalf("expected length <= %d, got %d", maxDNS1123Length, len(name))
	}
}

func TestComposeNameTruncatesAndHashes(t *testing.T) {
	longPart := "this-is-a-very-long-name-component-that-will-force-truncation"
	name := ComposeName(longPart, longPart, longPart)
	if len(name) > maxDNS1123Length {
		t.Fatalf("expected length <= %d, got %d", maxDNS1123Length, len(name))
	}
	// ensure hash suffix present (8 hex chars)
	if len(name) < 9 {
		t.Fatalf("expected hashed suffix, got %s", name)
	}
}
