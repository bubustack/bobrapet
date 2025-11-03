package identity

import (
	"testing"

	coreidentity "github.com/bubustack/core/runtime/identity"
)

func TestSelectorLabelsReturnsCopy(t *testing.T) {
	const storyRun = "demo-storyrun"
	original := coreidentity.StoryRunSelectorLabels(storyRun)
	copied := SelectorLabels(storyRun)

	if len(copied) != len(original) {
		t.Fatalf("expected %d labels, got %d", len(original), len(copied))
	}
	copied["extra"] = "mutation"
	if copied["extra"] != "mutation" {
		t.Fatalf("expected mutation to stick in copy")
	}
	if _, ok := original["extra"]; ok {
		t.Fatalf("expected original labels map to remain unmodified")
	}
}

func TestEngramRunnerHelpers(t *testing.T) {
	runner := NewEngramRunner("storyrun-a")
	if runner.ServiceAccountName() != ServiceAccountName("storyrun-a") {
		t.Fatalf("ServiceAccountName mismatch")
	}
	subject := runner.Subject("default")
	if subject.Name != runner.ServiceAccountName() {
		t.Fatalf("unexpected subject name %s", subject.Name)
	}
	if subject.Namespace != "default" {
		t.Fatalf("unexpected namespace %s", subject.Namespace)
	}
	if subject.Kind != "ServiceAccount" {
		t.Fatalf("unexpected subject kind %s", subject.Kind)
	}
}
