package childrun

import (
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
)

func TestUpdateTimeoutAndRetry(t *testing.T) {
	stepRun := &runsv1alpha1.StepRun{}
	maxRetries := int32(3)
	retry := &bubuv1alpha1.RetryPolicy{MaxRetries: &maxRetries}

	timeoutChanged, retryChanged := UpdateTimeoutAndRetry(stepRun, "30s", retry)
	if !timeoutChanged || !retryChanged {
		t.Fatalf("expected both timeout and retry to change")
	}
	if stepRun.Spec.Timeout != "30s" {
		t.Fatalf("unexpected timeout: %s", stepRun.Spec.Timeout)
	}
	if stepRun.Spec.Retry == retry {
		t.Fatalf("expected retry to be deep copied")
	}

	timeoutChanged, retryChanged = UpdateTimeoutAndRetry(stepRun, "30s", retry)
	if timeoutChanged || retryChanged {
		t.Fatalf("expected no changes when values match")
	}
}

func TestUpdateRequestedManifestClone(t *testing.T) {
	manifest := []runsv1alpha1.ManifestRequest{
		{Path: "spec.foo", Operations: []runsv1alpha1.ManifestOperation{"add"}},
	}
	stepRun := &runsv1alpha1.StepRun{}

	changed := UpdateRequestedManifest(&stepRun.Spec.RequestedManifest, manifest, true)
	if !changed {
		t.Fatalf("expected manifest to change on initial set")
	}
	if &stepRun.Spec.RequestedManifest[0] == &manifest[0] {
		t.Fatalf("expected manifest slice to be cloned when clone=true")
	}

	changed = UpdateRequestedManifest(&stepRun.Spec.RequestedManifest, manifest, true)
	if changed {
		t.Fatalf("expected no change when manifest matches")
	}

	changed = UpdateRequestedManifest(&stepRun.Spec.RequestedManifest, nil, true)
	if !changed || len(stepRun.Spec.RequestedManifest) != 0 {
		t.Fatalf("expected manifest to clear when desired nil")
	}
}

func TestUpdateRequestedManifestNoClone(t *testing.T) {
	manifest := []runsv1alpha1.ManifestRequest{
		{Path: "spec.foo", Operations: []runsv1alpha1.ManifestOperation{"add"}},
	}
	stepRun := &runsv1alpha1.StepRun{}

	changed := UpdateRequestedManifest(&stepRun.Spec.RequestedManifest, manifest, false)
	if !changed {
		t.Fatalf("expected manifest to change on initial set")
	}
	if &stepRun.Spec.RequestedManifest[0] != &manifest[0] {
		t.Fatalf("expected manifest slice to be reused when clone=false")
	}
}
