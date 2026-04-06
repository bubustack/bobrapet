package runs

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
