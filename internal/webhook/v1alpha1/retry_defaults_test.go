package v1alpha1

import (
	"testing"

	"github.com/bubustack/bobrapet/internal/config"
)

func TestResolveRetryPolicyHonorsZeroDefault(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	cfg.MaxRetries = 0

	policy := ResolveRetryPolicy(cfg, nil)
	if policy == nil || policy.MaxRetries == nil {
		t.Fatalf("expected non-nil retry policy with max retries")
	}
	if *policy.MaxRetries != 0 {
		t.Fatalf("expected max retries 0, got %d", *policy.MaxRetries)
	}
}
