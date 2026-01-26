package loop

import (
	"testing"

	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
)

func TestResolveOverrides_PrefersStepSettings(t *testing.T) {
	storyBatch := int32(5)
	stepBatch := int32(9)
	storyPolicy := &v1alpha1.LoopPolicy{DefaultBatchSize: &storyBatch}
	stepPolicy := &v1alpha1.LoopPolicy{DefaultBatchSize: &stepBatch}
	story := &v1alpha1.Story{
		Spec: v1alpha1.StorySpec{
			Policy: &v1alpha1.StoryPolicy{
				Execution: &v1alpha1.ExecutionPolicy{Loop: storyPolicy},
			},
		},
	}
	step := &v1alpha1.Step{
		Execution: &v1alpha1.ExecutionOverrides{Loop: stepPolicy},
	}

	overrides := ResolveOverrides(story, step)
	if overrides.DefaultBatchSize == nil || *overrides.DefaultBatchSize != int(stepBatch) {
		t.Fatalf("expected step override (%d), got %+v", stepBatch, overrides.DefaultBatchSize)
	}
}

func TestComputeLimits_AppliesOverridesAndClamps(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	cfg.DefaultLoopBatchSize = 50
	cfg.MaxLoopBatchSize = 100
	cfg.MaxLoopConcurrency = 20
	cfg.MaxConcurrencyLimit = 40

	customBatch := 10
	overrides := Overrides{
		DefaultBatchSize:    &customBatch,
		MaxBatchSize:        intPtr(30),
		MaxConcurrency:      intPtr(25),
		MaxConcurrencyLimit: intPtr(15),
	}

	total := 200
	limits := ComputeLimits(cfg, overrides, intPtr(80), intPtr(50), total)

	if limits.BatchSize != 15 {
		t.Fatalf("expected batch size 15 (clamped to concurrency), got %d", limits.BatchSize)
	}
	if limits.MaxConcurrency != 15 {
		t.Fatalf("expected max concurrency 15 (limit clamp), got %d", limits.MaxConcurrency)
	}
}

func intPtr(v int) *int {
	return &v
}
