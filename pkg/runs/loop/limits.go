package loop

import (
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
)

// Overrides captures default + maximum loop settings resolved from Story and step policies.
type Overrides struct {
	DefaultBatchSize    *int
	MaxBatchSize        *int
	MaxConcurrency      *int
	MaxConcurrencyLimit *int
}

// Limits represents the computed batch size and concurrency budget for loop executions.
type Limits struct {
	BatchSize      int
	MaxConcurrency int
}

// ResolveOverrides merges Story.Spec.Policy.Execution.Loop and step.Execution.Loop
// (step overrides take precedence).
func ResolveOverrides(story *v1alpha1.Story, step *v1alpha1.Step) Overrides {
	var overrides Overrides
	apply := func(policy *v1alpha1.LoopPolicy) {
		if policy == nil {
			return
		}
		if v := loopIntValue(policy.DefaultBatchSize); v != nil {
			overrides.DefaultBatchSize = v
		}
		if v := loopIntValue(policy.MaxBatchSize); v != nil {
			overrides.MaxBatchSize = v
		}
		if v := loopIntValue(policy.MaxConcurrency); v != nil {
			overrides.MaxConcurrency = v
		}
		if v := loopIntValue(policy.MaxConcurrencyLimit); v != nil {
			overrides.MaxConcurrencyLimit = v
		}
	}
	if story != nil && story.Spec.Policy != nil && story.Spec.Policy.Execution != nil {
		apply(story.Spec.Policy.Execution.Loop)
	}
	if step != nil && step.Execution != nil {
		apply(step.Execution.Loop)
	}
	return overrides
}

// ComputeLimits evaluates the final batch size/max concurrency using controller
// defaults, Story/step overrides, and runtime overrides from the loop config.
func ComputeLimits(
	ctrlCfg *config.ControllerConfig,
	overrides Overrides,
	batchOverride, concurrencyOverride *int,
	total int,
) Limits {
	cfg := controllerConfigOrDefault(ctrlCfg)

	batchSize := resolveBatchSize(cfg, overrides, batchOverride, total)
	maxConcurrency := resolveMaxConcurrency(cfg, overrides, concurrencyOverride, total, batchSize)
	if batchSize > 0 && maxConcurrency < batchSize {
		batchSize = maxConcurrency
	}

	return Limits{
		BatchSize:      batchSize,
		MaxConcurrency: maxConcurrency,
	}
}

func controllerConfigOrDefault(cfg *config.ControllerConfig) *config.ControllerConfig {
	if cfg != nil {
		return cfg
	}
	return config.DefaultControllerConfig()
}

func resolveBatchSize(cfg *config.ControllerConfig, overrides Overrides, batchOverride *int, total int) int {
	batchSize := cfg.DefaultLoopBatchSize
	if overrides.DefaultBatchSize != nil {
		batchSize = *overrides.DefaultBatchSize
	}
	if batchOverride != nil && *batchOverride > 0 {
		batchSize = *batchOverride
	}

	batchSize = normalizeBatchForTotal(batchSize, total)

	maxBatch := cfg.MaxLoopBatchSize
	if overrides.MaxBatchSize != nil {
		maxBatch = *overrides.MaxBatchSize
	}
	return clampBatchToMax(batchSize, maxBatch, total)
}

func normalizeBatchForTotal(batchSize, total int) int {
	if total == 0 {
		return 0
	}
	if batchSize <= 0 || batchSize > total {
		batchSize = total
	}
	if batchSize <= 0 && total > 0 {
		batchSize = 1
	}
	return batchSize
}

func clampBatchToMax(batchSize, maxBatch, total int) int {
	if batchSize > 0 && maxBatch > 0 && batchSize > maxBatch {
		batchSize = maxBatch
	}
	if total > 0 && batchSize > total {
		batchSize = total
	}
	return batchSize
}

func resolveMaxConcurrency(
	cfg *config.ControllerConfig,
	overrides Overrides,
	concurrencyOverride *int,
	total int,
	batchSize int,
) int {
	maxConcurrency := cfg.MaxLoopConcurrency
	if overrides.MaxConcurrency != nil {
		maxConcurrency = *overrides.MaxConcurrency
	}
	if concurrencyOverride != nil && *concurrencyOverride > 0 {
		maxConcurrency = *concurrencyOverride
	}
	if maxConcurrency <= 0 {
		maxConcurrency = batchSize
	}

	maxConcurrencyLimit := cfg.MaxConcurrencyLimit
	if overrides.MaxConcurrencyLimit != nil {
		maxConcurrencyLimit = *overrides.MaxConcurrencyLimit
	}
	if maxConcurrencyLimit > 0 && maxConcurrency > maxConcurrencyLimit {
		maxConcurrency = maxConcurrencyLimit
	}
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	if total > 0 && maxConcurrency > total {
		maxConcurrency = total
	}
	return maxConcurrency
}

func loopIntValue(src *int32) *int {
	if src == nil || *src <= 0 {
		return nil
	}
	v := int(*src)
	return &v
}
