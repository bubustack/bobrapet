package config

import (
	"context"

	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/storage"
)

const fallbackMaxRecursionDepth = 32

// EffectiveMaxRecursionDepth returns the configured global recursion budget or
// the process fallback when configuration is nil or unset.
func EffectiveMaxRecursionDepth(cfg *OperatorConfig) int {
	if cfg != nil {
		if depth := cfg.Controller.Engram.EngramControllerConfig.DefaultMaxRecursionDepth; depth > 0 {
			return depth
		}
	}
	return fallbackMaxRecursionDepth
}

// ResolveStoryMaxRecursionDepth returns the effective recursion depth for the Story,
// falling back to the provided global default when the Story does not override it.
func ResolveStoryMaxRecursionDepth(story *v1alpha1.Story, fallback int) int {
	if story != nil && story.Spec.Policy != nil && story.Spec.Policy.Execution != nil && story.Spec.Policy.Execution.MaxRecursionDepth != nil {
		if depth := *story.Spec.Policy.Execution.MaxRecursionDepth; depth > 0 {
			return depth
		}
	}
	return fallback
}

// WithStoryMaxRecursionDepth attaches the effective Story-scoped recursion depth to the context.
func WithStoryMaxRecursionDepth(ctx context.Context, story *v1alpha1.Story, fallback int) context.Context {
	return storage.WithMaxRecursionDepth(ctx, ResolveStoryMaxRecursionDepth(story, fallback))
}
