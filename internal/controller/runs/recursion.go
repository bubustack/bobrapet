package runs

import (
	"context"

	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
)

func withStoryMaxRecursionDepth(ctx context.Context, resolver *config.Resolver, story *v1alpha1.Story) context.Context {
	var cfg *config.OperatorConfig
	if resolver != nil {
		cfg = resolver.GetOperatorConfig()
	}
	return config.WithStoryMaxRecursionDepth(ctx, story, config.EffectiveMaxRecursionDepth(cfg))
}
