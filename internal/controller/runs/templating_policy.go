package runs

import (
	"strings"

	"github.com/bubustack/bobrapet/internal/config"
)

func resolveOffloadedPolicy(resolver *config.Resolver) string {
	if resolver != nil {
		if cfg := resolver.GetOperatorConfig(); cfg != nil {
			if policy := strings.TrimSpace(cfg.Controller.TemplateOffloadedPolicy); policy != "" {
				return policy
			}
		}
	}
	return config.DefaultControllerConfig().TemplateOffloadedPolicy
}

func shouldBlockOffloaded(policy string) bool {
	return policy == config.TemplatingOffloadedPolicyInject
}
