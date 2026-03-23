package runs

import (
	"strings"

	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/core/contracts"
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

// shouldBlockOffloaded returns true when the policy requires the controller to
// handle offloaded data via pod-based materialization. Only the "inject" policy
// uses materialization pods. The "controller" policy resolves offloaded data
// in-process (hydrate from S3 + evaluate templates in the reconciliation loop).
func shouldBlockOffloaded(policy string) bool {
	return policy == config.TemplatingOffloadedPolicyInject
}

// shouldResolveAllOffloaded returns true when the policy is "controller",
// meaning ALL step input templates referencing offloaded outputs are resolved
// on the controller side via in-process hydration — not just expressions with
// pipe functions.
func shouldResolveAllOffloaded(policy string) bool {
	return policy == config.TemplatingOffloadedPolicyController
}

// storyForcesControllerResolve returns true when the Story's annotations
// request that ALL step input templates be resolved on the controller side
// (via in-process hydration), regardless of whether they use pipe functions.
// Users set the annotation bubustack.io/controller-resolve: "true" on their
// Story to opt in. This per-Story override works independently of the
// operator-level policy.
func storyForcesControllerResolve(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(annotations[contracts.ControllerResolveAnnotation]), "true")
}
