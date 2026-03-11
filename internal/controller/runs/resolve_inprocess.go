package runs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/core/templating"
	"github.com/go-logr/logr"
)

// hydrateVarsInProcess deep-copies the steps map, filters to only steps
// referenced by the given expression string, hydrates storage refs from S3,
// and returns a new vars map with the hydrated steps. This is the shared
// foundation for both map and condition in-process resolution.
func hydrateVarsInProcess(ctx context.Context, expr string, vars map[string]any) (map[string]any, error) {
	// Fail fast if storage is not available — without it hydration is impossible.
	manager, err := storage.SharedManager(ctx)
	if err != nil || manager == nil {
		return nil, fmt.Errorf("storage manager unavailable; cannot hydrate offloaded data in-process: %w", err)
	}

	stepsMap, _ := vars["steps"].(map[string]any)
	hydratedSteps, err := deepCopyStepsMap(stepsMap)
	if err != nil {
		return nil, fmt.Errorf("failed to copy steps map for in-process hydration: %w", err)
	}

	// Filter to only the steps referenced by the expression to minimize S3 fetches.
	filtered := filterStepsForTemplate(expr, hydratedSteps)

	// Hydrate: replace storage refs with actual data from S3.
	hydratePriorOutputs(ctx, filtered, nil)

	// Post-hydration check: verify storage refs were resolved for referenced steps.
	if detectOffloadedInTemplateString(expr, filtered) != nil {
		return nil, fmt.Errorf("in-process hydration incomplete: some storage refs could not be resolved from storage")
	}

	// Build new vars with hydrated steps overlaid on originals.
	hydratedVars := make(map[string]any, len(vars))
	for k, v := range vars {
		hydratedVars[k] = v
	}
	mergedSteps := make(map[string]any, len(stepsMap))
	for k, v := range stepsMap {
		mergedSteps[k] = v
	}
	for k, v := range filtered {
		mergedSteps[k] = v
	}
	hydratedVars["steps"] = mergedSteps

	logr.FromContextOrDiscard(ctx).Info(
		"Hydrated offloaded step outputs in-process",
		"hydratedSteps", len(filtered),
	)

	return hydratedVars, nil
}

// resolveOffloadedInProcess hydrates offloaded step outputs from storage (S3)
// directly in the controller process and evaluates Go templates with the
// hydrated data. This avoids spawning a materialize StepRun pod.
// Used for step "with" blocks and story output templates (map evaluation).
func resolveOffloadedInProcess(
	ctx context.Context,
	evaluator *templating.Evaluator,
	withBlock map[string]any,
	vars map[string]any,
) (map[string]any, error) {
	if evaluator == nil {
		return nil, fmt.Errorf("template evaluator is nil; cannot resolve in-process")
	}

	withJSON, err := json.Marshal(withBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal with block for step filtering: %w", err)
	}

	hydratedVars, err := hydrateVarsInProcess(ctx, string(withJSON), vars)
	if err != nil {
		return nil, err
	}

	return evaluator.ResolveWithInputs(ctx, withBlock, hydratedVars)
}

// resolveConditionInProcess hydrates offloaded step outputs from storage (S3)
// directly in the controller process and evaluates a condition template with
// the hydrated data. Used for "if" and "wait" condition expressions.
func resolveConditionInProcess(
	ctx context.Context,
	evaluator *templating.Evaluator,
	condition string,
	vars map[string]any,
) (bool, error) {
	if evaluator == nil {
		return false, fmt.Errorf("template evaluator is nil; cannot resolve condition in-process")
	}

	hydratedVars, err := hydrateVarsInProcess(ctx, condition, vars)
	if err != nil {
		return false, err
	}

	return evaluator.EvaluateCondition(ctx, condition, hydratedVars)
}

// deepCopyStepsMap returns a deep copy of a map[string]any via JSON roundtrip.
// Only supports JSON-serializable types (maps, slices, strings, numbers, bools).
func deepCopyStepsMap(src map[string]any) (map[string]any, error) {
	if src == nil {
		return nil, nil
	}
	data, err := json.Marshal(src)
	if err != nil {
		return nil, err
	}
	var dst map[string]any
	if err := json.Unmarshal(data, &dst); err != nil {
		return nil, err
	}
	return dst, nil
}
