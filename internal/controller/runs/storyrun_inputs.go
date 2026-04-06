package runs

import (
	"context"
	"encoding/json"
	"fmt"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	runsinputs "github.com/bubustack/bobrapet/pkg/runs/inputs"
	"github.com/bubustack/bobrapet/pkg/storage"
)

func resolveHydratedStoryRunInputs(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
) (map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	resolved, err := runsinputs.ResolveStoryRunInputs(story, storyRun)
	if err != nil {
		return nil, err
	}
	if !containsStorageRef(resolved, 0) {
		return resolved, nil
	}

	manager, err := storage.SharedManager(ctx)
	if err != nil {
		return nil, fmt.Errorf("storyrun inputs reference offloaded data but storage is unavailable: %w", err)
	}
	if manager == nil {
		return nil, fmt.Errorf("storyrun inputs reference offloaded data but storage is unavailable")
	}

	hydrated, err := manager.HydrateStorageRefsOnly(ctx, resolved)
	if err != nil {
		return nil, fmt.Errorf("failed to hydrate offloaded storyrun inputs: %w", err)
	}

	hydratedMap, ok := hydrated.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("hydrated storyrun inputs must be an object, got %T", hydrated)
	}
	return hydratedMap, nil
}

func resolveHydratedStoryRunInputsBytes(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
) ([]byte, error) {
	inputs, err := resolveHydratedStoryRunInputs(ctx, storyRun, story)
	if err != nil {
		return nil, err
	}
	return json.Marshal(inputs)
}
