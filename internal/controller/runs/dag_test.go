/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package runs

import (
	"context"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/storage"
)

func TestApplyManifestPlaceholdersInsertsLengthPlaceholders(t *testing.T) {
	outputs := map[string]any{
		"$bubuStorageRef": "outputs/ref.json",
	}
	length := int64(3)
	manifest := map[string]runsv1alpha1.StepManifestData{
		"tools": {
			Length: &length,
		},
	}

	applyManifestPlaceholders(outputs, manifest)

	val, ok := outputs["tools"]
	if !ok {
		t.Fatalf("expected tools placeholder to be present")
	}
	slice, ok := val.([]any)
	if !ok {
		t.Fatalf("expected tools placeholder to be []any, got %T", val)
	}
	if len(slice) != int(length) {
		t.Fatalf("expected placeholder length %d, got %d", length, len(slice))
	}
}

func TestApplyManifestPlaceholdersAnnotatesRootLength(t *testing.T) {
	outputs := map[string]any{}
	length := int64(7)
	manifest := map[string]runsv1alpha1.StepManifestData{
		manifestRootPath: {
			Length: &length,
		},
	}

	applyManifestPlaceholders(outputs, manifest)

	val, ok := outputs[cel.ManifestLengthKey]
	if !ok {
		t.Fatalf("expected manifest length metadata to be present")
	}
	intVal, ok := val.(int64)
	if !ok {
		t.Fatalf("expected manifest length metadata to be int64, got %T", val)
	}
	if intVal != length {
		t.Fatalf("expected manifest length %d, got %d", length, intVal)
	}
}

func TestApplyManifestPlaceholdersInjectsStorageSelectors(t *testing.T) {
	exists := true
	outputs := map[string]any{
		storage.StorageRefKey: "outputs/ref.json",
	}
	manifest := map[string]runsv1alpha1.StepManifestData{
		"text": {
			Exists: &exists,
		},
	}

	applyManifestPlaceholders(outputs, manifest)

	val, ok := outputs["text"]
	if !ok {
		t.Fatalf("expected selector placeholder to be present")
	}
	selector, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected selector placeholder to be map, got %T", val)
	}
	if selector[storage.StorageRefKey] != "outputs/ref.json" {
		t.Fatalf("expected storage ref to match, got %v", selector[storage.StorageRefKey])
	}
	if selector[storage.StoragePathKey] != "text" {
		t.Fatalf("expected storage path to match, got %v", selector[storage.StoragePathKey])
	}
}

func TestApplyManifestPlaceholdersInjectsHashType(t *testing.T) {
	outputs := map[string]any{}
	manifest := map[string]runsv1alpha1.StepManifestData{
		"summary": {
			Hash: "abc123",
			Type: "string",
		},
	}

	applyManifestPlaceholders(outputs, manifest)

	val, ok := outputs["summary"]
	if !ok {
		t.Fatalf("expected summary placeholder to be present")
	}
	meta, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected summary placeholder to be map, got %T", val)
	}
	if meta[cel.ManifestHashKey] != "abc123" {
		t.Fatalf("expected hash placeholder, got %v", meta[cel.ManifestHashKey])
	}
	if meta[cel.ManifestTypeKey] != "string" {
		t.Fatalf("expected type placeholder, got %v", meta[cel.ManifestTypeKey])
	}
}

type celNopLogger struct{}

func (celNopLogger) CacheHit(string, string)                              {}
func (celNopLogger) EvaluationStart(string, string)                       {}
func (celNopLogger) EvaluationError(error, string, string, time.Duration) {}
func (celNopLogger) EvaluationSuccess(string, string, time.Duration, any) {}

func TestFindAndLaunchReadyStepsInitializesStepStates(t *testing.T) {
	eval, err := cel.New(celNopLogger{}, cel.Config{})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}
	t.Cleanup(eval.Close)

	reconciler := &DAGReconciler{CEL: eval}

	ifCondition := "false"
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "skip-me",
				Type: enums.StepTypeSetData,
				If:   &ifCondition,
			}},
		},
	}

	srun := &runsv1alpha1.StoryRun{}
	ready, skipped, _, err := reconciler.findAndLaunchReadySteps(
		context.Background(),
		srun,
		story,
		map[string]bool{},
		map[string]bool{},
		map[string]any{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ready) != 0 {
		t.Fatalf("expected no ready steps, got %d", len(ready))
	}
	if len(skipped) != 1 {
		t.Fatalf("expected exactly one skipped step, got %d", len(skipped))
	}

	state, ok := srun.Status.StepStates["skip-me"]
	if !ok {
		t.Fatalf("expected skip-me to have a StepState entry")
	}
	if state.Phase != enums.PhaseSkipped {
		t.Fatalf("expected step to be marked skipped, got phase %s", state.Phase)
	}
}

func TestDependencySatisfiedForStreaming(t *testing.T) {
	reconciler := &DAGReconciler{}
	story := &bubuv1alpha1.Story{Spec: bubuv1alpha1.StorySpec{Pattern: enums.StreamingPattern}}
	for _, tc := range []struct {
		name   string
		phase  enums.Phase
		expect bool
	}{
		{"pending", enums.PhasePending, true},
		{"running", enums.PhaseRunning, true},
		{"paused", enums.PhasePaused, true},
		{"succeeded", enums.PhaseSucceeded, true},
		{"failed", enums.PhaseFailed, false},
		{"canceled", enums.PhaseCanceled, false},
		{"empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := runsv1alpha1.StepState{Phase: tc.phase}
			if got := reconciler.dependencySatisfiedForStreaming(story, state); got != tc.expect {
				t.Fatalf("expected %v for phase %s, got %v", tc.expect, tc.phase, got)
			}
		})
	}

	// Non-streaming stories should never short-circuit dependencies.
	batchStory := &bubuv1alpha1.Story{Spec: bubuv1alpha1.StorySpec{Pattern: enums.BatchPattern}}
	if reconciler.dependencySatisfiedForStreaming(batchStory, runsv1alpha1.StepState{Phase: enums.PhaseRunning}) {
		t.Fatalf("expected non-streaming stories to always require completed dependencies")
	}
}

func TestBuildStateMapsIgnoresNonStorySteps(t *testing.T) {
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{Name: "fetch-feeds"},
				{Name: "generate-digest"},
			},
		},
	}

	states := map[string]runsv1alpha1.StepState{
		"fetch-feeds":      {Phase: enums.PhaseSucceeded},
		"generate-digest":  {Phase: enums.PhaseRunning},
		"fetch-feed-batch": {Phase: enums.PhaseSucceeded},
	}

	completed, running, failed := buildStateMaps(story, states)
	if len(failed) != 0 {
		t.Fatalf("expected no failed steps, got %d", len(failed))
	}
	if len(completed) != 1 || !completed["fetch-feeds"] {
		t.Fatalf("expected only fetch-feeds completed, got %v", completed)
	}
	if len(running) != 1 || !running["generate-digest"] {
		t.Fatalf("expected only generate-digest running, got %v", running)
	}
}
