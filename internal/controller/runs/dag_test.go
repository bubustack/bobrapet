package runs

import (
	"context"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/enums"
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

type celNopLogger struct{}

func (celNopLogger) CacheHit(string, string)                              {}
func (celNopLogger) EvaluationStart(string, string)                       {}
func (celNopLogger) EvaluationError(error, string, string, time.Duration) {}
func (celNopLogger) EvaluationSuccess(string, string, time.Duration, any) {}

func TestFindAndLaunchReadyStepsInitializesStepStates(t *testing.T) {
	eval, err := cel.New(celNopLogger{})
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
	ready, skipped, err := reconciler.findAndLaunchReadySteps(
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
