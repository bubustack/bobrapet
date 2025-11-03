package cel

import (
	"context"
	"testing"
	"time"
)

type nopLogger struct{}

func (nopLogger) CacheHit(string, string)                              {}
func (nopLogger) EvaluationStart(string, string)                       {}
func (nopLogger) EvaluationError(error, string, string, time.Duration) {}
func (nopLogger) EvaluationSuccess(string, string, time.Duration, any) {}

func TestEvaluateWhenConditionHandlesHyphenatedStepNames(t *testing.T) {
	eval, err := New(nopLogger{})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}
	defer eval.Close()

	tools := []any{"issue.create"}
	stepContext := map[string]any{
		"outputs": map[string]any{"tools": tools},
		"output":  map[string]any{"tools": tools},
	}

	vars := map[string]any{
		"inputs": map[string]any{},
		"steps": map[string]any{
			"list-tools": stepContext,
			"list_tools": stepContext,
		},
	}

	ok, err := eval.EvaluateWhenCondition(context.Background(), `len(steps.list-tools.output.tools) > 0`, vars)
	if err != nil {
		t.Fatalf("unexpected evaluation error: %v", err)
	}
	if !ok {
		t.Fatalf("expected condition to evaluate to true")
	}
}

func TestLenUsesManifestLengthMetadata(t *testing.T) {
	eval, err := New(nopLogger{})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}
	defer eval.Close()

	stepContext := map[string]any{
		"outputs": map[string]any{
			ManifestLengthKey: int64(4),
		},
		"output": map[string]any{
			ManifestLengthKey: int64(4),
		},
	}

	vars := map[string]any{
		"inputs": map[string]any{},
		"steps": map[string]any{
			"collect-data": stepContext,
			"collect_data": stepContext,
		},
	}

	ok, err := eval.EvaluateWhenCondition(context.Background(), `len(steps.collect-data.output) == 4`, vars)
	if err != nil {
		t.Fatalf("unexpected evaluation error: %v", err)
	}
	if !ok {
		t.Fatalf("expected manifest-backed length to evaluate to true")
	}
}
