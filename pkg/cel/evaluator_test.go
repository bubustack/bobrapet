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

package cel

import (
	"context"
	"errors"
	"testing"
	"time"
)

type nopLogger struct{}

func (nopLogger) CacheHit(string, string)                              {}
func (nopLogger) EvaluationStart(string, string)                       {}
func (nopLogger) EvaluationError(error, string, string, time.Duration) {}
func (nopLogger) EvaluationSuccess(string, string, time.Duration, any) {}

func TestEvaluateWhenConditionHandlesHyphenatedStepNames(t *testing.T) {
	eval, err := New(nopLogger{}, Config{})
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
	eval, err := New(nopLogger{}, Config{})
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

func TestResolveWithInputsRecursivelyEvaluatesNestedValues(t *testing.T) {
	eval, err := New(nopLogger{}, Config{})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}
	defer eval.Close()

	with := map[string]any{
		"room": map[string]any{
			"name": "{{ inputs.room.name }}",
			"meta": map[string]any{
				"sampleRate": "{{ steps[\"ingress\"].outputs.audio.sampleRate }}",
			},
		},
		"trackNames": []any{
			"{{ inputs.primaryTrack }}",
			"agent-playback",
		},
		"unchanged": "static-value",
	}

	vars := map[string]any{
		"inputs": map[string]any{
			"room": map[string]any{
				"name": "demo-room",
			},
			"primaryTrack": "participant-track",
		},
		"steps": map[string]any{
			"ingress": map[string]any{
				"outputs": map[string]any{
					"audio": map[string]any{
						"sampleRate": 48000,
					},
				},
			},
		},
	}

	resolved, err := eval.ResolveWithInputs(context.Background(), with, vars)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}

	room := resolved["room"].(map[string]any)
	if got := room["name"]; got != "demo-room" {
		t.Fatalf("expected room.name to be demo-room, got %v", got)
	}
	meta := room["meta"].(map[string]any)
	if got := meta["sampleRate"]; got != float64(48000) {
		t.Fatalf("expected sampleRate 48000, got %v", got)
	}

	tracks := resolved["trackNames"].([]any)
	if tracks[0] != "participant-track" || tracks[1] != "agent-playback" {
		t.Fatalf("unexpected tracks: %#v", tracks)
	}

	if resolved["unchanged"] != "static-value" {
		t.Fatalf("static string should stay untouched")
	}
}

func TestResolveWithInputsBlocksWhenUpstreamOutputsMissing(t *testing.T) {
	eval, err := New(nopLogger{}, Config{})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}
	defer eval.Close()

	with := map[string]any{
		"audio": map[string]any{
			"data": "{{ steps[\"ingress\"].outputs.audio.data }}",
		},
	}

	vars := map[string]any{
		"inputs": map[string]any{},
		"steps": map[string]any{
			"ingress": map[string]any{
				"outputs": map[string]any{
					"event": map[string]any{
						"type": "bootstrap",
					},
				},
			},
		},
	}

	_, err = eval.ResolveWithInputs(context.Background(), with, vars)
	if err == nil {
		t.Fatalf("expected evaluation to block when upstream data missing")
	}
	var blocked *ErrEvaluationBlocked
	if !errors.As(err, &blocked) {
		t.Fatalf("expected ErrEvaluationBlocked, got %T: %v", err, err)
	}
	if blocked.Reason == "" {
		t.Fatalf("expected blocked error to include reason")
	}
}
