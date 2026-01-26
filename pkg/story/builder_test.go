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

package story

import (
	"encoding/json"
	"testing"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestBuilderAddStepAndBuild(t *testing.T) {
	builder := New("demo-story", "default")
	builder.Pattern(enums.StreamingPattern)
	concurrency := int32(2)
	builder.Policy(&bubuv1alpha1.StoryPolicy{Concurrency: &concurrency})

	step := bubuv1alpha1.Step{Name: "first"}
	if err := builder.AddStep(step); err != nil {
		t.Fatalf("AddStep error = %v", err)
	}
	story, err := builder.Build()
	if err != nil {
		t.Fatalf("Build error = %v", err)
	}
	if story.Name != "demo-story" || story.Spec.Pattern != enums.StreamingPattern {
		t.Fatalf("unexpected story output: %+v", story)
	}
	if len(story.Spec.Steps) != 1 || story.Spec.Steps[0].Name != "first" {
		t.Fatalf("steps not copied correctly: %+v", story.Spec.Steps)
	}
}

func TestBuilderAddEngramStep(t *testing.T) {
	builder := New("story", "default")
	if err := builder.AddStep(bubuv1alpha1.Step{Name: "bootstrap"}); err != nil {
		t.Fatalf("AddStep bootstrap error = %v", err)
	}

	ref := refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "eng"}}
	err := builder.AddEngramStep("engram-step", ref, []string{"bootstrap"}, map[string]any{"foo": "bar"})
	if err != nil {
		t.Fatalf("AddEngramStep error = %v", err)
	}
	story, err := builder.Build()
	if err != nil {
		t.Fatalf("Build error = %v", err)
	}
	if len(story.Spec.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(story.Spec.Steps))
	}
}

func TestBuilderRejectsInvalidLoopStep(t *testing.T) {
	builder := New("loops", "default")
	step := bubuv1alpha1.Step{
		Name: "fan-out",
		Type: enums.StepTypeLoop,
		With: rawExtension(t, map[string]any{
			"template": map[string]any{"name": "loop"},
		}),
		Execution: &bubuv1alpha1.ExecutionOverrides{
			Loop: &bubuv1alpha1.LoopPolicy{DefaultBatchSize: ptrInt32(0)},
		},
	}
	if err := builder.AddStep(step); err == nil {
		t.Fatalf("expected loop validation error")
	}
}

func TestBuilderAcceptsValidLoopStep(t *testing.T) {
	builder := New("loops", "default")
	valid := bubuv1alpha1.Step{
		Name: "fan-out",
		Type: enums.StepTypeLoop,
		With: rawExtension(t, map[string]any{
			"template": map[string]any{
				"name": "work",
				"ref":  map[string]any{"name": "loop-engram"},
			},
			"batchSize":   2,
			"concurrency": 2,
		}),
		Execution: &bubuv1alpha1.ExecutionOverrides{
			Loop: &bubuv1alpha1.LoopPolicy{DefaultBatchSize: ptrInt32(2)},
		},
	}
	if err := builder.AddStep(valid); err != nil {
		t.Fatalf("AddStep returned unexpected error: %v", err)
	}
}

func rawExtension(t *testing.T, payload any) *runtime.RawExtension {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	return &runtime.RawExtension{Raw: data}
}

func ptrInt32(v int32) *int32 {
	return &v
}
