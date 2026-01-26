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
	"k8s.io/apimachinery/pkg/runtime"
)

func TestValidateLoopStepRequiresTemplate(t *testing.T) {
	step := &bubuv1alpha1.Step{
		Name: "loop",
		Type: enums.StepTypeLoop,
		With: raw(t, map[string]any{
			"template": map[string]any{"name": ""},
		}),
	}
	errs := ValidateLoopStep(step)
	if len(errs) == 0 {
		t.Fatalf("expected errors for missing template ref")
	}
	found := false
	for _, e := range errs {
		if e.Field == ".with.template.ref" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected template ref error, got %+v", errs)
	}
}

func TestValidateLoopStepRejectsNonPositiveOverrides(t *testing.T) {
	zero := int32(0)
	step := &bubuv1alpha1.Step{
		Name: "loop",
		Type: enums.StepTypeLoop,
		Execution: &bubuv1alpha1.ExecutionOverrides{
			Loop: &bubuv1alpha1.LoopPolicy{
				DefaultBatchSize: &zero,
			},
		},
		With: raw(t, map[string]any{
			"template": map[string]any{
				"name": "iter",
				"ref":  map[string]any{"name": "eng"},
			},
		}),
	}
	errs := ValidateLoopStep(step)
	if len(errs) == 0 {
		t.Fatalf("expected defaultBatchSize error")
	}
	found := false
	for _, e := range errs {
		if e.Field == ".execution.loop.defaultBatchSize" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected defaultBatchSize error, got %+v", errs)
	}
}

func raw(t *testing.T, payload any) *runtime.RawExtension {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	return &runtime.RawExtension{Raw: data}
}
