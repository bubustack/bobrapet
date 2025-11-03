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
	"fmt"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"k8s.io/apimachinery/pkg/runtime"
)

// LoopValidationError captures a field-specific validation failure for loop steps.
type LoopValidationError struct {
	Field   string
	Reason  string
	Message string
}

// ValidateLoopStep inspects a loop-type step for required template fields and
// positive overrides in both the Execution policy and loop with-block.
func ValidateLoopStep(step *bubuv1alpha1.Step) []LoopValidationError {
	if step == nil || step.Type != enums.StepTypeLoop {
		return nil
	}
	var errs []LoopValidationError
	if step.With == nil || len(step.With.Raw) == 0 {
		errs = append(errs, LoopValidationError{
			Field:   "",
			Reason:  conditions.ReasonValidationFailed,
			Message: fmt.Sprintf("loop step '%s' requires a 'with' block", step.Name),
		})
		return errs
	}

	cfg, parseErr := parseLoopWith(step.With)
	if parseErr != nil {
		errs = append(errs, LoopValidationError{
			Field:   ".with",
			Reason:  conditions.ReasonValidationFailed,
			Message: fmt.Sprintf("loop step '%s' has invalid 'with' block: %v", step.Name, parseErr),
		})
		return errs
	}

	if cfg.Template.Ref == nil || cfg.Template.Ref.Name == "" {
		errs = append(errs, LoopValidationError{
			Field:   ".with.template.ref",
			Reason:  conditions.ReasonEngramReferenceInvalid,
			Message: fmt.Sprintf("loop step '%s' must reference an Engram template", step.Name),
		})
	}
	if cfg.Template.Name == "" {
		errs = append(errs, LoopValidationError{
			Field:   ".with.template.name",
			Reason:  conditions.ReasonValidationFailed,
			Message: fmt.Sprintf("loop step '%s' must declare template.name", step.Name),
		})
	}
	if cfg.BatchSize != nil && *cfg.BatchSize <= 0 {
		errs = append(errs, LoopValidationError{
			Field:   ".with.batchSize",
			Reason:  conditions.ReasonValidationFailed,
			Message: "batchSize must be greater than 0 when set",
		})
	}
	if cfg.Concurrency != nil && *cfg.Concurrency <= 0 {
		errs = append(errs, LoopValidationError{
			Field:   ".with.concurrency",
			Reason:  conditions.ReasonValidationFailed,
			Message: "concurrency must be greater than 0 when set",
		})
	}

	if loopPolicy := stepExecutionLoop(step); loopPolicy != nil {
		validatePolicyInt(loopPolicy.DefaultBatchSize, ".execution.loop.defaultBatchSize", &errs)
		validatePolicyInt(loopPolicy.MaxBatchSize, ".execution.loop.maxBatchSize", &errs)
		validatePolicyInt(loopPolicy.MaxConcurrency, ".execution.loop.maxConcurrency", &errs)
		validatePolicyInt(loopPolicy.MaxConcurrencyLimit, ".execution.loop.maxConcurrencyLimit", &errs)
	}

	return errs
}

type loopTemplate struct {
	Name string                `json:"name"`
	Ref  *refs.EngramReference `json:"ref"`
}

type loopWithConfig struct {
	Template    loopTemplate `json:"template"`
	BatchSize   *int         `json:"batchSize,omitempty"`
	Concurrency *int         `json:"concurrency,omitempty"`
}

func parseLoopWith(raw *runtime.RawExtension) (*loopWithConfig, error) {
	cfg := &loopWithConfig{}
	if err := json.Unmarshal(raw.Raw, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func stepExecutionLoop(step *bubuv1alpha1.Step) *bubuv1alpha1.LoopPolicy {
	if step.Execution == nil {
		return nil
	}
	return step.Execution.Loop
}

func validatePolicyInt(value *int32, field string, errs *[]LoopValidationError) {
	if value == nil {
		return
	}
	if *value <= 0 {
		*errs = append(*errs, LoopValidationError{
			Field:   field,
			Reason:  conditions.ReasonValidationFailed,
			Message: fmt.Sprintf("%s must be greater than 0 when set", field),
		})
	}
}
