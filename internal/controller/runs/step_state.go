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
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ensureStepStateTimes sets StartedAt/FinishedAt when missing.
func ensureStepStateTimes(state runsv1alpha1.StepState, now metav1.Time) runsv1alpha1.StepState {
	if state.Phase != "" && state.StartedAt == nil {
		state.StartedAt = &now
	}
	if state.Phase.IsTerminal() && state.FinishedAt == nil {
		state.FinishedAt = &now
	}
	return state
}

// mergeStepState overlays desired fields onto existing without dropping metadata.
func mergeStepState(existing, desired runsv1alpha1.StepState) runsv1alpha1.StepState {
	if desired.Phase != "" {
		existing.Phase = desired.Phase
		existing.Message = desired.Message
	} else if desired.Message != "" {
		existing.Message = desired.Message
	}
	if desired.StartedAt != nil {
		existing.StartedAt = desired.StartedAt
	}
	if desired.FinishedAt != nil {
		existing.FinishedAt = desired.FinishedAt
	}
	if desired.SubStoryRunName != "" {
		existing.SubStoryRunName = desired.SubStoryRunName
	}
	return existing
}
