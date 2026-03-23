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

package status

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
)

func TestBuildStoryRunError_BasicFailure(t *testing.T) {
	failedSteps := map[string]bool{"deploy": true}
	stepStates := map[string]runsv1alpha1.StepState{
		"deploy": {Phase: enums.PhaseFailed, Message: "pod crashed"},
	}

	ext := BuildStoryRunError(conditions.ReasonDependencyFailed, "Story completed with failed step: deploy.", failedSteps, stepStates)
	require.NotNil(t, ext)

	var errObj map[string]any
	require.NoError(t, json.Unmarshal(ext.Raw, &errObj))

	assert.Equal(t, conditions.ReasonDependencyFailed, errObj["type"])
	assert.Equal(t, "Story completed with failed step: deploy.", errObj["message"])

	steps, ok := errObj["failedSteps"].([]any)
	require.True(t, ok)
	require.Len(t, steps, 1)

	step := steps[0].(map[string]any)
	assert.Equal(t, "deploy", step["name"])
	assert.Equal(t, string(enums.PhaseFailed), step["phase"])
	assert.Equal(t, "pod crashed", step["message"])
}

func TestBuildStoryRunError_MultipleFailedSteps(t *testing.T) {
	failedSteps := map[string]bool{"step-a": true, "step-b": true}
	stepStates := map[string]runsv1alpha1.StepState{
		"step-a": {Phase: enums.PhaseFailed, Message: "err a"},
		"step-b": {Phase: enums.PhaseTimeout, Message: "timed out"},
	}

	ext := BuildStoryRunError(conditions.ReasonDependencyFailed, "multiple failures", failedSteps, stepStates)
	require.NotNil(t, ext)

	var errObj map[string]any
	require.NoError(t, json.Unmarshal(ext.Raw, &errObj))

	steps, ok := errObj["failedSteps"].([]any)
	require.True(t, ok)
	assert.Len(t, steps, 2)
}

func TestBuildStoryRunError_NoFailedSteps(t *testing.T) {
	ext := BuildStoryRunError(conditions.ReasonTimedOut, "StoryRun exceeded timeout of 5m.", nil, nil)
	require.NotNil(t, ext)

	var errObj map[string]any
	require.NoError(t, json.Unmarshal(ext.Raw, &errObj))

	assert.Equal(t, conditions.ReasonTimedOut, errObj["type"])
	assert.Equal(t, "StoryRun exceeded timeout of 5m.", errObj["message"])
	assert.Nil(t, errObj["failedSteps"], "failedSteps should be omitted when empty")
}

func TestBuildStoryRunError_CompensationFailure(t *testing.T) {
	failedSteps := map[string]bool{"rollback": true}
	stepStates := map[string]runsv1alpha1.StepState{
		"rollback": {Phase: enums.PhaseFailed, Message: "rollback failed"},
	}

	ext := BuildStoryRunError(conditions.ReasonCompensationFailed, "Compensation step rollback failed.", failedSteps, stepStates)
	require.NotNil(t, ext)

	var errObj map[string]any
	require.NoError(t, json.Unmarshal(ext.Raw, &errObj))
	assert.Equal(t, conditions.ReasonCompensationFailed, errObj["type"])

	steps := errObj["failedSteps"].([]any)
	require.Len(t, steps, 1)
	step := steps[0].(map[string]any)
	assert.Equal(t, "rollback", step["name"])
}

func TestBuildStoryRunError_StepStateWithoutMessage(t *testing.T) {
	failedSteps := map[string]bool{"step-x": true}
	stepStates := map[string]runsv1alpha1.StepState{
		"step-x": {Phase: enums.PhaseFailed},
	}

	ext := BuildStoryRunError(conditions.ReasonDependencyFailed, "failure", failedSteps, stepStates)
	require.NotNil(t, ext)

	var errObj map[string]any
	require.NoError(t, json.Unmarshal(ext.Raw, &errObj))

	steps := errObj["failedSteps"].([]any)
	step := steps[0].(map[string]any)
	assert.Equal(t, "step-x", step["name"])
	assert.Equal(t, string(enums.PhaseFailed), step["phase"])
	_, hasMessage := step["message"]
	assert.False(t, hasMessage, "message should be omitted when empty")
}

func TestBuildStoryRunError_FailedStepNotInStepStates(t *testing.T) {
	failedSteps := map[string]bool{"orphan": true}
	stepStates := map[string]runsv1alpha1.StepState{}

	ext := BuildStoryRunError(conditions.ReasonDependencyFailed, "failure", failedSteps, stepStates)
	require.NotNil(t, ext)

	var errObj map[string]any
	require.NoError(t, json.Unmarshal(ext.Raw, &errObj))

	steps := errObj["failedSteps"].([]any)
	step := steps[0].(map[string]any)
	assert.Equal(t, "orphan", step["name"])
	// No phase or message since not in stepStates.
	_, hasPhase := step["phase"]
	assert.False(t, hasPhase)
}
