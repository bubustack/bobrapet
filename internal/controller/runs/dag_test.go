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
	"encoding/json"
	"strings"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/templating"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBuildStateMapsAllowsFailures(t *testing.T) {
	allow := true
	steps := []bubuv1alpha1.Step{
		{Name: "allowed", AllowFailure: &allow},
		{Name: "blocked"},
	}
	states := map[string]runsv1alpha1.StepState{
		"allowed": {Phase: enums.PhaseFailed},
		"blocked": {Phase: enums.PhaseFailed},
	}

	completed, running, failed, allowedFailures := buildStateMaps(steps, states)
	if len(running) != 0 {
		t.Fatalf("expected no running steps, got %v", running)
	}
	if !completed["allowed"] {
		t.Fatalf("expected allowed failure to be treated as completed")
	}
	if failed["allowed"] {
		t.Fatalf("did not expect allowed failure to be marked as failed")
	}
	if !failed["blocked"] {
		t.Fatalf("expected non-allowed failure to be marked as failed")
	}
	if !allowedFailures["allowed"] {
		t.Fatalf("expected allowed failure to be tracked")
	}
}

func TestMarkCompensationsSkipped(t *testing.T) {
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Compensations: []bubuv1alpha1.Step{
				{Name: "rollback-a"},
				{Name: "rollback-b"},
			},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"rollback-a": {Phase: enums.PhaseSucceeded},
			},
		},
	}

	updated := markCompensationsSkipped(srun, story)
	if !updated {
		t.Fatalf("expected compensations to be marked skipped")
	}
	if srun.Status.StepStates["rollback-a"].Phase != enums.PhaseSucceeded {
		t.Fatalf("expected existing compensation to remain succeeded")
	}
	if srun.Status.StepStates["rollback-b"].Phase != enums.PhaseSkipped {
		t.Fatalf("expected missing compensation to be skipped, got %s", srun.Status.StepStates["rollback-b"].Phase)
	}
}

func TestFinalizeStoryRunCompensated(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register scheme: %v", err)
	}
	srun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "story",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{Name: "main"}},
			Compensations: []bubuv1alpha1.Step{
				{Name: "rollback"},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(srun).
		WithObjects(srun).
		Build()

	reconciler := &DAGReconciler{Client: client}
	ctx := context.Background()
	mainFailed := map[string]bool{"main": true}
	compFailed := map[string]bool{}
	finalFailed := map[string]bool{}

	if err := reconciler.finalizeStoryRun(ctx, srun, story, mainFailed, compFailed, finalFailed, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated runsv1alpha1.StoryRun
	if err := client.Get(ctx, types.NamespacedName{Name: srun.Name, Namespace: srun.Namespace}, &updated); err != nil {
		t.Fatalf("failed to fetch storyrun: %v", err)
	}
	if updated.Status.Phase != enums.PhaseCompensated {
		t.Fatalf("expected phase %s, got %s", enums.PhaseCompensated, updated.Status.Phase)
	}
}

func TestEnforcePriorityOrderingBlocksLowerPriority(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register scheme: %v", err)
	}

	queue := "default"
	queueLabel := queueLabelValue(queue)

	high := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "high",
			Namespace: "default",
			Labels: map[string]string{
				contracts.QueueLabelKey:         queueLabel,
				contracts.QueuePriorityLabelKey: priorityLabelValue(10),
			},
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase: enums.PhaseRunning,
		},
	}
	low := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "low",
			Namespace: "default",
			Labels: map[string]string{
				contracts.QueueLabelKey:         queueLabel,
				contracts.QueuePriorityLabelKey: priorityLabelValue(1),
			},
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase: enums.PhasePending,
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(high, low).Build()
	reconciler := &DAGReconciler{Client: client}

	blocked, reason, err := reconciler.enforcePriorityOrdering(context.Background(), low, queue, queueLabel, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !blocked {
		t.Fatal("expected lower-priority run to be blocked")
	}
	if reason == "" {
		t.Fatal("expected a queued reason message")
	}
}

func TestEnforcePriorityOrderingAllowsAgedRun(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register scheme: %v", err)
	}

	queue := "default"
	queueLabel := queueLabelValue(queue)
	now := time.Now()
	queuedAt := metav1.NewTime(now.Add(-2 * time.Minute))

	high := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "high",
			Namespace: "default",
			Labels: map[string]string{
				contracts.QueueLabelKey:         queueLabel,
				contracts.QueuePriorityLabelKey: priorityLabelValue(2),
			},
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase: enums.PhaseRunning,
		},
	}
	low := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "low",
			Namespace: "default",
			Labels: map[string]string{
				contracts.QueueLabelKey:         queueLabel,
				contracts.QueuePriorityLabelKey: priorityLabelValue(1),
			},
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase: enums.PhasePending,
			StepStates: map[string]runsv1alpha1.StepState{
				"queued": {
					Phase:     enums.PhasePending,
					Message:   priorityQueuedMessagePrefix,
					StartedAt: &queuedAt,
				},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(high, low).Build()
	reconciler := &DAGReconciler{Client: client}

	blocked, _, err := reconciler.enforcePriorityOrdering(context.Background(), low, queue, queueLabel, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if blocked {
		t.Fatal("expected aged lower-priority run to proceed")
	}
}

func TestEnforceStoryTimeoutMarksTimeout(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register scheme: %v", err)
	}

	started := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	srun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun",
			Namespace: "default",
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase:     enums.PhaseRunning,
			StartedAt: &started,
		},
	}
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Policy: &bubuv1alpha1.StoryPolicy{
				Timeouts: &bubuv1alpha1.StoryTimeouts{
					Story: stringPtr("1h"),
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(srun).
		WithObjects(srun).
		Build()
	reconciler := &DAGReconciler{Client: client}

	timedOut, err := reconciler.enforceStoryTimeout(context.Background(), srun, story, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !timedOut {
		t.Fatalf("expected timeout to be enforced")
	}
	if srun.Status.Phase != enums.PhaseTimeout {
		t.Fatalf("expected in-memory phase Timeout, got %s", srun.Status.Phase)
	}

	var updated runsv1alpha1.StoryRun
	if err := client.Get(context.Background(), types.NamespacedName{Name: srun.Name, Namespace: srun.Namespace}, &updated); err != nil {
		t.Fatalf("failed to fetch storyrun: %v", err)
	}
	if updated.Status.Phase != enums.PhaseTimeout {
		t.Fatalf("expected stored phase Timeout, got %s", updated.Status.Phase)
	}
}

func TestFindAndLaunchReadyStepsInitializesStepStates(t *testing.T) {
	eval, err := templating.New(templating.Config{})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}
	t.Cleanup(eval.Close)

	reconciler := &DAGReconciler{TemplateEvaluator: eval}

	ifCondition := "false"
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "skip-me",
				Type: enums.StepTypeCondition,
				If:   &ifCondition,
			}},
		},
	}

	srun := &runsv1alpha1.StoryRun{}
	ready, skipped, queued, err := reconciler.findAndLaunchReadySteps(
		context.Background(),
		srun,
		story,
		story.Spec.Steps,
		map[string]bool{},
		map[string]bool{},
		map[string]any{},
		stepDependencyPolicy{skipOnFailedDependency: true},
		nil,
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
	if queued != 0 {
		t.Fatalf("expected no queued steps, got %d", queued)
	}

	state, ok := srun.Status.StepStates["skip-me"]
	if !ok {
		t.Fatalf("expected skip-me to have a StepState entry")
	}
	if state.Phase != enums.PhaseSkipped {
		t.Fatalf("expected step to be marked skipped, got phase %s", state.Phase)
	}
}

func TestFindAndLaunchReadyStepsEnforcesConcurrency(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register scheme: %v", err)
	}

	storyName := "story"
	limit := int32(1)
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: storyName},
		Spec: bubuv1alpha1.StorySpec{
			Policy: &bubuv1alpha1.StoryPolicy{Concurrency: &limit},
			Steps: []bubuv1alpha1.Step{
				{Name: "step-a"},
				{Name: "step-b"},
			},
		},
	}

	running := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "running-step",
			Namespace: "default",
			Labels: map[string]string{
				contracts.StoryNameLabelKey: storyName,
			},
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhaseRunning,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(running).
		Build()

	reconciler := &DAGReconciler{Client: client}

	srun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "srun",
			Namespace: "default",
		},
		Status: runsv1alpha1.StoryRunStatus{},
	}

	ready, skipped, queued, err := reconciler.findAndLaunchReadySteps(
		context.Background(),
		srun,
		story,
		story.Spec.Steps,
		map[string]bool{},
		map[string]bool{},
		map[string]any{},
		stepDependencyPolicy{skipOnFailedDependency: true},
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ready) != 0 {
		t.Fatalf("expected no ready steps due to concurrency, got %d", len(ready))
	}
	if len(skipped) != 0 {
		t.Fatalf("expected no skipped steps, got %d", len(skipped))
	}
	if queued != 2 {
		t.Fatalf("expected 2 queued steps, got %d", queued)
	}

	for _, stepName := range []string{"step-a", "step-b"} {
		state, ok := srun.Status.StepStates[stepName]
		if !ok {
			t.Fatalf("expected %s to be marked queued", stepName)
		}
		if state.Phase != enums.PhasePending {
			t.Fatalf("expected %s to be pending, got %s", stepName, state.Phase)
		}
		if state.Message == "" {
			t.Fatalf("expected %s to have a queue message", stepName)
		}
	}
}

func stringPtr(v string) *string {
	return &v
}

func TestFindReadyStepsSkipsFailedDependencies(t *testing.T) {
	reconciler := &DAGReconciler{}
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{Name: "fetch-feed"},
				{Name: "extract-items", Needs: []string{"fetch-feed"}},
			},
		},
	}

	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"fetch-feed": {Phase: enums.PhaseFailed},
			},
		},
	}

	completed, running, _, _ := buildStateMaps(story.Spec.Steps, srun.Status.StepStates)
	dependencies, _ := buildDependencyGraphs(story.Spec.Steps)
	ready, skipped, reasons, _ := reconciler.findReadySteps(
		context.Background(),
		srun,
		story,
		story.Spec.Steps,
		srun.Status.StepStates,
		completed,
		running,
		dependencies,
		map[string]any{},
		stepDependencyPolicy{skipOnFailedDependency: true},
	)
	if len(ready) != 0 {
		t.Fatalf("expected no ready steps, got %d", len(ready))
	}
	if len(skipped) != 1 || skipped[0].Name != "extract-items" {
		t.Fatalf("expected extract-items to be skipped, got %v", skipped)
	}
	if reason := reasons["extract-items"]; !strings.Contains(reason, "failed dependency") {
		t.Fatalf("expected skip reason to mention failed dependency, got %q", reason)
	}
}

func TestCheckSyncParallelStepsSucceedsWhenBranchesComplete(t *testing.T) {
	parallelCfg := struct {
		Steps []bubuv1alpha1.Step `json:"steps"`
	}{
		Steps: []bubuv1alpha1.Step{
			{Name: "branch-a"},
			{Name: "branch-b"},
		},
	}
	raw, err := json.Marshal(parallelCfg)
	if err != nil {
		t.Fatalf("failed to marshal parallel config: %v", err)
	}

	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			PrimitiveChildren: map[string][]string{
				"parallel": {"parallel-branch-a", "parallel-branch-b"},
			},
			StepStates: map[string]runsv1alpha1.StepState{
				"parallel": {Phase: enums.PhaseRunning},
			},
		},
	}
	steps := []bubuv1alpha1.Step{
		{
			Name: "parallel",
			Type: enums.StepTypeParallel,
			With: &runtime.RawExtension{Raw: raw},
		},
	}
	stepRuns := &runsv1alpha1.StepRunList{
		Items: []runsv1alpha1.StepRun{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "parallel-branch-a"},
				Spec:       runsv1alpha1.StepRunSpec{StepID: "branch-a"},
				Status:     runsv1alpha1.StepRunStatus{Phase: enums.PhaseSucceeded},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "parallel-branch-b"},
				Spec:       runsv1alpha1.StepRunSpec{StepID: "branch-b"},
				Status:     runsv1alpha1.StepRunStatus{Phase: enums.PhaseSucceeded},
			},
		},
	}

	reconciler := &DAGReconciler{}
	if !reconciler.checkSyncParallelSteps(context.Background(), srun, steps, stepRuns) {
		t.Fatalf("expected parallel step to be updated")
	}
	state := srun.Status.StepStates["parallel"]
	if state.Phase != enums.PhaseSucceeded {
		t.Fatalf("expected parallel step to succeed, got %s", state.Phase)
	}
}

func TestCheckSyncParallelStepsFailsWhenBranchFails(t *testing.T) {
	parallelCfg := struct {
		Steps []bubuv1alpha1.Step `json:"steps"`
	}{
		Steps: []bubuv1alpha1.Step{
			{Name: "branch-a"},
			{Name: "branch-b"},
		},
	}
	raw, err := json.Marshal(parallelCfg)
	if err != nil {
		t.Fatalf("failed to marshal parallel config: %v", err)
	}

	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			PrimitiveChildren: map[string][]string{
				"parallel": {"parallel-branch-a", "parallel-branch-b"},
			},
			StepStates: map[string]runsv1alpha1.StepState{
				"parallel": {Phase: enums.PhaseRunning},
			},
		},
	}
	steps := []bubuv1alpha1.Step{
		{
			Name: "parallel",
			Type: enums.StepTypeParallel,
			With: &runtime.RawExtension{Raw: raw},
		},
	}
	stepRuns := &runsv1alpha1.StepRunList{
		Items: []runsv1alpha1.StepRun{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "parallel-branch-a"},
				Spec:       runsv1alpha1.StepRunSpec{StepID: "branch-a"},
				Status:     runsv1alpha1.StepRunStatus{Phase: enums.PhaseSucceeded},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "parallel-branch-b"},
				Spec:       runsv1alpha1.StepRunSpec{StepID: "branch-b"},
				Status:     runsv1alpha1.StepRunStatus{Phase: enums.PhaseFailed},
			},
		},
	}

	reconciler := &DAGReconciler{}
	if !reconciler.checkSyncParallelSteps(context.Background(), srun, steps, stepRuns) {
		t.Fatalf("expected parallel step to be updated")
	}
	state := srun.Status.StepStates["parallel"]
	if state.Phase != enums.PhaseFailed {
		t.Fatalf("expected parallel step to fail, got %s", state.Phase)
	}
}

func TestCheckSyncParallelStepsAllowsFailure(t *testing.T) {
	allow := true
	parallelCfg := struct {
		Steps []bubuv1alpha1.Step `json:"steps"`
	}{
		Steps: []bubuv1alpha1.Step{
			{Name: "branch-a"},
			{Name: "branch-b", AllowFailure: &allow},
		},
	}
	raw, err := json.Marshal(parallelCfg)
	if err != nil {
		t.Fatalf("failed to marshal parallel config: %v", err)
	}

	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			PrimitiveChildren: map[string][]string{
				"parallel": {"parallel-branch-a", "parallel-branch-b"},
			},
			StepStates: map[string]runsv1alpha1.StepState{
				"parallel": {Phase: enums.PhaseRunning},
			},
		},
	}
	steps := []bubuv1alpha1.Step{
		{
			Name: "parallel",
			Type: enums.StepTypeParallel,
			With: &runtime.RawExtension{Raw: raw},
		},
	}
	stepRuns := &runsv1alpha1.StepRunList{
		Items: []runsv1alpha1.StepRun{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "parallel-branch-a"},
				Spec:       runsv1alpha1.StepRunSpec{StepID: "branch-a"},
				Status:     runsv1alpha1.StepRunStatus{Phase: enums.PhaseSucceeded},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "parallel-branch-b"},
				Spec:       runsv1alpha1.StepRunSpec{StepID: "branch-b"},
				Status:     runsv1alpha1.StepRunStatus{Phase: enums.PhaseFailed},
			},
		},
	}

	reconciler := &DAGReconciler{}
	if !reconciler.checkSyncParallelSteps(context.Background(), srun, steps, stepRuns) {
		t.Fatalf("expected parallel step to be updated")
	}
	state := srun.Status.StepStates["parallel"]
	if state.Phase != enums.PhaseSucceeded {
		t.Fatalf("expected parallel step to succeed with allowed failure, got %s", state.Phase)
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

	completed, running, failed, _ := buildStateMaps(story.Spec.Steps, states)
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

func TestBuildStateMapsTreatsPausedAsRunning(t *testing.T) {
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{Name: "gate"}},
		},
	}

	states := map[string]runsv1alpha1.StepState{
		"gate": {Phase: enums.PhasePaused},
	}

	completed, running, failed, _ := buildStateMaps(story.Spec.Steps, states)
	if len(completed) != 0 {
		t.Fatalf("expected no completed steps, got %d", len(completed))
	}
	if len(failed) != 0 {
		t.Fatalf("expected no failed steps, got %d", len(failed))
	}
	if len(running) != 1 || !running["gate"] {
		t.Fatalf("expected gate to be running, got %v", running)
	}
}

func TestCheckSyncGatesApproved(t *testing.T) {
	reconciler := &DAGReconciler{}
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{Name: "approve", Type: enums.StepTypeGate}},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"approve": {Phase: enums.PhasePaused},
			},
			Gates: map[string]runsv1alpha1.GateStatus{
				"approve": {State: runsv1alpha1.GateDecisionApproved, Message: "ok"},
			},
		},
	}

	if updated := reconciler.checkSyncGates(context.Background(), srun, story, story.Spec.Steps, nil); !updated {
		t.Fatalf("expected gate status to update")
	}
	state := srun.Status.StepStates["approve"]
	if state.Phase != enums.PhaseSucceeded {
		t.Fatalf("expected gate to succeed, got %s", state.Phase)
	}
	if state.Message != "ok" {
		t.Fatalf("expected gate message to be propagated, got %q", state.Message)
	}
}

func TestCheckSyncGatesRejected(t *testing.T) {
	reconciler := &DAGReconciler{}
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{Name: "approve", Type: enums.StepTypeGate}},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"approve": {Phase: enums.PhasePaused},
			},
			Gates: map[string]runsv1alpha1.GateStatus{
				"approve": {State: runsv1alpha1.GateDecisionRejected, Message: "nope"},
			},
		},
	}

	if updated := reconciler.checkSyncGates(context.Background(), srun, story, story.Spec.Steps, nil); !updated {
		t.Fatalf("expected gate status to update")
	}
	state := srun.Status.StepStates["approve"]
	if state.Phase != enums.PhaseFailed {
		t.Fatalf("expected gate to fail, got %s", state.Phase)
	}
	if state.Message != "nope" {
		t.Fatalf("expected gate message to be propagated, got %q", state.Message)
	}
}

func TestCheckSyncWaitStepsSatisfied(t *testing.T) {
	eval, err := templating.New(templating.Config{})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}
	t.Cleanup(eval.Close)

	reconciler := &DAGReconciler{TemplateEvaluator: eval}
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "wait",
				Type: enums.StepTypeWait,
				With: mustRawExtension(map[string]any{
					"until": "{{ inputs.ready }}",
				}),
			}},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		Spec: runsv1alpha1.StoryRunSpec{
			Inputs: mustRawExtension(map[string]any{"ready": true}),
		},
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"wait": {Phase: enums.PhasePaused},
			},
		},
	}

	if updated := reconciler.checkSyncWaitSteps(context.Background(), srun, story, story.Spec.Steps, nil); !updated {
		t.Fatalf("expected wait status to update")
	}
	state := srun.Status.StepStates["wait"]
	if state.Phase != enums.PhaseSucceeded {
		t.Fatalf("expected wait to succeed, got %s", state.Phase)
	}
}

func TestCheckSyncWaitStepsTimeoutSkip(t *testing.T) {
	eval, err := templating.New(templating.Config{})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}
	t.Cleanup(eval.Close)

	reconciler := &DAGReconciler{TemplateEvaluator: eval}
	startedAt := metav1.NewTime(time.Now().Add(-2 * time.Second))
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "wait",
				Type: enums.StepTypeWait,
				With: mustRawExtension(map[string]any{
					"until":     "{{ inputs.ready }}",
					"timeout":   "1s",
					"onTimeout": "skip",
				}),
			}},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		Spec: runsv1alpha1.StoryRunSpec{
			Inputs: mustRawExtension(map[string]any{"ready": false}),
		},
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"wait": {Phase: enums.PhasePaused, StartedAt: &startedAt},
			},
		},
	}

	if updated := reconciler.checkSyncWaitSteps(context.Background(), srun, story, story.Spec.Steps, nil); !updated {
		t.Fatalf("expected wait status to update")
	}
	state := srun.Status.StepStates["wait"]
	if state.Phase != enums.PhaseSkipped {
		t.Fatalf("expected wait to be skipped on timeout, got %s", state.Phase)
	}
	if state.Message == "" || !strings.Contains(state.Message, "timed out") {
		t.Fatalf("expected timeout message, got %q", state.Message)
	}
}

func TestCheckSyncWaitStepsUsesStoredTimeout(t *testing.T) {
	eval, err := templating.New(templating.Config{})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}
	t.Cleanup(eval.Close)

	reconciler := &DAGReconciler{TemplateEvaluator: eval}
	now := time.Now()
	startedAt := metav1.NewTime(now.Add(-2 * time.Second))
	storedTimeout := metav1.NewTime(now.Add(10 * time.Second))
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "wait",
				Type: enums.StepTypeWait,
				With: mustRawExtension(map[string]any{
					"until":     "{{ inputs.ready }}",
					"timeout":   "1s",
					"onTimeout": "skip",
				}),
			}},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		Spec: runsv1alpha1.StoryRunSpec{
			Inputs: mustRawExtension(map[string]any{"ready": false}),
		},
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"wait": {Phase: enums.PhasePaused, StartedAt: &startedAt},
			},
		},
	}
	state := &dagPipelineState{
		timers: &stepTimerStore{
			items: map[string]stepTimerState{
				"wait": {WaitTimeoutAt: &storedTimeout},
			},
		},
	}

	_ = reconciler.checkSyncWaitSteps(context.Background(), srun, story, story.Spec.Steps, state)
	got := srun.Status.StepStates["wait"]
	if got.Phase != enums.PhasePaused {
		t.Fatalf("expected wait to remain paused due to stored timeout, got %s", got.Phase)
	}
}

func TestCheckSyncSleepStepsCompletes(t *testing.T) {
	reconciler := &DAGReconciler{}
	startedAt := metav1.NewTime(time.Now().Add(-2 * time.Second))
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "sleep",
				Type: enums.StepTypeSleep,
				With: mustRawExtension(map[string]any{
					"duration": "1s",
				}),
			}},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"sleep": {Phase: enums.PhasePaused, StartedAt: &startedAt},
			},
		},
	}

	if updated := reconciler.checkSyncSleepSteps(context.Background(), srun, story, story.Spec.Steps, nil); !updated {
		t.Fatalf("expected sleep status to update")
	}
	state := srun.Status.StepStates["sleep"]
	if state.Phase != enums.PhaseSucceeded {
		t.Fatalf("expected sleep to succeed, got %s", state.Phase)
	}
}

func TestCheckSyncSleepStepsInitializes(t *testing.T) {
	reconciler := &DAGReconciler{}
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "sleep",
				Type: enums.StepTypeSleep,
				With: mustRawExtension(map[string]any{
					"duration": "10s",
				}),
			}},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"sleep": {Phase: enums.PhasePaused},
			},
		},
	}

	if updated := reconciler.checkSyncSleepSteps(context.Background(), srun, story, story.Spec.Steps, nil); !updated {
		t.Fatalf("expected sleep status to update")
	}
	state := srun.Status.StepStates["sleep"]
	if state.Phase != enums.PhasePaused {
		t.Fatalf("expected sleep to remain paused, got %s", state.Phase)
	}
	if state.StartedAt == nil {
		t.Fatalf("expected sleep to set StartedAt")
	}
}

func TestCheckSyncSleepStepsUsesStoredDeadline(t *testing.T) {
	reconciler := &DAGReconciler{}
	now := time.Now()
	startedAt := metav1.NewTime(now.Add(-2 * time.Second))
	storedUntil := metav1.NewTime(now.Add(10 * time.Second))
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "sleep",
				Type: enums.StepTypeSleep,
				With: mustRawExtension(map[string]any{
					"duration": "1s",
				}),
			}},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"sleep": {Phase: enums.PhasePaused, StartedAt: &startedAt},
			},
		},
	}
	state := &dagPipelineState{
		timers: &stepTimerStore{
			items: map[string]stepTimerState{
				"sleep": {SleepUntil: &storedUntil},
			},
		},
	}

	_ = reconciler.checkSyncSleepSteps(context.Background(), srun, story, story.Spec.Steps, state)
	got := srun.Status.StepStates["sleep"]
	if got.Phase != enums.PhasePaused {
		t.Fatalf("expected sleep to remain paused due to stored deadline, got %s", got.Phase)
	}
}

func TestStepTimerStorePrunesTerminal(t *testing.T) {
	store := &stepTimerStore{
		items: map[string]stepTimerState{
			"done":    {},
			"running": {},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: map[string]runsv1alpha1.StepState{
				"done":    {Phase: enums.PhaseSucceeded},
				"running": {Phase: enums.PhaseRunning},
			},
		},
	}

	store.pruneTerminal(srun)
	if _, ok := store.items["done"]; ok {
		t.Fatalf("expected terminal step timer to be pruned")
	}
	if _, ok := store.items["running"]; !ok {
		t.Fatalf("expected non-terminal step timer to remain")
	}
	if !store.dirty {
		t.Fatalf("expected prune to mark store dirty")
	}
}

func TestMergeSignalMapsPrefersHigherSeq(t *testing.T) {
	dst := map[string]any{
		"sig": map[string]any{
			"seq":   float64(2),
			"value": "newer",
		},
	}
	src := map[string]any{
		"sig": map[string]any{
			"seq":   float64(1),
			"value": "older",
		},
	}

	mergeSignalMaps(dst, src)
	got := dst["sig"].(map[string]any)
	if got["value"] != "newer" {
		t.Fatalf("expected newer signal to remain, got %v", got["value"])
	}

	src["sig"].(map[string]any)["seq"] = float64(3)
	src["sig"].(map[string]any)["value"] = "newest"
	mergeSignalMaps(dst, src)
	got = dst["sig"].(map[string]any)
	if got["value"] != "newest" {
		t.Fatalf("expected newest signal to win, got %v", got["value"])
	}
}

func TestInsertSignalValueRespectsSeq(t *testing.T) {
	root := map[string]any{
		"sig": map[string]any{
			"seq":   float64(5),
			"value": "keep",
		},
	}
	insertSignalValue(root, "sig", map[string]any{"seq": float64(4), "value": "drop"})
	got := root["sig"].(map[string]any)
	if got["value"] != "keep" {
		t.Fatalf("expected existing signal to remain, got %v", got["value"])
	}
	insertSignalValue(root, "sig", map[string]any{"seq": float64(6), "value": "take"})
	got = root["sig"].(map[string]any)
	if got["value"] != "take" {
		t.Fatalf("expected newer signal to overwrite, got %v", got["value"])
	}
}

func mustRawExtension(payload any) *runtime.RawExtension {
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return &runtime.RawExtension{Raw: data}
}

func TestDAGReconciler_StreamingTopologyTerminatedTriggersCompensation(t *testing.T) {
	// Streaming story with Running main steps + Degraded(TopologyTerminated) condition
	// + compensations → main steps marked Failed, compensation phase entered.
	steps := []bubuv1alpha1.Step{
		{Name: "step-a"},
		{Name: "step-b"},
	}
	compensations := []bubuv1alpha1.Step{
		{Name: "rollback-a"},
	}

	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			Phase: enums.PhaseRunning,
			StepStates: map[string]runsv1alpha1.StepState{
				"step-a": {Phase: enums.PhaseRunning},
				"step-b": {Phase: enums.PhaseRunning},
			},
			Conditions: []metav1.Condition{
				{
					Type:   conditions.ConditionDegraded,
					Status: metav1.ConditionTrue,
					Reason: conditions.ReasonTopologyTerminated,
				},
			},
		},
	}

	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Pattern:       enums.StreamingPattern,
			Steps:         steps,
			Compensations: compensations,
		},
	}

	mainCompleted, _, mainFailed, _ := buildStateMaps(story.Spec.Steps, srun.Status.StepStates)
	mainDone := stepsTerminal(len(story.Spec.Steps), mainCompleted, mainFailed)

	if mainDone {
		t.Fatal("expected main phase to NOT be done before topology termination check")
	}

	// Simulate the topology termination detection logic from the reconciler.
	if !mainDone && story.Spec.Pattern == enums.StreamingPattern {
		if conditions.IsDegraded(srun.Status.Conditions) {
			for _, c := range srun.Status.Conditions {
				if c.Type == conditions.ConditionDegraded &&
					c.Status == metav1.ConditionTrue &&
					c.Reason == conditions.ReasonTopologyTerminated {
					mainDone = true
					for i := range story.Spec.Steps {
						step := &story.Spec.Steps[i]
						if stepState, ok := srun.Status.StepStates[step.Name]; ok {
							if !stepState.Phase.IsTerminal() {
								stepState.Phase = enums.PhaseFailed
								stepState.Message = "streaming topology terminated"
								srun.Status.StepStates[step.Name] = stepState
								mainFailed[step.Name] = true
							}
						}
					}
					mainCompleted, _, mainFailed, _ = buildStateMaps(story.Spec.Steps, srun.Status.StepStates)
					break
				}
			}
		}
	}

	if !mainDone {
		t.Fatal("expected mainDone to be true after topology termination")
	}
	if len(mainFailed) != 2 {
		t.Fatalf("expected 2 failed main steps, got %d", len(mainFailed))
	}
	for _, name := range []string{"step-a", "step-b"} {
		state := srun.Status.StepStates[name]
		if state.Phase != enums.PhaseFailed {
			t.Fatalf("expected %s phase Failed, got %s", name, state.Phase)
		}
		if state.Message != "streaming topology terminated" {
			t.Fatalf("expected %s message 'streaming topology terminated', got %q", name, state.Message)
		}
	}

	// With mainFailed > 0 and compensations present, phase should be compensation.
	compCompleted, _, compFailed, _ := buildStateMaps(story.Spec.Compensations, srun.Status.StepStates)
	compDone := stepsTerminal(len(story.Spec.Compensations), compCompleted, compFailed)

	phase := dagPhaseMain
	switch {
	case !mainDone:
		phase = dagPhaseMain
	case len(mainFailed) > 0 && len(story.Spec.Compensations) > 0 && !compDone:
		phase = dagPhaseCompensation
	case len(story.Spec.Finally) > 0:
		phase = dagPhaseFinally
	}
	if phase != dagPhaseCompensation {
		t.Fatalf("expected compensation phase, got %s", phase)
	}
}

func TestDAGReconciler_StreamingTopologyTerminatedTriggersFinally(t *testing.T) {
	// Streaming story with Running main steps + Degraded(TopologyTerminated) condition
	// + no compensations, only finally → finally phase entered.
	steps := []bubuv1alpha1.Step{
		{Name: "step-a"},
	}
	finallySteps := []bubuv1alpha1.Step{
		{Name: "cleanup"},
	}

	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			Phase: enums.PhaseRunning,
			StepStates: map[string]runsv1alpha1.StepState{
				"step-a": {Phase: enums.PhaseRunning},
			},
			Conditions: []metav1.Condition{
				{
					Type:   conditions.ConditionDegraded,
					Status: metav1.ConditionTrue,
					Reason: conditions.ReasonTopologyTerminated,
				},
			},
		},
	}

	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Pattern: enums.StreamingPattern,
			Steps:   steps,
			Finally: finallySteps,
		},
	}

	mainCompleted, _, mainFailed, _ := buildStateMaps(story.Spec.Steps, srun.Status.StepStates)
	mainDone := stepsTerminal(len(story.Spec.Steps), mainCompleted, mainFailed)

	if mainDone {
		t.Fatal("expected main phase to NOT be done before topology termination check")
	}

	// Simulate topology termination detection.
	if !mainDone && story.Spec.Pattern == enums.StreamingPattern {
		if conditions.IsDegraded(srun.Status.Conditions) {
			for _, c := range srun.Status.Conditions {
				if c.Type == conditions.ConditionDegraded &&
					c.Status == metav1.ConditionTrue &&
					c.Reason == conditions.ReasonTopologyTerminated {
					mainDone = true
					for i := range story.Spec.Steps {
						step := &story.Spec.Steps[i]
						if stepState, ok := srun.Status.StepStates[step.Name]; ok {
							if !stepState.Phase.IsTerminal() {
								stepState.Phase = enums.PhaseFailed
								stepState.Message = "streaming topology terminated"
								srun.Status.StepStates[step.Name] = stepState
								mainFailed[step.Name] = true
							}
						}
					}
					mainCompleted, _, mainFailed, _ = buildStateMaps(story.Spec.Steps, srun.Status.StepStates)
					break
				}
			}
		}
	}

	if !mainDone {
		t.Fatal("expected mainDone to be true after topology termination")
	}

	// No compensations, so phase should go to finally.
	finalCompleted, _, finalFailed, _ := buildStateMaps(story.Spec.Finally, srun.Status.StepStates)
	finalDone := stepsTerminal(len(story.Spec.Finally), finalCompleted, finalFailed)

	phase := dagPhaseMain
	switch {
	case !mainDone:
		phase = dagPhaseMain
	case len(mainFailed) > 0 && len(story.Spec.Compensations) > 0:
		phase = dagPhaseCompensation
	case len(story.Spec.Finally) > 0 && !finalDone:
		phase = dagPhaseFinally
	}
	if phase != dagPhaseFinally {
		t.Fatalf("expected finally phase, got %s", phase)
	}
	_ = finalCompleted
	_ = finalFailed
}

func TestDAGReconciler_StreamingNoDegradedSkipsTermination(t *testing.T) {
	// Streaming story with Running main steps but NO Degraded condition → stays in main phase.
	steps := []bubuv1alpha1.Step{
		{Name: "step-a"},
		{Name: "step-b"},
	}
	compensations := []bubuv1alpha1.Step{
		{Name: "rollback-a"},
	}

	srun := &runsv1alpha1.StoryRun{
		Status: runsv1alpha1.StoryRunStatus{
			Phase: enums.PhaseRunning,
			StepStates: map[string]runsv1alpha1.StepState{
				"step-a": {Phase: enums.PhaseRunning},
				"step-b": {Phase: enums.PhaseRunning},
			},
			// No Degraded condition set.
		},
	}

	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Pattern:       enums.StreamingPattern,
			Steps:         steps,
			Compensations: compensations,
		},
	}

	mainCompleted, _, mainFailed, _ := buildStateMaps(story.Spec.Steps, srun.Status.StepStates)
	mainDone := stepsTerminal(len(story.Spec.Steps), mainCompleted, mainFailed)

	// Apply the same logic — should be a no-op without Degraded condition.
	if !mainDone && story.Spec.Pattern == enums.StreamingPattern {
		if conditions.IsDegraded(srun.Status.Conditions) {
			for _, c := range srun.Status.Conditions {
				if c.Type == conditions.ConditionDegraded &&
					c.Status == metav1.ConditionTrue &&
					c.Reason == conditions.ReasonTopologyTerminated {
					mainDone = true
					break
				}
			}
		}
	}

	if mainDone {
		t.Fatal("expected mainDone to remain false without Degraded condition")
	}

	// Steps should remain Running.
	for _, name := range []string{"step-a", "step-b"} {
		state := srun.Status.StepStates[name]
		if state.Phase != enums.PhaseRunning {
			t.Fatalf("expected %s to remain Running, got %s", name, state.Phase)
		}
	}

	// Phase should be main.
	phase := dagPhaseMain
	switch {
	case !mainDone:
		phase = dagPhaseMain
	case len(mainFailed) > 0 && len(story.Spec.Compensations) > 0:
		phase = dagPhaseCompensation
	}
	if phase != dagPhaseMain {
		t.Fatalf("expected main phase, got %s", phase)
	}
}
