package runs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestComputeManifestRequestsHandlesHyphenatedSteps(t *testing.T) {
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{Name: "list-tools", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "dummy"}}},
				{Name: "create-issue", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "dummy"}},
					If: ptrString("len(steps.list-tools.output.tools) > 0"),
				},
			},
		},
	}

	var executor StepExecutor
	requests := executor.computeManifestRequests(story, "list-tools")
	if len(requests) == 0 {
		t.Fatalf("expected manifest requests for create-issue, got none")
	}

	found := false
	for _, req := range requests {
		if req.Path == "output.tools" || req.Path == "tools" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected manifest request for output.tools, got %#v", requests)
	}
}

func TestExecuteStoryStepCreatesSubStoryRunAndWaitsForCompletion(t *testing.T) {
	env := newExecuteStoryTestEnv(t, "child-story", "parent-run")
	step := env.newExecuteStoryStep("invoke-sub", true, map[string]any{"foo": "{{ inputs.foo }}"})
	if err := env.run(step, env.vars(map[string]any{"foo": "bar"})); err != nil {
		t.Fatalf("executeStoryStep returned error: %v", err)
	}

	state := env.stepState("invoke-sub")
	if state.Phase != enums.PhaseRunning {
		t.Fatalf("expected phase Running, got %s", state.Phase)
	}
	if state.SubStoryRunName != "parent-run-invoke-sub" {
		t.Fatalf("expected subStoryRunName 'parent-run-invoke-sub', got %q", state.SubStoryRunName)
	}

	subRun := env.assertSubRunMetadata(state.SubStoryRunName)
	env.assertSubRunInputs(subRun, map[string]any{"foo": "bar"})
}

func TestExecuteStoryStepAsyncMarksSucceeded(t *testing.T) {
	env := newExecuteStoryTestEnv(t, "async-child", "parent-async")
	step := env.newExecuteStoryStep("fire-and-forget", false, nil)
	if err := env.run(step, env.vars(nil)); err != nil {
		t.Fatalf("executeStoryStep returned error: %v", err)
	}

	state := env.stepState("fire-and-forget")
	if state.Phase != enums.PhaseSucceeded {
		t.Fatalf("expected phase Succeeded for async executeStory, got %s", state.Phase)
	}
	if state.SubStoryRunName != "parent-async-fire-and-forget" {
		t.Fatalf("expected SubStoryRunName to be set, got %q", state.SubStoryRunName)
	}
}

func TestMergeWithBlocksDeepMergesNestedObjects(t *testing.T) {
	executor := StepExecutor{}

	engramWith := &runtime.RawExtension{Raw: []byte(`{"auth":{"headers":{"x-api-key":"default"},"retry":3},"timeout":10}`)}
	stepWith := &runtime.RawExtension{Raw: []byte(`{"auth":{"headers":{"x-trace-id":"abc"}},"timeout":20}`)}

	merged, err := executor.mergeWithBlocks(engramWith, stepWith)
	if err != nil {
		t.Fatalf("mergeWithBlocks returned error: %v", err)
	}
	var mergedMap map[string]any
	if err := json.Unmarshal(merged.Raw, &mergedMap); err != nil {
		t.Fatalf("failed to unmarshal merged result: %v", err)
	}
	auth, ok := mergedMap["auth"].(map[string]any)
	if !ok {
		t.Fatalf("expected auth map, got %#v", mergedMap["auth"])
	}
	headers, ok := auth["headers"].(map[string]any)
	if !ok {
		t.Fatalf("expected headers map, got %#v", auth["headers"])
	}
	if headers["x-api-key"] != "default" {
		t.Fatalf("expected to preserve default api key, got %#v", headers["x-api-key"])
	}
	if headers["x-trace-id"] != "abc" {
		t.Fatalf("expected to apply override header, got %#v", headers["x-trace-id"])
	}
	if auth["retry"] != float64(3) {
		t.Fatalf("expected to keep retry value, got %#v", auth["retry"])
	}
	if mergedMap["timeout"] != float64(20) {
		t.Fatalf("expected timeout override 20, got %#v", mergedMap["timeout"])
	}
}

func TestExecuteEngramStepSetsTimeoutAndRetryFromStep(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(runsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(bubuv1alpha1.AddToScheme(scheme))

	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: "worker-template"},
		},
	}

	step := bubuv1alpha1.Step{
		Name: "process",
		Ref: &refs.EngramReference{
			ObjectReference: refs.ObjectReference{Name: engram.Name},
		},
		Execution: &bubuv1alpha1.ExecutionOverrides{
			Timeout: ptrString("45s"),
			Retry: &bubuv1alpha1.RetryPolicy{
				MaxRetries: ptrInt32(5),
			},
		},
	}

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-story",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{step},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(engram).
		Build()

	executor := StepExecutor{
		Client: fakeClient,
		Scheme: scheme,
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-1",
			Namespace: "default",
		},
		Status: runsv1alpha1.StoryRunStatus{
			StepStates: make(map[string]runsv1alpha1.StepState),
		},
	}

	if err := executor.executeEngramStep(context.Background(), storyRun, story, &story.Spec.Steps[0]); err != nil {
		t.Fatalf("executeEngramStep returned error: %v", err)
	}

	var created runsv1alpha1.StepRun
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "run-1-process", Namespace: "default"}, &created); err != nil {
		t.Fatalf("failed to fetch created StepRun: %v", err)
	}

	if created.Spec.Timeout != "45s" {
		t.Fatalf("expected timeout '45s', got %q", created.Spec.Timeout)
	}
	if created.Spec.Retry == nil || created.Spec.Retry.MaxRetries == nil || *created.Spec.Retry.MaxRetries != 5 {
		t.Fatalf("expected retry maxRetries 5, got %#v", created.Spec.Retry)
	}
}

type nopLogger struct{}

func (nopLogger) CacheHit(string, string)                              {}
func (nopLogger) EvaluationStart(string, string)                       {}
func (nopLogger) EvaluationError(error, string, string, time.Duration) {}
func (nopLogger) EvaluationSuccess(string, string, time.Duration, any) {}

func ptrString(v string) *string { return &v }

func ptrInt32(v int32) *int32 { return &v }

type executeStoryTestEnv struct {
	t              *testing.T
	ctx            context.Context
	eval           *cel.Evaluator
	client         client.Client
	executor       StepExecutor
	storyRun       *runsv1alpha1.StoryRun
	childStoryName string
	childStoryUID  string
}

func newExecuteStoryTestEnv(t *testing.T, childStoryName, storyRunName string) *executeStoryTestEnv {
	t.Helper()

	ctx := context.Background()
	eval, err := cel.New(nopLogger{})
	if err != nil {
		t.Fatalf("failed to create evaluator: %v", err)
	}
	t.Cleanup(func() { eval.Close() })

	scheme := runtime.NewScheme()
	utilruntime.Must(runsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(bubuv1alpha1.AddToScheme(scheme))

	childUID := types.UID(childStoryName + "-uid")
	childStory := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childStoryName,
			Namespace: "default",
		},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{Name: "noop", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "dummy"}}},
			},
		},
	}
	childStory.UID = childUID

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(childStory).Build()

	executor := StepExecutor{
		Client: cl,
		Scheme: scheme,
		CEL:    eval,
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      storyRunName,
			Namespace: "default",
		},
		Status: runsv1alpha1.StoryRunStatus{StepStates: make(map[string]runsv1alpha1.StepState)},
	}
	storyRun.UID = types.UID(storyRunName + "-uid")

	return &executeStoryTestEnv{
		t:              t,
		ctx:            ctx,
		eval:           eval,
		client:         cl,
		executor:       executor,
		storyRun:       storyRun,
		childStoryName: childStoryName,
		childStoryUID:  string(childUID),
	}
}

func (e *executeStoryTestEnv) vars(inputs map[string]any) map[string]any {
	e.t.Helper()
	if inputs == nil {
		inputs = map[string]any{}
	}
	return map[string]any{
		"inputs": inputs,
		"steps":  map[string]any{},
	}
}

func (e *executeStoryTestEnv) newExecuteStoryStep(stepName string, waitForCompletion bool, with map[string]any) *bubuv1alpha1.Step {
	e.t.Helper()
	payload := map[string]any{
		"storyRef": map[string]any{"name": e.childStoryName},
	}
	if !waitForCompletion {
		payload["waitForCompletion"] = false
	}
	if with != nil {
		payload["with"] = with
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		e.t.Fatalf("failed to marshal executeStory payload: %v", err)
	}
	return &bubuv1alpha1.Step{
		Name: stepName,
		Type: enums.StepTypeExecuteStory,
		With: &runtime.RawExtension{Raw: raw},
	}
}

func (e *executeStoryTestEnv) run(step *bubuv1alpha1.Step, vars map[string]any) error {
	e.t.Helper()
	return e.executor.Execute(e.ctx, e.storyRun, &bubuv1alpha1.Story{}, step, vars)
}

func (e *executeStoryTestEnv) stepState(stepName string) runsv1alpha1.StepState {
	e.t.Helper()
	state, ok := e.storyRun.Status.StepStates[stepName]
	if !ok {
		e.t.Fatalf("expected step state for %s to be set", stepName)
	}
	return state
}

func (e *executeStoryTestEnv) fetchSubRun(name string) *runsv1alpha1.StoryRun {
	e.t.Helper()
	var sub runsv1alpha1.StoryRun
	if err := e.client.Get(e.ctx, types.NamespacedName{Name: name, Namespace: e.storyRun.Namespace}, &sub); err != nil {
		e.t.Fatalf("failed to fetch sub-StoryRun %s: %v", name, err)
	}
	return &sub
}

func (e *executeStoryTestEnv) assertSubRunMetadata(name string) *runsv1alpha1.StoryRun {
	e.t.Helper()
	subRun := e.fetchSubRun(name)
	if subRun.Spec.StoryRef.Name != e.childStoryName {
		e.t.Fatalf("expected sub-StoryRun to reference %s, got %s", e.childStoryName, subRun.Spec.StoryRef.Name)
	}
	if subRun.Spec.StoryRef.UID == nil || string(*subRun.Spec.StoryRef.UID) != e.childStoryUID {
		e.t.Fatalf("expected sub-StoryRun StoryRef UID to be %s, got %#v", e.childStoryUID, subRun.Spec.StoryRef.UID)
	}
	if len(subRun.OwnerReferences) != 1 || subRun.OwnerReferences[0].Name != e.storyRun.Name {
		e.t.Fatalf("expected sub-StoryRun to have parent owner reference")
	}
	return subRun
}

func (e *executeStoryTestEnv) assertSubRunInputs(subRun *runsv1alpha1.StoryRun, expected map[string]any) {
	e.t.Helper()
	if subRun.Spec.Inputs == nil {
		e.t.Fatalf("expected sub-StoryRun inputs to be set")
	}
	var inputs map[string]any
	if err := json.Unmarshal(subRun.Spec.Inputs.Raw, &inputs); err != nil {
		e.t.Fatalf("failed to unmarshal sub-StoryRun inputs: %v", err)
	}
	for key, want := range expected {
		got, ok := inputs[key]
		if !ok {
			e.t.Fatalf("expected propagated input %q to be present", key)
		}
		if got != want {
			e.t.Fatalf("expected propagated input %q=%#v, got %#v", key, want, got)
		}
	}
}
