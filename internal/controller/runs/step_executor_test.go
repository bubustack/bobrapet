package runs

import (
	"context"
	"strings"
	"testing"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	refs "github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/templating"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestDescribeStepRunDrift_NoDrift(t *testing.T) {
	storyRun := &runsv1alpha1.StoryRun{ObjectMeta: metav1.ObjectMeta{Name: "story-run"}}
	story := &bubuv1alpha1.Story{ObjectMeta: metav1.ObjectMeta{Name: "story"}}
	step := &bubuv1alpha1.Step{Name: "step-a", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram"}}}
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "story-run-step-a",
			Namespace: "default",
			Labels: map[string]string{
				contracts.StoryRunLabelKey:  runsidentity.SelectorLabels(storyRun.Name)[contracts.StoryRunLabelKey],
				contracts.StoryNameLabelKey: "story",
			},
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: "story-run"}},
			StepID:      "step-a",
			EngramRef:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram"}},
		},
	}

	if drift := describeStepRunDrift(stepRun, storyRun, story, step); len(drift) != 0 {
		t.Fatalf("expected no drift, got %v", drift)
	}
}

func TestDescribeStepRunDrift_DetectsLabelAndRefChanges(t *testing.T) {
	storyRun := &runsv1alpha1.StoryRun{ObjectMeta: metav1.ObjectMeta{Name: "story-run"}}
	story := &bubuv1alpha1.Story{ObjectMeta: metav1.ObjectMeta{Name: "story"}}
	step := &bubuv1alpha1.Step{Name: "step-a", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram"}}}
	otherNS := "other"
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "story-run-step-a",
			Namespace: "default",
			Labels: map[string]string{
				contracts.StoryRunLabelKey:  "spoofed",
				contracts.StoryNameLabelKey: "story",
			},
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: "story-run"}},
			StepID:      "step-a",
			EngramRef: &refs.EngramReference{ObjectReference: refs.ObjectReference{
				Name:      "engram",
				Namespace: &otherNS,
			}},
		},
	}

	drift := describeStepRunDrift(stepRun, storyRun, story, step)
	if len(drift) == 0 {
		t.Fatalf("expected drift to be detected")
	}
	foundLabelDrift := false
	foundRefDrift := false
	for _, reason := range drift {
		if strings.Contains(reason, contracts.StoryRunLabelKey) {
			foundLabelDrift = true
		}
		if strings.Contains(reason, "EngramRef") {
			foundRefDrift = true
		}
	}
	if !foundLabelDrift || !foundRefDrift {
		t.Fatalf("expected label and EngramRef drift, got %v", drift)
	}
}

func TestEnsureSubStoryRunRejectsInputMismatch(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))

	parent := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "parent-run",
			Namespace: "default",
		},
	}
	target := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "target-story",
			Namespace: "default",
			UID:       types.UID("target-uid"),
		},
	}
	step := &bubuv1alpha1.Step{Name: "child-step"}

	existingInputs := &runtime.RawExtension{Raw: []byte(`{"value":"old"}`)}
	subRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kubeutil.ComposeName(parent.Name, step.Name),
			Namespace: parent.Namespace,
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{
					Name:      target.Name,
					Namespace: &target.Namespace,
				},
				UID: &target.UID,
			},
			Inputs: existingInputs,
		},
	}
	require.NoError(t, controllerutil.SetControllerReference(parent, subRun, scheme))

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(subRun).
		Build()

	executor := &StepExecutor{
		Client: client,
		Scheme: scheme,
	}

	desiredInputs := &runtime.RawExtension{Raw: []byte(`{"value":"new"}`)}
	_, err := executor.ensureSubStoryRun(
		context.Background(),
		parent,
		step,
		target,
		desiredInputs,
		false,
		logging.NewControllerLogger(context.Background(), "test"),
		schedulingDecision{},
	)
	require.Error(t, err)
}

func TestExecuteStoryStepMergesPolicyWith(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))

	parentRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "parent-run",
			Namespace: "default",
		},
	}
	parentStory := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "parent-story",
			Namespace: "default",
		},
	}
	childStory := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-story",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "noop",
				Type: enums.StepTypeCondition,
			}},
			Policy: &bubuv1alpha1.StoryPolicy{
				With: &runtime.RawExtension{Raw: []byte(`{"a":1,"shared":"base"}`)},
			},
		},
	}
	step := &bubuv1alpha1.Step{
		Name: "call-child",
		Type: enums.StepTypeExecuteStory,
		With: &runtime.RawExtension{Raw: []byte(`{"storyRef":{"name":"child-story"},"with":{"b":2,"shared":"override"}}`)},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(parentRun, childStory).
		Build()

	eval, err := templating.New(templating.Config{})
	require.NoError(t, err)
	t.Cleanup(eval.Close)

	executor := &StepExecutor{
		Client:            client,
		Scheme:            scheme,
		TemplateEvaluator: eval,
	}

	err = executor.executeStoryStep(context.Background(), parentRun, parentStory, step, map[string]any{})
	require.NoError(t, err)

	var subRun runsv1alpha1.StoryRun
	subRunName := kubeutil.ComposeName(parentRun.Name, step.Name)
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: subRunName, Namespace: parentRun.Namespace}, &subRun))
	require.NotNil(t, subRun.Spec.Inputs)
	require.JSONEq(t, `{"a":1,"b":2,"shared":"override"}`, string(subRun.Spec.Inputs.Raw))
}

func TestResolveStepRetryPolicyPrefersStepOverride(t *testing.T) {
	stepRetry := &bubuv1alpha1.RetryPolicy{MaxRetries: new(int32(2))}
	storyRetry := &bubuv1alpha1.RetryPolicy{MaxRetries: new(int32(7))}
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Policy: &bubuv1alpha1.StoryPolicy{
				Retries: &bubuv1alpha1.StoryRetries{
					StepRetryPolicy: storyRetry,
				},
			},
		},
	}

	resolved := resolveStepRetryPolicy(stepRetry, story)
	if resolved == nil || resolved.MaxRetries == nil || *resolved.MaxRetries != 2 {
		t.Fatalf("expected step retry to be used, got %+v", resolved)
	}
}

func TestResolveStepRetryPolicyFallsBackToStoryPolicy(t *testing.T) {
	storyRetry := &bubuv1alpha1.RetryPolicy{MaxRetries: new(int32(5))}
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Policy: &bubuv1alpha1.StoryPolicy{
				Retries: &bubuv1alpha1.StoryRetries{
					StepRetryPolicy: storyRetry,
				},
			},
		},
	}

	resolved := resolveStepRetryPolicy(nil, story)
	if resolved == nil || resolved.MaxRetries == nil || *resolved.MaxRetries != 5 {
		t.Fatalf("expected story retry to be used, got %+v", resolved)
	}
	if resolved == storyRetry {
		t.Fatalf("expected story retry policy to be deep copied")
	}
}

func TestResolveStepRetryPolicyNilWhenUnset(t *testing.T) {
	resolved := resolveStepRetryPolicy(nil, &bubuv1alpha1.Story{})
	if resolved != nil {
		t.Fatalf("expected nil retry policy, got %+v", resolved)
	}
}

func TestCreateEngramStepRun_SetsTemplateGeneration(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))

	const templateGeneration int64 = 42

	engramTemplate := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "text-emitter-template",
			Generation: templateGeneration,
		},
		Spec: catalogv1alpha1.EngramTemplateSpec{},
	}
	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "text-emitter",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: "text-emitter-template"},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "default",
			UID:       types.UID("test-uid"),
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-story",
			Namespace: "default",
		},
	}
	step := &bubuv1alpha1.Step{
		Name: "emit",
		Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "text-emitter"}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(engramTemplate, engram).
		Build()

	executor := &StepExecutor{
		Client: k8sClient,
		Scheme: scheme,
	}

	stepName := kubeutil.ComposeName(srun.Name, step.Name)
	err := executor.createEngramStepRun(context.Background(), srun, story, step, stepName, "", nil, nil)
	require.NoError(t, err)

	var created runsv1alpha1.StepRun
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      stepName,
		Namespace: srun.Namespace,
	}, &created))

	require.Equal(t, templateGeneration, created.Spec.TemplateGeneration,
		"StepRun should record EngramTemplate generation")
}

func TestCreateEngramStepRun_TemplateGenerationZeroWhenTemplateMissing(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))

	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "text-emitter",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: "missing-template"},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "default",
			UID:       types.UID("test-uid"),
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-story",
			Namespace: "default",
		},
	}
	step := &bubuv1alpha1.Step{
		Name: "emit",
		Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "text-emitter"}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(engram).
		Build()

	executor := &StepExecutor{
		Client: k8sClient,
		Scheme: scheme,
	}

	stepName := kubeutil.ComposeName(srun.Name, step.Name)
	err := executor.createEngramStepRun(context.Background(), srun, story, step, stepName, "", nil, nil)
	require.NoError(t, err)

	var created runsv1alpha1.StepRun
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      stepName,
		Namespace: srun.Namespace,
	}, &created))

	require.Equal(t, int64(0), created.Spec.TemplateGeneration,
		"StepRun should have zero TemplateGeneration when template is missing")
}

func TestCreateEngramStepRun_IdempotencyKeyTemplate(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))

	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "notify",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: "notify-tpl"},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-abc",
			Namespace: "default",
			UID:       "uid-123",
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-story",
			Namespace: "default",
		},
	}

	tpl := "notify-{{ .inputs.orderId }}-{{ .inputs.customerId }}"
	step := &bubuv1alpha1.Step{
		Name:                   "send-notify",
		Ref:                    &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "notify"}},
		IdempotencyKeyTemplate: &tpl,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(engram).
		Build()

	eval, err := templating.New(templating.Config{})
	require.NoError(t, err)
	t.Cleanup(eval.Close)

	executor := &StepExecutor{
		Client:            k8sClient,
		Scheme:            scheme,
		TemplateEvaluator: eval,
	}

	vars := map[string]any{
		"inputs": map[string]any{
			"orderId":    "order-42",
			"customerId": "cust-7",
		},
		"steps": map[string]any{},
	}

	stepName := kubeutil.ComposeName(srun.Name, step.Name)
	err = executor.createEngramStepRun(context.Background(), srun, story, step, stepName, "", nil, vars)
	require.NoError(t, err)

	var created runsv1alpha1.StepRun
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      stepName,
		Namespace: srun.Namespace,
	}, &created))

	require.Equal(t, "notify-order-42-cust-7", created.Spec.IdempotencyKey,
		"StepRun should use evaluated idempotency key template")
}

func TestCreateEngramStepRun_IdempotencyKeyTemplateFallback(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))

	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "notify",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: "notify-tpl"},
		},
	}
	srun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-abc",
			Namespace: "default",
			UID:       "uid-123",
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-story",
			Namespace: "default",
		},
	}

	// No IdempotencyKeyTemplate set — should use auto-generated key.
	step := &bubuv1alpha1.Step{
		Name: "send-notify",
		Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "notify"}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(engram).
		Build()

	executor := &StepExecutor{
		Client: k8sClient,
		Scheme: scheme,
	}

	stepName := kubeutil.ComposeName(srun.Name, step.Name)
	err := executor.createEngramStepRun(context.Background(), srun, story, step, stepName, "", nil, nil)
	require.NoError(t, err)

	var created runsv1alpha1.StepRun
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      stepName,
		Namespace: srun.Namespace,
	}, &created))

	expected := runsidentity.StepRunIdempotencyKey(srun.Namespace, srun.Name, step.Name)
	require.Equal(t, expected, created.Spec.IdempotencyKey,
		"StepRun should use auto-generated idempotency key when template is not set")
}
