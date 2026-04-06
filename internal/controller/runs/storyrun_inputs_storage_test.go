package runs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/core/contracts"
)

func TestResolveRunScopedInputsHydratesOffloadedStoryRunInputs(t *testing.T) {
	ctx := context.Background()
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-crash-analyzer-run",
			Namespace: "pod-crash-notifier",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			Inputs: dehydrateStoryRunInputsForTest(t, ctx, map[string]any{
				"object": map[string]any{
					"metadata": map[string]any{
						"name":      "crasher",
						"namespace": "default",
					},
					"status": map[string]any{
						"phase":   "Failed",
						"details": strings.Repeat("x", 512),
					},
				},
			}),
		},
	}

	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "analyze",
			Namespace: "pod-crash-notifier",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "analyze",
			Input:  &runtime.RawExtension{Raw: []byte(`{"podName":"{{ inputs.object.metadata.name }}","phase":"{{ inputs.object.status.phase }}"}`)},
		},
	}

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			Scheme: scheme,
		},
	}

	resolved, err := reconciler.resolveRunScopedInputs(ctx, step, storyRun, nil, nil, "")
	require.NoError(t, err)
	require.JSONEq(t, `{"podName":"crasher","phase":"Failed"}`, string(resolved))
}

func TestValidateStoryRunInputsHydratesOffloadedStoryRunInputs(t *testing.T) {
	ctx := context.Background()
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-crash-analyzer-run",
			Namespace: "pod-crash-notifier",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			Inputs: dehydrateStoryRunInputsForTest(t, ctx, map[string]any{
				"object": map[string]any{
					"metadata": map[string]any{
						"name":      "crasher",
						"namespace": "default",
					},
					"status": map[string]any{
						"details": strings.Repeat("x", 512),
					},
				},
			}),
		},
	}

	schema, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"object": map[string]any{
				"type":     "object",
				"required": []any{"metadata"},
				"properties": map[string]any{
					"metadata": map[string]any{
						"type":     "object",
						"required": []any{"name", "namespace"},
						"properties": map[string]any{
							"name":      map[string]any{"type": "string"},
							"namespace": map[string]any{"type": "string"},
						},
					},
				},
			},
		},
		"required": []any{"object"},
	})
	require.NoError(t, err)

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-crash-analyzer",
			Namespace: "pod-crash-notifier",
		},
		Spec: bubuv1alpha1.StorySpec{
			InputsSchema: &runtime.RawExtension{Raw: schema},
		},
	}

	handled, err := (&StoryRunReconciler{}).validateStoryRunInputs(ctx, storyRun, story, logging.NewControllerLogger(ctx, "storyrun-test"))
	require.NoError(t, err)
	require.False(t, handled)
}

func TestValidateStoryRunInputsHonorsStoryMaxRecursionDepth(t *testing.T) {
	ctx := context.Background()
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-crash-analyzer-run",
			Namespace: "pod-crash-notifier",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			Inputs: dehydrateStoryRunInputsForTest(t, ctx, map[string]any{
				"object": deeplyNestedMap(12, "leaf", map[string]any{
					"name":      "crasher",
					"namespace": "default",
				}),
			}),
		},
	}

	schema, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"object": map[string]any{"type": "object"},
		},
		"required": []any{"object"},
	})
	require.NoError(t, err)

	maxRecursionDepth := 32
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-crash-analyzer",
			Namespace: "pod-crash-notifier",
		},
		Spec: bubuv1alpha1.StorySpec{
			InputsSchema: &runtime.RawExtension{Raw: schema},
			Policy: &bubuv1alpha1.StoryPolicy{
				Execution: &bubuv1alpha1.ExecutionPolicy{
					MaxRecursionDepth: &maxRecursionDepth,
				},
			},
		},
	}

	handled, err := (&StoryRunReconciler{}).validateStoryRunInputs(ctx, storyRun, story, logging.NewControllerLogger(ctx, "storyrun-test"))
	require.NoError(t, err)
	require.False(t, handled)
}

func dehydrateStoryRunInputsForTest(t *testing.T, ctx context.Context, inputs map[string]any) *runtime.RawExtension {
	t.Helper()

	storage.ResetSharedManagerCacheForTests()
	t.Cleanup(storage.ResetSharedManagerCacheForTests)

	t.Setenv(contracts.StorageProviderEnv, "file")
	t.Setenv(contracts.StoragePathEnv, t.TempDir())
	t.Setenv(contracts.MaxInlineSizeEnv, "128")

	manager, err := storage.NewManager(ctx)
	require.NoError(t, err)

	dehydrated, err := manager.DehydrateInputs(ctx, inputs, "storyrun-inputs")
	require.NoError(t, err)
	require.True(t, containsStorageRef(dehydrated, 0))

	raw, err := json.Marshal(dehydrated)
	require.NoError(t, err)
	return &runtime.RawExtension{Raw: raw}
}

func deeplyNestedMap(depth int, key string, leaf any) map[string]any {
	result := map[string]any{key: leaf}
	for i := depth; i >= 1; i-- {
		result = map[string]any{fmt.Sprintf("level%d", i): result}
	}
	return result
}
