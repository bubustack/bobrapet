package runs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
)

func TestBuildBaseEnvVarsUsesStoryMaxRecursionDepth(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	manager, err := config.NewOperatorConfigManager(cl, "default", "bobrapet-operator-config")
	require.NoError(t, err)

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	maxRecursionDepth := 48
	story := &v1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-crash-analyzer",
			Namespace: "pod-crash-notifier",
		},
		Spec: v1alpha1.StorySpec{
			Policy: &v1alpha1.StoryPolicy{
				Execution: &v1alpha1.ExecutionPolicy{
					MaxRecursionDepth: &maxRecursionDepth,
				},
			},
		},
	}
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-crash-analyzer-run-analyze",
			Namespace: "pod-crash-notifier",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "analyze",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{
					Name: "pod-crash-analyzer-run",
				},
			},
		},
	}

	envVars := reconciler.buildBaseEnvVars(stepRun, story, []byte(`{}`), time.Minute, "batch", &config.ResolvedExecutionConfig{
		MaxInlineSize: 4096,
	}, false)

	require.Equal(t, "48", envValue(envVars, contracts.MaxRecursionDepthEnv))
}

func envValue(envVars []corev1.EnvVar, name string) string {
	for _, env := range envVars {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}
