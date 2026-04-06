package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
)

func TestBuildImpulsePodTemplateIncludesResolvedMaxInlineSize(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	manager, err := config.NewOperatorConfigManager(cl, "default", "bobrapet-operator-config")
	require.NoError(t, err)

	reconciler := &ImpulseReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	impulse := &v1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-crash-watcher",
			Namespace: "pod-crash-notifier",
		},
		Spec: v1alpha1.ImpulseSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: "pod-crash-analyzer"},
			},
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

	execCfg := &config.ResolvedExecutionConfig{
		ServiceAccountName:           "pod-crash-runner",
		AutomountServiceAccountToken: true,
		MaxInlineSize:                16384,
	}

	template := reconciler.buildImpulsePodTemplate(impulse, story, execCfg, map[string]string{"app": "test"}, nil, nil)
	require.Len(t, template.Spec.Containers, 1)

	var value string
	var recursionValue string
	for _, env := range template.Spec.Containers[0].Env {
		if env.Name == contracts.MaxInlineSizeEnv {
			value = env.Value
		}
		if env.Name == contracts.MaxRecursionDepthEnv {
			recursionValue = env.Value
		}
	}
	require.Equal(t, "16384", value)
	require.Equal(t, "48", recursionValue)
}
