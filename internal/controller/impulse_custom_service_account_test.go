package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
)

func TestResolveImpulseConfigPreservesExplicitServiceAccountName(t *testing.T) {
	t.Parallel()

	template := &catalogv1alpha1.ImpulseTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "kubernetes"},
		Spec: catalogv1alpha1.ImpulseTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Version:        "1.0.0",
				Image:          "ghcr.io/bubustack/kubernetes-impulse:0.1.0",
				SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
			},
		},
	}

	customSA := "pod-crash-runner"
	impulse := &v1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-crash-watcher",
			Namespace: "pod-crash-notifier",
		},
		Spec: v1alpha1.ImpulseSpec{
			Execution: &v1alpha1.ExecutionOverrides{
				ServiceAccountName: &customSA,
			},
		},
	}

	reconciler := &ImpulseReconciler{
		ControllerDependencies: config.ControllerDependencies{
			ConfigResolver: config.NewResolver(nil, nil),
		},
	}

	resolved, err := reconciler.resolveImpulseConfig(context.Background(), impulse, template, nil)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	require.Equal(t, customSA, resolved.ServiceAccountName)
}
