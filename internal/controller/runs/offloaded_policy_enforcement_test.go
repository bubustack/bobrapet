/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0
*/

package runs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/core/contracts"
)

func TestResolveRunScopedInputsFailsClosedForOffloadedPolicyError(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	operatorConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bobrapet-operator-config",
			Namespace: "default",
		},
		Data: map[string]string{
			contracts.KeyTemplatingOffloadedPolicy: config.TemplatingOffloadedPolicyError,
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun",
			Namespace: "default",
		},
	}
	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "consumer",
			Namespace: "default",
			Labels:    runsidentity.SelectorLabels(storyRun.Name),
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "consumer",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: storyRun.Name},
			},
			Input: &runtime.RawExtension{Raw: []byte(`{"value":"{{ steps.fetch.output.body }}"}`)},
		},
	}
	fetch := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "fetch",
			Namespace: "default",
			Labels:    runsidentity.SelectorLabels(storyRun.Name),
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "fetch",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: storyRun.Name},
			},
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhaseSucceeded,
			Output: &runtime.RawExtension{Raw: []byte(`{
				"body":{"$bubuStorageRef":"storyruns/test/outputs/fetch.json"}
			}`)},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(operatorConfig, storyRun, step, fetch).
		Build()

	manager, err := config.NewOperatorConfigManager(client, "default", "bobrapet-operator-config")
	require.NoError(t, err)
	require.NoError(t, manager.LoadInitial(context.Background()))

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
		},
	}

	_, err = reconciler.resolveRunScopedInputs(context.Background(), step, storyRun, nil, nil, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), `templating.offloaded-data-policy="error"`)
	require.Contains(t, err.Error(), "step input references offloaded data")
}

func TestResolveTemplateWithFailsClosedForOffloadedPolicyError(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	operatorConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bobrapet-operator-config",
			Namespace: "default",
		},
		Data: map[string]string{
			contracts.KeyTemplatingOffloadedPolicy: config.TemplatingOffloadedPolicyError,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(operatorConfig).
		Build()

	manager, err := config.NewOperatorConfigManager(client, "default", "bobrapet-operator-config")
	require.NoError(t, err)
	require.NoError(t, manager.LoadInitial(context.Background()))

	executor := &StepExecutor{
		Client:         client,
		Scheme:         scheme,
		ConfigResolver: config.NewResolver(client, manager),
	}

	raw := &runtime.RawExtension{Raw: []byte(`{"value":"{{ steps.fetch.output.body }}"}`)}
	vars := map[string]any{
		"steps": map[string]any{
			"fetch": map[string]any{
				"output": map[string]any{
					storage.StorageRefKey: "storyruns/test/outputs/fetch.json",
				},
			},
		},
	}

	_, err = executor.resolveTemplateWith(context.Background(), nil, "step-input", "consumer", "", raw, vars, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), `templating.offloaded-data-policy="error"`)
	require.Contains(t, err.Error(), "'with' block references offloaded data")
}
