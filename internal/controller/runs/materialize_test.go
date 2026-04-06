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
	"testing"

	"github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsureMaterializeStepRunAcceptsOwnedMatchingStepRun(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	storyRun := &v1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun-a",
			Namespace: "default",
			UID:       types.UID("storyrun-a-uid"),
		},
	}

	purpose := materializePurposeStepInput
	stepName := "step-a"
	stepID := materializeStepID(purpose, stepName, "")
	stepRunName := materializeStepRunName(storyRun.Name, stepID)
	engramName := resolveMaterializeEngramName(nil)

	existing := newMaterializeStepRunFixture(storyRun, stepRunName, stepID, purpose, stepName, engramName)
	existing.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "StoryRun",
			Name:       storyRun.Name,
			UID:        storyRun.UID,
			Controller: boolPtr(true),
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		Build()

	got, err := ensureMaterializeStepRun(
		context.Background(),
		cl,
		scheme,
		nil,
		storyRun,
		purpose,
		stepName,
		"",
		materializeModeObject,
		map[string]any{"x": "y"},
		map[string]any{"inputs": map[string]any{"k": "v"}},
		"",
	)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, existing.Name, got.Name)
	require.Equal(t, existing.Spec.StepID, got.Spec.StepID)
}

func TestEnsureMaterializeStepRunRejectsUnownedSpoofedStepRun(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	storyRun := &v1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun-b",
			Namespace: "default",
			UID:       types.UID("storyrun-b-uid"),
		},
	}

	purpose := materializePurposeStepInput
	stepName := "step-b"
	stepID := materializeStepID(purpose, stepName, "")
	stepRunName := materializeStepRunName(storyRun.Name, stepID)
	engramName := resolveMaterializeEngramName(nil)

	spoofed := newMaterializeStepRunFixture(storyRun, stepRunName, stepID, purpose, stepName, engramName)
	spoofed.OwnerReferences = nil

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(spoofed).
		Build()

	_, err := ensureMaterializeStepRun(
		context.Background(),
		cl,
		scheme,
		nil,
		storyRun,
		purpose,
		stepName,
		"",
		materializeModeObject,
		map[string]any{"x": "y"},
		map[string]any{"inputs": map[string]any{"k": "v"}},
		"",
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "identity validation")
	require.ErrorContains(t, err, "controller owner reference is required")
}

func TestEnsureMaterializeStepRunRejectsMismatchedIdentity(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	storyRun := &v1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun-c",
			Namespace: "default",
			UID:       types.UID("storyrun-c-uid"),
		},
	}

	purpose := materializePurposeStepInput
	stepName := "step-c"
	stepID := materializeStepID(purpose, stepName, "")
	stepRunName := materializeStepRunName(storyRun.Name, stepID)
	engramName := resolveMaterializeEngramName(nil)

	mismatched := newMaterializeStepRunFixture(storyRun, stepRunName, "wrong-step-id", purpose, stepName, engramName)
	mismatched.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "StoryRun",
			Name:       storyRun.Name,
			UID:        storyRun.UID,
			Controller: boolPtr(true),
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(mismatched).
		Build()

	_, err := ensureMaterializeStepRun(
		context.Background(),
		cl,
		scheme,
		nil,
		storyRun,
		purpose,
		stepName,
		"",
		materializeModeObject,
		map[string]any{"x": "y"},
		map[string]any{"inputs": map[string]any{"k": "v"}},
		"",
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "identity validation")
	require.ErrorContains(t, err, "spec.stepId mismatch")
}

func newMaterializeStepRunFixture(
	storyRun *v1alpha1.StoryRun,
	name string,
	stepID string,
	purpose string,
	stepName string,
	engramName string,
) *v1alpha1.StepRun {
	return &v1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: storyRun.Namespace,
			Labels: map[string]string{
				contracts.MaterializeLabelKey: "true",
			},
			Annotations: map[string]string{
				contracts.MaterializePurposeAnnotation: purpose,
				contracts.MaterializeTargetAnnotation:  stepName,
			},
		},
		Spec: v1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: storyRun.Name},
			},
			StepID: stepID,
			EngramRef: &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: engramName},
			},
		},
	}
}

func boolPtr(v bool) *bool {
	return &v
}
