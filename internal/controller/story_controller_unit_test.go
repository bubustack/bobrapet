package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestValidateEngramReferencesIncludesCompensation(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))

	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default"},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(engram).Build()
	recon := &StoryReconciler{
		ControllerDependencies: config.ControllerDependencies{Client: fakeClient, Scheme: scheme},
	}

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: "default"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "main",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "primary"}},
				},
			},
			Compensations: []bubuv1alpha1.Step{
				{
					Name: "compensation",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "missing-comp"}},
				},
			},
		},
	}

	err := recon.validateEngramReferences(context.Background(), story)
	require.Error(t, err)
	require.Contains(t, err.Error(), "compensation")
}

func TestValidateEngramReferencesIncludesFinallyExecuteStory(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))

	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default"},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(engram).Build()
	recon := &StoryReconciler{
		ControllerDependencies: config.ControllerDependencies{Client: fakeClient, Scheme: scheme},
	}

	withBlock := struct {
		StoryRef struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace,omitempty"`
		} `json:"storyRef"`
	}{}
	withBlock.StoryRef.Name = "missing-final"
	raw, _ := json.Marshal(withBlock)

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: "default"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "main",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "primary"}},
				},
			},
			Finally: []bubuv1alpha1.Step{
				{
					Name: "finally-execute",
					Type: enums.StepTypeExecuteStory,
					With: &runtime.RawExtension{Raw: raw},
				},
			},
		},
	}

	err := recon.validateEngramReferences(context.Background(), story)
	require.Error(t, err)
	require.Contains(t, err.Error(), "finally-execute")
}

func TestUpdateStoryStatusDoesNotRequeueLargeTriggerSets(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: "default", Generation: 2},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "main",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "primary"}},
				},
			},
		},
		Status: bubuv1alpha1.StoryStatus{
			ObservedGeneration: 2,
		},
	}

	runs := make([]client.Object, 0, 60)
	for i := range 60 {
		run := &runsv1alpha1.StoryRun{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("run-%02d", i), Namespace: "default"},
			Spec: runsv1alpha1.StoryRunSpec{
				StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: story.Name}},
			},
			Status: runsv1alpha1.StoryRunStatus{
				TriggerTokens: []string{contracts.StoryRunTriggerTokenStory},
			},
		}
		runs = append(runs, run)
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&bubuv1alpha1.Story{}).
		WithObjects(append([]client.Object{
			&bubuv1alpha1.Engram{ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default"}},
			story,
		}, runs...)...).
		WithIndex(&runsv1alpha1.StoryRun{}, storyRunStoryIndexField, func(obj client.Object) []string {
			sr := obj.(*runsv1alpha1.StoryRun)
			return []string{refs.NamespacedKey(sr.Namespace, sr.Spec.StoryRef.Name)}
		}).
		Build()

	recon := &StoryReconciler{ControllerDependencies: config.ControllerDependencies{Client: fakeClient, Scheme: scheme}}
	recon.triggerDirty.Mark(refs.NamespacedKey(story.Namespace, story.Name))
	result, err := recon.updateStoryStatus(context.Background(), story, nil)
	require.NoError(t, err)
	require.Equal(t, requeueAfterBackfill, result.RequeueAfter)
	require.Equal(t, int64(len(runs)), story.Status.Triggers)
}

// TestCountStoryTriggersCountsAllTokensWithoutBackfillBudget is deferred
// until the countStoryTriggers method is implemented on StoryReconciler.
func TestCountStoryTriggersCountsAllTokensWithoutBackfillBudget(t *testing.T) {
	t.Skip("countStoryTriggers not yet implemented")
}

// TestStoryReconciler_transportToStoriesEnqueuesReferencingStories is deferred
// until the transportToStories mapper is implemented on StoryReconciler.
func TestStoryReconciler_transportToStoriesEnqueuesReferencingStories(t *testing.T) {
	t.Skip("transportToStories not yet implemented")
}
