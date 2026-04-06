package setup

import (
	"encoding/json"
	"testing"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestExtractStoryStepEngramRefsIncludesCompensationsAndFinally(t *testing.T) {
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: "ns"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{Name: "step-main", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "primary"}}},
			},
			Compensations: []bubuv1alpha1.Step{
				{Name: "step-comp", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "comp"}}},
			},
			Finally: []bubuv1alpha1.Step{
				{Name: "step-final", Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "final"}}},
			},
		},
	}

	got := extractStoryStepEngramRefs(story)
	require.ElementsMatch(t, []string{
		refs.NamespacedKey("ns", "primary"),
		refs.NamespacedKey("ns", "comp"),
		refs.NamespacedKey("ns", "final"),
	}, got)
}

func TestExtractStoryExecuteStoryRefsIncludesCompensationsAndFinally(t *testing.T) {
	with := func(name, namespace string) *runtime.RawExtension {
		payload := struct {
			StoryRef struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace,omitempty"`
			} `json:"storyRef"`
		}{}
		payload.StoryRef.Name = name
		payload.StoryRef.Namespace = namespace
		raw, _ := json.Marshal(payload)
		return &runtime.RawExtension{Raw: raw}
	}

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story", Namespace: "ns"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "main-exec",
					Type: enums.StepTypeExecuteStory,
					With: with("child-main", ""),
				},
			},
			Compensations: []bubuv1alpha1.Step{
				{
					Name: "comp-exec",
					Type: enums.StepTypeExecuteStory,
					With: with("child-comp", "ns"),
				},
			},
			Finally: []bubuv1alpha1.Step{
				{
					Name: "final-exec",
					Type: enums.StepTypeExecuteStory,
					With: with("child-final", "other"),
				},
			},
		},
	}

	got := extractStoryExecuteStoryRefs(story)
	require.ElementsMatch(t, []string{
		refs.NamespacedKey("ns", "child-main"),
		refs.NamespacedKey("ns", "child-comp"),
		refs.NamespacedKey("other", "child-final"),
	}, got)
}
