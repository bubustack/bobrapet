package runs

import (
	"strings"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	refs "github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
				contracts.StoryRunLabelKey:  "story-run",
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
