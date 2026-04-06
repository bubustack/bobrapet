package runs

import (
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildRealtimeAnnotations_BindingHubOverrideWins(t *testing.T) {
	step := &runsv1alpha1.StepRun{
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "buffer",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{
					Name: "sr-1",
				},
			},
		},
	}
	story := &v1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story-1"},
		Spec: v1alpha1.StorySpec{
			Steps: []v1alpha1.Step{
				{Name: "ingress"},
				{Name: "buffer", Needs: []string{"ingress"}},
				{Name: "transcribe", Needs: []string{"buffer"}},
			},
		},
	}
	// Topology alone would choose p2p for buffer in this DAG, but binding
	// settings explicitly force hub.
	bindingInfo := &transportpb.BindingInfo{
		Payload: []byte(`{"routing":{"mode":"hub"}}`),
	}

	annotations := buildRealtimeAnnotations(step, story, "", bindingInfo)
	if got := annotations["transport.bubustack.io/routing-mode"]; got != "hub" {
		t.Fatalf("routing-mode annotation = %q, want %q", got, "hub")
	}
}

func TestRoutingModeFromBindingSettings(t *testing.T) {
	mode, ok := routingModeFromBindingSettings(&transportpb.BindingInfo{
		Payload: []byte(`{"routing":{"mode":"hub"}}`),
	})
	if !ok {
		t.Fatalf("expected routing mode override to be detected")
	}
	if mode != "hub" {
		t.Fatalf("routing mode = %q, want %q", mode, "hub")
	}
}
