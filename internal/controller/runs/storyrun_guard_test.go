package runs

import (
	"context"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestGuardOversizedInputsFallsBackWhenConfigResolverMissing(t *testing.T) {
	t.Parallel()

	srun := &runsv1alpha1.StoryRun{
		Spec: runsv1alpha1.StoryRunSpec{
			Inputs: &runtime.RawExtension{Raw: make([]byte, DefaultMaxInlineInputsSize+1)},
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase: enums.PhaseFailed,
		},
	}

	handled, err := (&StoryRunReconciler{}).guardOversizedInputs(context.Background(), srun, nil)
	if err != nil {
		t.Fatalf("guardOversizedInputs returned error: %v", err)
	}
	if !handled {
		t.Fatal("expected oversized inputs to be handled")
	}
}
