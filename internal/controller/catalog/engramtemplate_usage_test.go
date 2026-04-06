package catalog

import (
	"testing"

	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
)

func TestAggregateEngramExecutions(t *testing.T) {
	templateName := "demo-template"
	records := []bubushv1alpha1.Engram{
		{
			Spec: bubushv1alpha1.EngramSpec{
				TemplateRef: refs.EngramTemplateReference{Name: templateName},
			},
			Status: bubushv1alpha1.EngramStatus{TotalExecutions: 3},
		},
		{
			Spec: bubushv1alpha1.EngramSpec{
				TemplateRef: refs.EngramTemplateReference{Name: templateName},
			},
			Status: bubushv1alpha1.EngramStatus{TotalExecutions: 2},
		},
		{
			Spec: bubushv1alpha1.EngramSpec{
				TemplateRef: refs.EngramTemplateReference{Name: "other"},
			},
			Status: bubushv1alpha1.EngramStatus{TotalExecutions: 5},
		},
	}

	total := aggregateEngramExecutions(records, templateName)
	if total != 2 {
		t.Fatalf("expected usage count 2, got %d", total)
	}

	if total := aggregateEngramExecutions(nil, templateName); total != 0 {
		t.Fatalf("expected zero usage for nil slice, got %d", total)
	}

	if total := aggregateEngramExecutions(records, "missing"); total != 0 {
		t.Fatalf("expected zero usage for unknown template, got %d", total)
	}
}
