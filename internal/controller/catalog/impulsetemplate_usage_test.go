package catalog

import (
	"testing"

	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
)

func TestAggregateImpulseTriggers(t *testing.T) {
	templateName := "impulse-template"
	impulses := []bubushv1alpha1.Impulse{
		{
			Spec: bubushv1alpha1.ImpulseSpec{
				TemplateRef: refs.ImpulseTemplateReference{Name: templateName},
			},
			Status: bubushv1alpha1.ImpulseStatus{TriggersReceived: 4},
		},
		{
			Spec: bubushv1alpha1.ImpulseSpec{
				TemplateRef: refs.ImpulseTemplateReference{Name: templateName},
			},
			Status: bubushv1alpha1.ImpulseStatus{TriggersReceived: 6},
		},
		{
			Spec: bubushv1alpha1.ImpulseSpec{
				TemplateRef: refs.ImpulseTemplateReference{Name: "other"},
			},
			Status: bubushv1alpha1.ImpulseStatus{TriggersReceived: 10},
		},
	}

	if total := aggregateImpulseTriggers(impulses, templateName); total != 2 {
		t.Fatalf("expected usage count 2, got %d", total)
	}

	if total := aggregateImpulseTriggers(nil, templateName); total != 0 {
		t.Fatalf("expected zero usage for nil slice, got %d", total)
	}

	if total := aggregateImpulseTriggers(impulses, "other-template"); total != 0 {
		t.Fatalf("expected zero usage for unrelated template, got %d", total)
	}
}
