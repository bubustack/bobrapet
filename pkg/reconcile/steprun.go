package reconcile

import (
	"reflect"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
)

// UpdateStepRunSpec mutates existing to match the desired StepRun spec and
// returns true when any field changed.
func UpdateStepRunSpec(existing, desired *runsv1alpha1.StepRun) bool {
	if existing == nil || desired == nil {
		return false
	}
	changed := false

	if existing.Spec.StepID != desired.Spec.StepID {
		existing.Spec.StepID = desired.Spec.StepID
		changed = true
	}
	if !reflect.DeepEqual(existing.Spec.StoryRunRef, desired.Spec.StoryRunRef) {
		existing.Spec.StoryRunRef = desired.Spec.StoryRunRef
		changed = true
	}
	if syncEngramRef(existing, desired) {
		changed = true
	}
	if !rawExtensionsEqual(existing.Spec.Input, desired.Spec.Input) {
		existing.Spec.Input = cloneRawExtension(desired.Spec.Input)
		changed = true
	}

	return changed
}

func syncEngramRef(existing, desired *runsv1alpha1.StepRun) bool {
	switch {
	case desired.Spec.EngramRef == nil && existing.Spec.EngramRef == nil:
		return false
	case desired.Spec.EngramRef == nil:
		existing.Spec.EngramRef = nil
		return true
	case existing.Spec.EngramRef == nil:
		clone := *desired.Spec.EngramRef
		existing.Spec.EngramRef = &clone
		return true
	case !reflect.DeepEqual(existing.Spec.EngramRef, desired.Spec.EngramRef):
		clone := *desired.Spec.EngramRef
		existing.Spec.EngramRef = &clone
		return true
	default:
		return false
	}
}

// rawExtensionsEqual compares two RawExtension pointers for byte equality.
func rawExtensionsEqual(a, b *runtime.RawExtension) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return reflect.DeepEqual(a.Raw, b.Raw)
	}
}

// cloneRawExtension creates a deep copy of a RawExtension.
func cloneRawExtension(src *runtime.RawExtension) *runtime.RawExtension {
	if src == nil {
		return nil
	}
	if len(src.Raw) == 0 {
		return &runtime.RawExtension{Raw: nil}
	}
	cp := make([]byte, len(src.Raw))
	copy(cp, src.Raw)
	return &runtime.RawExtension{Raw: cp}
}
