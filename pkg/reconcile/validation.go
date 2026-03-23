// Package reconcile provides shared helpers for controller reconciliation.
package reconcile

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
)

// TemplateRefResult holds the outcome of a template reference validation
// so reconcilers can update conditions, emit events, and set validation status
// uniformly without duplicating the classification logic.
//
// Behavior:
//   - Encapsulates all condition/status fields needed for template validation.
//   - Provides ShouldEmitEvent to determine if condition transition warrants an event.
//   - Used by Engram, Impulse, and Story reconcilers for template/reference validation.
type TemplateRefResult struct {
	// ValidationStatus is the overall validation status (Valid, Invalid, Unknown).
	ValidationStatus enums.ValidationStatus

	// ValidationErrors contains human-readable error messages for invalid references.
	ValidationErrors []string

	// ConditionStatus is the status to set on the TemplateResolved condition.
	ConditionStatus metav1.ConditionStatus

	// ConditionReason is the reason code for the condition (e.g., TemplateResolved, TemplateNotFound).
	ConditionReason string

	// ConditionMessage is the human-readable message for the condition.
	ConditionMessage string

	// IsTransient indicates the error is transient (network, timeout) and should trigger backoff.
	IsTransient bool

	// OriginalError is the underlying error that caused the validation failure.
	OriginalError error
}

// ShouldEmitEvent determines if a condition transition warrants emitting a Kubernetes event.
//
// Behavior:
//   - Returns true when the condition status changed.
//   - Returns true when the reason changed (different failure type).
//   - Returns true when the message changed (different failure detail).
//   - Used to prevent event spam on repeated reconciles with the same state.
//
// Arguments:
//   - prev *metav1.Condition: the previous condition state; nil means first observation.
//
// Returns:
//   - bool: true when an event should be emitted.
func (r *TemplateRefResult) ShouldEmitEvent(prev *metav1.Condition) bool {
	if prev == nil {
		// First observation; emit event only for failures.
		return r.ConditionStatus != metav1.ConditionTrue
	}
	return prev.Status != r.ConditionStatus ||
		prev.Reason != r.ConditionReason ||
		prev.Message != r.ConditionMessage
}

// IsValid returns true when the template reference is valid.
func (r *TemplateRefResult) IsValid() bool {
	return r.ValidationStatus == enums.ValidationStatusValid
}

// ClassifyTemplateError converts a template lookup error into a TemplateRefResult
// with appropriate condition status, reason, and message.
//
// Behavior:
//   - Returns Valid result when err is nil.
//   - Returns Invalid with TemplateNotFound when err is NotFound.
//   - Returns Invalid with custom reason when err is a sentinel (e.g., empty name).
//   - Returns Invalid with TransientError for other errors (network, timeout).
//
// Arguments:
//   - err error: the error from template lookup (may be nil).
//   - templateType string: the template type for messages (e.g., "EngramTemplate").
//   - templateName string: the template name for messages.
//   - emptyRefError error: sentinel error for empty reference name (compared via errors.Is).
//   - emptyRefReason string: condition reason for empty reference (e.g., "TemplateRefInvalid").
//
// Returns:
//   - *TemplateRefResult: the classified result with all fields populated.
func ClassifyTemplateError(
	err error,
	templateType string,
	templateName string,
	emptyRefError error,
	emptyRefReason string,
) *TemplateRefResult {
	switch {
	case err == nil:
		return &TemplateRefResult{
			ValidationStatus: enums.ValidationStatusValid,
			ValidationErrors: nil,
			ConditionStatus:  metav1.ConditionTrue,
			ConditionReason:  conditions.ReasonTemplateResolved,
			ConditionMessage: fmt.Sprintf("%s reference is valid.", templateType),
			IsTransient:      false,
			OriginalError:    nil,
		}

	case apierrors.IsNotFound(err):
		msg := fmt.Sprintf("%s %q not found.", templateType, templateName)
		return &TemplateRefResult{
			ValidationStatus: enums.ValidationStatusInvalid,
			ValidationErrors: []string{msg},
			ConditionStatus:  metav1.ConditionFalse,
			ConditionReason:  conditions.ReasonTemplateNotFound,
			ConditionMessage: msg,
			IsTransient:      false,
			OriginalError:    err,
		}

	case emptyRefError != nil && errors.Is(err, emptyRefError):
		msg := err.Error()
		return &TemplateRefResult{
			ValidationStatus: enums.ValidationStatusInvalid,
			ValidationErrors: []string{msg},
			ConditionStatus:  metav1.ConditionFalse,
			ConditionReason:  emptyRefReason,
			ConditionMessage: msg,
			IsTransient:      false,
			OriginalError:    err,
		}

	default:
		// Transient/unknown resolution error.
		msg := fmt.Sprintf("Failed to resolve %s: %s", templateType, err.Error())
		return &TemplateRefResult{
			ValidationStatus: enums.ValidationStatusInvalid,
			ValidationErrors: []string{msg},
			ConditionStatus:  metav1.ConditionUnknown,
			ConditionReason:  conditions.ReasonTemplateResolutionFailed,
			ConditionMessage: msg,
			IsTransient:      true,
			OriginalError:    err,
		}
	}
}

// EmitValidationEvent emits a Kubernetes warning event for a validation failure
// when the condition transition warrants it (to prevent event spam).
//
// Behavior:
//   - Does nothing when recorder is nil or result is valid.
//   - Checks ShouldEmitEvent against the previous condition.
//   - Emits a Warning event with the condition reason and message.
//
// Arguments:
//   - recorder record.EventRecorder: the event recorder (may be nil in tests).
//   - obj client.Object: the object to emit the event for.
//   - result *TemplateRefResult: the validation result.
//   - prevCondition *metav1.Condition: the previous condition for transition detection.
func EmitValidationEvent(
	recorder record.EventRecorder,
	obj client.Object,
	result *TemplateRefResult,
	prevCondition *metav1.Condition,
) {
	if recorder == nil || result.IsValid() {
		return
	}
	if !result.ShouldEmitEvent(prevCondition) {
		return
	}
	Emit(recorder, obj, corev1.EventTypeWarning, result.ConditionReason, result.ConditionMessage)
}

// ValidateTemplateRef performs a template reference validation by fetching the
// template and classifying any errors into a TemplateRefResult.
//
// Behavior:
//   - Validates the template name is non-empty (using emptyRefError sentinel).
//   - Fetches the template using the provided client.
//   - Classifies the result using ClassifyTemplateError.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - c client.Client: the Kubernetes client.
//   - templateName string: the name of the template to fetch (trimmed).
//   - templateType string: the type for messages (e.g., "EngramTemplate").
//   - newTemplate func() client.Object: factory for the template type.
//   - key client.ObjectKey: the key to fetch (cluster-scoped uses just Name).
//   - emptyRefError error: sentinel error for empty reference.
//   - emptyRefReason string: condition reason for empty reference.
//
// Returns:
//   - client.Object: the fetched template (nil on error).
//   - *TemplateRefResult: the validation result.
func ValidateTemplateRef(
	ctx context.Context,
	c client.Client,
	templateName string,
	templateType string,
	newTemplate func() client.Object,
	key client.ObjectKey,
	emptyRefError error,
	emptyRefReason string,
) (client.Object, *TemplateRefResult) {
	if templateName == "" && emptyRefError != nil {
		return nil, ClassifyTemplateError(emptyRefError, templateType, templateName, emptyRefError, emptyRefReason)
	}

	template := newTemplate()
	err := c.Get(ctx, key, template)

	result := ClassifyTemplateError(err, templateType, templateName, emptyRefError, emptyRefReason)
	if err != nil {
		return nil, result
	}
	return template, result
}

// SnapshotCondition captures the current state of a condition for later comparison.
//
// Behavior:
//   - Returns nil when the condition is not found.
//   - Returns a snapshot of Status, Reason, Message for transition detection.
//
// Arguments:
//   - conds []metav1.Condition: the conditions slice to search.
//   - conditionType string: the condition type to find.
//
// Returns:
//   - *metav1.Condition: the found condition or nil.
func SnapshotCondition(conds []metav1.Condition, conditionType string) *metav1.Condition {
	return conditions.GetCondition(conds, conditionType)
}

// ApplyTemplateCondition updates the TemplateResolved condition on a conditions slice
// using the TemplateRefResult.
//
// Behavior:
//   - Clones the conditions slice to avoid mutating the original.
//   - Sets the TemplateResolved condition with the result's fields.
//   - Returns the updated conditions slice.
//
// Arguments:
//   - conds []metav1.Condition: the existing conditions.
//   - generation int64: the resource generation for ObservedGeneration.
//   - result *TemplateRefResult: the validation result.
//
// Returns:
//   - []metav1.Condition: the updated conditions slice.
func ApplyTemplateCondition(
	conds []metav1.Condition,
	generation int64,
	result *TemplateRefResult,
) []metav1.Condition {
	updated := append([]metav1.Condition(nil), conds...)
	cm := conditions.NewConditionManager(generation)
	cm.SetCondition(
		&updated,
		conditions.ConditionTemplateResolved,
		result.ConditionStatus,
		result.ConditionReason,
		result.ConditionMessage,
	)
	return updated
}

// ReferenceLoadResult holds the outcome of loading a reference (template, story, etc.)
// with phase-blocking semantics used by Impulse-style reconcilers.
//
// Behavior:
//   - Encapsulates whether to stop reconciliation (NotFound → block).
//   - Provides the loaded object when successful.
//   - Used by Impulse controller for ImpulseTemplate and Story reference loading.
type ReferenceLoadResult[T any] struct {
	// Object is the loaded reference (nil on error).
	Object T

	// Stop indicates reconciliation should stop (NotFound case).
	Stop bool

	// Error is the transient error that should trigger backoff (nil when Stop=true for NotFound).
	Error error

	// BlockMessage is the message to use when setting phase to Blocked.
	BlockMessage string

	// EventReason is the reason for the warning event.
	EventReason string
}

// LoadClusterTemplate fetches a cluster-scoped template and returns a result
// with phase-blocking semantics.
//
// Behavior:
//   - Returns the template on success.
//   - Returns Stop=true with BlockMessage on NotFound.
//   - Returns Error on transient failures.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - c client.Client: the Kubernetes client.
//   - templateName string: the name of the template.
//   - templateType string: for messages (e.g., "ImpulseTemplate").
//   - newTemplate func() T: factory for the template type.
//
// Returns:
//   - *ReferenceLoadResult[T]: the load result.
func LoadClusterTemplate[T client.Object](
	ctx context.Context,
	c client.Client,
	templateName string,
	templateType string,
	newTemplate func() T,
) *ReferenceLoadResult[T] {
	template := newTemplate()
	err := c.Get(ctx, client.ObjectKey{Name: templateName}, template)

	if err == nil {
		return &ReferenceLoadResult[T]{
			Object: template,
		}
	}

	if apierrors.IsNotFound(err) {
		return &ReferenceLoadResult[T]{
			Stop:         true,
			BlockMessage: fmt.Sprintf("%s '%s' not found", templateType, templateName),
			EventReason:  conditions.ReasonTemplateNotFound,
		}
	}

	var zero T
	return &ReferenceLoadResult[T]{
		Object: zero,
		Error:  fmt.Errorf("failed to get %s: %w", templateType, err),
	}
}
