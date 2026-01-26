package validation

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
)

// ValidationError represents a single validation error with field path and reason.
//
// Behavior:
//   - Captures the field path (e.g., "spec.steps[3].ref") for precise error location.
//   - Includes a structured reason for Kubernetes events.
//   - Provides a human-readable message for users.
type ValidationError struct {
	// Field is the path to the invalid field (e.g., "spec.steps[3].ref").
	Field string

	// Reason is the event reason code (e.g., conditions.ReasonEngramReferenceInvalid).
	Reason string

	// Message is the human-readable error message.
	Message string

	// Err is the underlying error (may be nil).
	Err error
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s: %s", e.Field, e.Message)
	}
	return e.Message
}

// Unwrap returns the underlying error for errors.Is/errors.As.
func (e *ValidationError) Unwrap() error { return e.Err }

// Aggregator collects multiple validation errors for bulk status updates.
//
// Behavior:
//   - Collects errors without stopping on the first failure.
//   - Converts collected errors to field.ErrorList for webhook responses.
//   - Converts collected errors to condition/status for controller status.
//   - Thread-safe for single-goroutine use within a reconcile loop.
//
// Usage (controller):
//
//	agg := validation.NewAggregator()
//	agg.AddFieldError("spec.steps[0].ref", conditions.ReasonEngramReferenceInvalid, "engram not found")
//	agg.AddFieldError("spec.steps[1].transport", conditions.ReasonTransportReferenceInvalid, "transport not found")
//	if agg.HasErrors() {
//	    status := agg.ToStatus()
//	}
//
// Usage (webhook):
//
//	agg := validation.NewAggregator()
//	agg.AddFieldError("spec.steps[0].ref", conditions.ReasonEngramReferenceInvalid, "engram not found")
//	if agg.HasErrors() {
//	    return nil, agg.ToFieldErrors()
//	}
type Aggregator struct {
	errors []*ValidationError
}

// NewAggregator creates a new validation error aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{
		errors: make([]*ValidationError, 0),
	}
}

// Add adds an existing ValidationError to the aggregator.
func (a *Aggregator) Add(err *ValidationError) {
	if err != nil {
		a.errors = append(a.errors, err)
	}
}

// AddError wraps a generic error and adds it to the aggregator.
//
// Behavior:
//   - Extracts reason from *ReasonError if present.
//   - Falls back to ExtractReason heuristics if not.
func (a *Aggregator) AddError(err error) {
	if err == nil {
		return
	}
	a.errors = append(a.errors, &ValidationError{
		Reason:  ExtractReason(err),
		Message: err.Error(),
		Err:     err,
	})
}

// AddFieldError adds a validation error with field path, reason, and message.
func (a *Aggregator) AddFieldError(fieldName, reason, message string) {
	a.errors = append(a.errors, &ValidationError{
		Field:   fieldName,
		Reason:  reason,
		Message: message,
	})
}

// AddFieldErrorf adds a validation error with formatted message.
func (a *Aggregator) AddFieldErrorf(fieldName, reason, format string, args ...interface{}) {
	a.AddFieldError(fieldName, reason, fmt.Sprintf(format, args...))
}

// HasErrors returns true if any validation errors were collected.
func (a *Aggregator) HasErrors() bool {
	return len(a.errors) > 0
}

// Errors returns the collected validation errors.
func (a *Aggregator) Errors() []*ValidationError {
	return a.errors
}

// FirstReason returns the reason of the first error, or ReasonReferenceNotFound if empty.
func (a *Aggregator) FirstReason() string {
	if len(a.errors) == 0 {
		return conditions.ReasonReferenceNotFound
	}
	return a.errors[0].Reason
}

// FirstError returns the first error, or nil if empty.
func (a *Aggregator) FirstError() error {
	if len(a.errors) == 0 {
		return nil
	}
	return a.errors[0]
}

// Messages returns all error messages as a slice.
func (a *Aggregator) Messages() []string {
	messages := make([]string, len(a.errors))
	for i, err := range a.errors {
		messages[i] = err.Message
	}
	return messages
}

// CombinedMessage returns all error messages joined by semicolons.
func (a *Aggregator) CombinedMessage() string {
	return strings.Join(a.Messages(), "; ")
}

// ToFieldErrors converts collected errors to field.ErrorList for webhook responses.
//
// Behavior:
//   - Creates a field.Error for each ValidationError with Type=Invalid.
//   - Uses the field path if provided, otherwise uses an empty path.
func (a *Aggregator) ToFieldErrors() field.ErrorList {
	if len(a.errors) == 0 {
		return nil
	}
	fieldErrors := make(field.ErrorList, len(a.errors))
	for i, err := range a.errors {
		var path *field.Path
		if err.Field != "" {
			path = field.NewPath(err.Field)
		}
		fieldErrors[i] = field.Invalid(path, nil, err.Message)
	}
	return fieldErrors
}

// ValidationResult holds the outcome of validation for controller status updates.
type ValidationResult struct {
	// ValidationStatus is Valid when no errors, Invalid otherwise.
	ValidationStatus enums.ValidationStatus

	// ValidationErrors contains all error messages.
	ValidationErrors []string

	// ConditionStatus is the status for the Ready condition.
	ConditionStatus metav1.ConditionStatus

	// ConditionReason is the reason for the Ready condition.
	ConditionReason string

	// ConditionMessage is the message for the Ready condition.
	ConditionMessage string
}

// ToValidationResult converts collected errors to a ValidationResult for status updates.
//
// Behavior:
//   - Returns Valid status when no errors.
//   - Returns Invalid status with aggregated messages when errors exist.
//   - Uses the first error's reason for the condition reason.
//
// Arguments:
//   - successMessage string: the message to use when validation passes.
func (a *Aggregator) ToValidationResult(successMessage string) *ValidationResult {
	if !a.HasErrors() {
		return &ValidationResult{
			ValidationStatus: enums.ValidationStatusValid,
			ValidationErrors: nil,
			ConditionStatus:  metav1.ConditionTrue,
			ConditionReason:  conditions.ReasonValidationPassed,
			ConditionMessage: successMessage,
		}
	}

	return &ValidationResult{
		ValidationStatus: enums.ValidationStatusInvalid,
		ValidationErrors: a.Messages(),
		ConditionStatus:  metav1.ConditionFalse,
		ConditionReason:  a.FirstReason(),
		ConditionMessage: a.CombinedMessage(),
	}
}
