// Package validation provides shared validation helpers for controllers and webhooks.
package validation

import (
	"errors"
	"strings"

	"github.com/bubustack/bobrapet/pkg/conditions"
)

// ReasonError wraps an error with a structured reason string so callers can
// emit consistent Kubernetes event reasons while preserving the original error.
//
// Behavior:
//   - Error() returns the underlying error message.
//   - Unwrap() returns the underlying error for errors.Is/errors.As chains.
//   - Reason() returns the structured reason string.
//
// Usage:
//
//	err := validation.WrapWithReason(conditions.ReasonEngramReferenceInvalid, originalErr)
//	var reasonErr *validation.ReasonError
//	if errors.As(err, &reasonErr) {
//	    fmt.Println(reasonErr.Reason()) // "EngramReferenceInvalid"
//	}
type ReasonError struct {
	reason string
	err    error
}

// Error satisfies the error interface by returning the wrapped error's message.
func (e *ReasonError) Error() string { return e.err.Error() }

// Unwrap returns the underlying error for errors.Is/errors.As traversal.
func (e *ReasonError) Unwrap() error { return e.err }

// Reason returns the structured reason string for Kubernetes events.
func (e *ReasonError) Reason() string { return e.reason }

// WrapWithReason attaches a structured reason to a validation error so event
// emitters can extract consistent Kubernetes event reasons.
//
// Behavior:
//   - Returns nil when err is nil (nil-safe).
//   - Wraps the error with a reason that can be extracted via errors.As.
//   - Preserves original error via Unwrap for errors.Is/errors.As chains.
//
// Arguments:
//   - reason string: the event reason to attach (e.g., conditions.ReasonEngramReferenceInvalid).
//   - err error: the underlying validation error.
//
// Returns:
//   - error: nil when err is nil, otherwise a *ReasonError.
func WrapWithReason(reason string, err error) error {
	if err == nil {
		return nil
	}
	return &ReasonError{reason: reason, err: err}
}

// ExtractReason extracts the reason string from a validation error.
//
// Behavior:
//   - Returns the reason from *ReasonError if present.
//   - Falls back to substring heuristics (engram/transport/story).
//   - Defaults to conditions.ReasonReferenceNotFound when no signal is found.
//
// Arguments:
//   - err error: the validation error to analyze.
//
// Returns:
//   - string: the event reason string for Kubernetes events.
func ExtractReason(err error) string {
	if err == nil {
		return conditions.ReasonReferenceNotFound
	}

	var reasonErr *ReasonError
	if errors.As(err, &reasonErr) && reasonErr.reason != "" {
		return reasonErr.reason
	}

	// Fallback heuristics for errors not wrapped with WrapWithReason.
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "engram"):
		return conditions.ReasonEngramReferenceInvalid
	case strings.Contains(msg, "transport"):
		return conditions.ReasonTransportReferenceInvalid
	case strings.Contains(msg, "story"):
		return conditions.ReasonStoryReferenceInvalid
	default:
		return conditions.ReasonReferenceNotFound
	}
}
