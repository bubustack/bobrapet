/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubeutil

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bubustack/bobrapet/pkg/enums"
)

// ValidationParams captures the aggregated validation/binding context required to
// derive a Ready condition snapshot plus the corresponding enums.ValidationStatus
// value for controllers. Callers supply controller-specific reason/message text so
// Ready conditions remain informative while the helper enforces consistent state
// transitions across controllers.
type ValidationParams struct {
	// ValidationErrors lists spec validation failures (trimmed human-readable text).
	ValidationErrors []string
	// BindingErrors lists runtime/binding failures that should surface alongside
	// validation errors when present.
	BindingErrors []string
	// BindingSummary allows callers to provide an already-compacted summary to
	// avoid rejoining binding errors; when empty, BindingErrors are joined with "; ".
	BindingSummary string

	// ReadyBindings/TotalBindings/PendingBindings describe live binding counts.
	// Pending logic only activates when TotalBindings > 0, ReadyBindings == 0,
	// and PendingBindings > 0 (mirroring existing controller behavior).
	ReadyBindings   int
	TotalBindings   int
	PendingBindings int

	// SuccessReason/SuccessMessage annotate the Ready condition when all checks pass.
	SuccessReason  string
	SuccessMessage string

	// ValidationFailedReason describes the Ready condition reason when spec validation fails.
	ValidationFailedReason string
	// BindingFailedReason is used when runtime/binding errors exist; defaults to
	// ValidationFailedReason when omitted.
	BindingFailedReason string
	// PendingReason labels the Ready condition while bindings negotiate; defaults
	// to ValidationFailedReason when omitted.
	PendingReason string

	// SuccessStatus / ValidationFailedStatus / BindingFailedStatus / PendingStatus
	// let callers override the enums.ValidationStatus assigned for each scenario.
	// When left empty they default to Valid / Invalid / Invalid / Unknown.
	SuccessStatus          enums.ValidationStatus
	ValidationFailedStatus enums.ValidationStatus
	BindingFailedStatus    enums.ValidationStatus
	PendingStatus          enums.ValidationStatus

	// PendingMessageFormatter renders the Ready condition message for the pending
	// state when provided; otherwise we fall back to a generic
	// "awaiting reconciliation (%d pending)" string.
	PendingMessageFormatter func(pending int) string
}

// ValidationOutcome encapsulates the Ready condition data plus the
// enums.ValidationStatus value that callers should persist.
type ValidationOutcome struct {
	ReadyStatus       metav1.ConditionStatus
	ReadyReason       string
	ReadyMessage      string
	ValidationStatus  enums.ValidationStatus
	ValidationErrors  []string
	hasValidationInfo bool
}

// AggregatedErrors returns the combined validation+binding error slice,
// guaranteeing a nil slice when no errors were supplied. Callers can store this
// in their status structs without additional copying.
func (o ValidationOutcome) AggregatedErrors() []string {
	if len(o.ValidationErrors) == 0 && !o.hasValidationInfo {
		return nil
	}
	return o.ValidationErrors
}

// ComputeValidationOutcome normalizes validation/binding inputs into a single
// Ready condition + validation status tuple so controllers present consistent
// messaging. All slices are defensively copied to avoid aliasing caller data.
//
// Arguments:
//   - p ValidationParams: the validation parameters.
//
// Returns:
//   - ValidationOutcome: computed Ready condition and validation status.
func ComputeValidationOutcome(p ValidationParams) ValidationOutcome {
	outcome := ValidationOutcome{
		ReadyStatus:      metav1.ConditionTrue,
		ReadyReason:      p.SuccessReason,
		ReadyMessage:     p.SuccessMessage,
		ValidationStatus: pickValidationStatus(p.SuccessStatus, enums.ValidationStatusValid),
	}

	validationErrors := copyStrings(p.ValidationErrors)
	bindingErrors := copyStrings(p.BindingErrors)
	aggregated := append([]string(nil), validationErrors...)
	if len(bindingErrors) > 0 {
		aggregated = append(aggregated, bindingErrors...)
	}
	if len(aggregated) > 0 {
		outcome.hasValidationInfo = true
	}

	switch {
	case len(validationErrors) > 0:
		outcome.ReadyStatus = metav1.ConditionFalse
		outcome.ReadyReason = fallbackString(p.ValidationFailedReason, p.SuccessReason)
		outcome.ReadyMessage = strings.Join(validationErrors, "; ")
		outcome.ValidationStatus = pickValidationStatus(p.ValidationFailedStatus, enums.ValidationStatusInvalid)

	case len(bindingErrors) > 0:
		outcome.ReadyStatus = metav1.ConditionFalse
		outcome.ReadyReason = fallbackString(p.BindingFailedReason, fallbackString(p.ValidationFailedReason, p.SuccessReason))
		outcome.ReadyMessage = bindingSummary(p.BindingSummary, bindingErrors)
		outcome.ValidationStatus = pickValidationStatus(p.BindingFailedStatus, enums.ValidationStatusInvalid)

	case p.TotalBindings > 0 && p.PendingBindings > 0 && p.ReadyBindings == 0:
		outcome.ReadyStatus = metav1.ConditionFalse
		outcome.ReadyReason = fallbackString(p.PendingReason, fallbackString(p.ValidationFailedReason, p.SuccessReason))
		outcome.ValidationStatus = pickValidationStatus(p.PendingStatus, enums.ValidationStatusUnknown)
		if p.PendingMessageFormatter != nil {
			outcome.ReadyMessage = p.PendingMessageFormatter(p.PendingBindings)
		} else {
			outcome.ReadyMessage = fmt.Sprintf("awaiting reconciliation (%d pending)", p.PendingBindings)
		}
	}

	outcome.ValidationErrors = nilIfEmpty(aggregated)
	return outcome
}

func copyStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func bindingSummary(summary string, bindingErrors []string) string {
	if trimmed := strings.TrimSpace(summary); trimmed != "" {
		return trimmed
	}
	return strings.Join(bindingErrors, "; ")
}

func fallbackString(primary, backup string) string {
	if strings.TrimSpace(primary) != "" {
		return primary
	}
	return backup
}

func pickValidationStatus(candidate, defaultStatus enums.ValidationStatus) enums.ValidationStatus {
	if candidate != "" {
		return candidate
	}
	return defaultStatus
}

func nilIfEmpty(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	return in
}
