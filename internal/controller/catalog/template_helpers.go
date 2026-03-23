package catalog

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/validation"
	"github.com/bubustack/core/contracts"
)

// TemplateStatusAccessor provides access to the embedded TemplateStatus.
//
// Behavior:
//   - Implemented by EngramTemplate and ImpulseTemplate.
//   - Allows shared status update helpers to operate on both types.
type TemplateStatusAccessor interface {
	// GetGeneration returns the resource's generation for ObservedGeneration.
	GetGeneration() int64
	// GetTemplateStatus returns a pointer to the embedded TemplateStatus.
	GetTemplateStatus() *catalogv1alpha1.TemplateStatus
}

// setErrorStatus updates a template's status to indicate a validation error.
//
// Behavior:
//   - Sets ObservedGeneration to current generation.
//   - Sets ValidationStatus to Invalid with the error message.
//   - Resets UsageCount to 0.
//   - Sets Ready condition to False with ValidationFailed reason.
//
// Arguments:
//   - t TemplateStatusAccessor: the template to update.
//   - message string: error message to record.
//
// Side Effects:
//   - Mutates the template's status fields in memory.
func setErrorStatus(t TemplateStatusAccessor, message string) {
	status := t.GetTemplateStatus()
	generation := t.GetGeneration()
	status.ObservedGeneration = generation
	status.ValidationStatus = enums.ValidationStatusInvalid
	status.ValidationErrors = []string{message}
	status.UsageCount = 0
	cm := conditions.NewConditionManager(generation)
	cm.SetCondition(&status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonValidationFailed, message)
}

// setReadyStatus updates a template's status to indicate it is valid and ready.
//
// Behavior:
//   - Sets ObservedGeneration to current generation.
//   - Sets ValidationStatus to Valid.
//   - Clears ValidationErrors.
//   - Sets Ready condition to True with ValidationPassed reason.
//
// Arguments:
//   - t TemplateStatusAccessor: the template to update.
//
// Side Effects:
//   - Mutates the template's status fields in memory.
func setReadyStatus(t TemplateStatusAccessor) {
	status := t.GetTemplateStatus()
	generation := t.GetGeneration()
	status.ObservedGeneration = generation
	status.ValidationStatus = enums.ValidationStatusValid
	status.ValidationErrors = nil
	cm := conditions.NewConditionManager(generation)
	cm.SetCondition(&status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonValidationPassed, "Template validated successfully")
}

// TemplateSpecAccessor provides access to common template spec fields.
//
// Behavior:
//   - Implemented by EngramTemplate and ImpulseTemplate.
//   - Allows shared validation helpers to operate on both types.
type TemplateSpecAccessor interface {
	// GetImage returns the template's container image.
	GetImage() string
	// GetVersion returns the template's version.
	GetVersion() string
}

// checkRequiredFields validates that image and version are present and non-empty.
//
// Behavior:
//   - Trims whitespace from image and version before checking.
//   - Returns the first missing field name, or empty string if all are present.
//
// Arguments:
//   - t TemplateSpecAccessor: the template to validate.
//
// Returns:
//   - missingField string: "image" or "version" if missing, "" if valid.
//   - handled bool: true if a field is missing, false otherwise.
func checkRequiredFields(t TemplateSpecAccessor) (missingField string, handled bool) {
	if strings.TrimSpace(t.GetImage()) == "" {
		return "image", true
	}
	if strings.TrimSpace(t.GetVersion()) == "" {
		return "version", true
	}
	return "", false
}

// SchemaEntry represents a named JSON schema for validation.
type SchemaEntry struct {
	Name   string
	Schema *runtime.RawExtension
}

// validateSchemas validates multiple JSON schemas and aggregates all errors.
//
// Behavior:
//   - Iterates through all provided schemas.
//   - Validates each non-nil schema using validation.ValidateJSONSchema.
//   - Collects all validation errors instead of stopping at the first.
//
// Arguments:
//   - schemas []SchemaEntry: list of named schemas to validate.
//
// Returns:
//   - []string: list of error messages in format "schemaName: error".
//   - Empty slice if all schemas are valid or nil.
func validateSchemas(schemas []SchemaEntry) []string {
	var errors []string
	for _, entry := range schemas {
		if entry.Schema == nil || len(entry.Schema.Raw) == 0 {
			continue
		}
		if err := validation.ValidateJSONSchema(entry.Schema.Raw); err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", entry.Name, err))
		}
	}
	return errors
}

// listByTemplateRefNameOrAll attempts an indexed list first, falls back to full list.
//
// Behavior:
//   - Tries to list using the spec.templateRef.name field index. Both
//     contracts.IndexEngramTemplateRef and contracts.IndexImpulseTemplateRef share
//     the same field path value, so the constant works for EngramList and ImpulseList
//     as long as the relevant type's index is registered with the manager.
//   - Falls back to an unfiltered list if the indexed lookup fails.
//   - Logs at V(1) when falling back to help detect missing field indexes.
//
// Arguments:
//   - ctx context.Context: for API calls and cancellation.
//   - logger logr.Logger: for V(1) debug logging on fallback.
//   - cl client.Client: Kubernetes client.
//   - list client.ObjectList: target list to populate (EngramList or ImpulseList).
//   - templateName string: template name to filter by.
//
// Returns:
//   - nil if at least one list succeeds.
//   - Error from fallback list if both fail.
func listByTemplateRefNameOrAll(ctx context.Context, logger logr.Logger, cl client.Client, list client.ObjectList, templateName string) error {
	if err := cl.List(ctx, list, client.MatchingFields{contracts.IndexEngramTemplateRef: templateName}); err == nil {
		return nil
	} else {
		logger.V(1).Info("Indexed list failed, falling back to full list",
			"index", contracts.IndexEngramTemplateRef,
			"templateName", templateName,
			"error", err.Error())
	}
	return cl.List(ctx, list)
}

// aggregateByTemplateRefName counts items matching the template name.
//
// Behavior:
//   - Returns 0 if templateName is empty or items slice is nil/empty.
//   - Iterates items and counts matches using the refName extractor.
//
// Arguments:
//   - items []T: slice of resources to count.
//   - templateName string: template name to match against.
//   - refName func(*T) string: extracts templateRef.name from each item.
//
// Returns:
//   - int32: count of items referencing the template.
func aggregateByTemplateRefName[T any](items []T, templateName string, refName func(*T) string) int32 {
	if templateName == "" || len(items) == 0 {
		return 0
	}
	var count int32
	for i := range items {
		if refName(&items[i]) == templateName {
			count++
		}
	}
	return count
}

// enqueueTemplateRequest trims the template name extracted from obj and returns a single
// reconcile.Request when the reference is non-empty.
//
// Behavior:
//   - Returns nil when obj is nil.
//   - Extracts and trims the template name using the provided extractor.
//   - Returns nil when the extracted name is empty.
//   - Centralizes the shared logic used by EngramTemplate and ImpulseTemplate watch handlers.
//
// Arguments:
//   - obj client.Object: the source object from the watch event.
//   - extract func(client.Object) string: extracts the template name from the object.
//
// Returns:
//   - []reconcile.Request: nil or a single request targeting the template.
func enqueueTemplateRequest(obj client.Object, extract func(client.Object) string) []reconcile.Request {
	if obj == nil {
		return nil
	}
	name := strings.TrimSpace(extract(obj))
	if name == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Name: name},
	}}
}

type templateStatusSnapshot struct {
	conditions         []metav1.Condition
	validationErrors   []string
	usageCount         int32
	observedGeneration int64
	validationStatus   enums.ValidationStatus
}

// snapshotTemplateStatus creates a deep copy of template status fields.
//
// Behavior:
//   - Clones conditions and validationErrors slices to prevent mutation.
//   - Captures scalar fields (usageCount, observedGeneration, validationStatus).
//
// Arguments:
//   - conditions []metav1.Condition: current conditions to clone.
//   - validationErrors []string: current errors to clone.
//   - usageCount int32: number of resources using this template.
//   - observedGeneration int64: last observed generation.
//   - validationStatus enums.ValidationStatus: current validation status.
//
// Returns:
//   - templateStatusSnapshot with deep-copied fields.
func snapshotTemplateStatus(
	conditionList []metav1.Condition,
	validationErrors []string,
	usageCount int32,
	observedGeneration int64,
	validationStatus enums.ValidationStatus,
) templateStatusSnapshot {
	clonedConditions := make([]metav1.Condition, len(conditionList))
	copy(clonedConditions, conditionList)
	return templateStatusSnapshot{
		conditions:         clonedConditions,
		validationErrors:   append([]string(nil), validationErrors...),
		usageCount:         usageCount,
		observedGeneration: observedGeneration,
		validationStatus:   validationStatus,
	}
}

// applyToEngramTemplateStatus writes snapshot fields to an EngramTemplate status.
//
// Behavior:
//   - Overwrites all status fields with snapshot values.
//   - Used by patch.RetryableStatusUpdate during conflict retries.
//
// Arguments:
//   - status *catalogv1alpha1.EngramTemplateStatus: target status to update.
//
// Side Effects:
//   - Mutates the provided status struct.
func (s templateStatusSnapshot) applyToEngramTemplateStatus(status *catalogv1alpha1.EngramTemplateStatus) {
	status.ObservedGeneration = s.observedGeneration
	status.Conditions = s.conditions
	status.ValidationStatus = s.validationStatus
	status.ValidationErrors = s.validationErrors
	status.UsageCount = s.usageCount
}

// applyToImpulseTemplateStatus writes snapshot fields to an ImpulseTemplate status.
//
// Behavior:
//   - Overwrites all status fields with snapshot values.
//   - Used by patch.RetryableStatusUpdate during conflict retries.
//
// Arguments:
//   - status *catalogv1alpha1.ImpulseTemplateStatus: target status to update.
//
// Side Effects:
//   - Mutates the provided status struct.
func (s templateStatusSnapshot) applyToImpulseTemplateStatus(status *catalogv1alpha1.ImpulseTemplateStatus) {
	status.ObservedGeneration = s.observedGeneration
	status.Conditions = s.conditions
	status.ValidationStatus = s.validationStatus
	status.ValidationErrors = s.validationErrors
	status.UsageCount = s.usageCount
}
