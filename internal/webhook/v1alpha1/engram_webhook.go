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

package v1alpha1

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/validation"
)

// log is for logging in this package.
var engramlog = logf.Log.WithName("engram-resource")

type EngramWebhook struct {
	client.Client
	Config *config.ControllerConfig
}

// SetupWebhookWithManager registers the webhook for Engram in the manager.
//
// Behavior:
//   - Stores the manager's client for template lookups during validation.
//   - Creates both defaulter and validator webhooks for Engram resources.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//
// Returns:
//   - nil on success.
//   - Error if webhook registration fails.
func (wh *EngramWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	wh.Client = mgr.GetClient()

	return ctrl.NewWebhookManagedBy(mgr).For(&bubushv1alpha1.Engram{}).
		WithValidator(&EngramCustomValidator{
			Client: wh.Client,
			Config: wh.Config,
		}).
		WithDefaulter(&EngramCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-bubustack-io-v1alpha1-engram,mutating=true,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=engrams,verbs=create;update,versions=v1alpha1,name=mengram-v1alpha1.kb.io,admissionReviewVersions=v1

// EngramCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Engram when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type EngramCustomDefaulter struct {
	// No fields needed; Engram defaults are intentionally minimal.
}

var _ webhook.CustomDefaulter = &EngramCustomDefaulter{}

// Default implements webhook.CustomDefaulter for Engram resources.
//
// Behavior:
//   - Currently a no-op; Engram defaults are intentionally minimal.
//   - Controller handles defaulting at reconcile time.
//
// Arguments:
//   - ctx context.Context: unused but required by interface.
//   - obj runtime.Object: expected to be *bubushv1alpha1.Engram.
//
// Returns:
//   - nil on success.
//   - Error if obj is not an Engram.
func (d *EngramCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	return DefaultResource[*bubushv1alpha1.Engram](obj, "Engram", engramlog, nil)
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-bubustack-io-v1alpha1-engram,mutating=false,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=engrams,verbs=create;update,versions=v1alpha1,name=vengram-v1alpha1.kb.io,admissionReviewVersions=v1

// EngramCustomValidator struct is responsible for validating the Engram resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type EngramCustomValidator struct {
	Client client.Client
	Config *config.ControllerConfig
}

var _ webhook.CustomValidator = &EngramCustomValidator{}

// ValidateCreate implements webhook.CustomValidator for Engram creation.
//
// Behavior:
//   - Type-asserts obj to Engram and validates spec.
//   - Validates templateRef.name is set and template exists.
//   - Validates with block is a JSON object and within size limits.
//
// Arguments:
//   - ctx context.Context: for template lookup.
//   - obj runtime.Object: expected to be *bubushv1alpha1.Engram.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if type assertion fails or validation errors exist.
func (v *EngramCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return ValidateCreateResource[*bubushv1alpha1.Engram](ctx, obj, "Engram", engramlog, v.validateEngram)
}

// ValidateUpdate implements webhook.CustomValidator for Engram updates.
//
// Behavior:
//   - Type-asserts newObj to Engram and validates spec.
//   - Validates templateRef.name is set and template exists.
//   - Validates with block is a JSON object and within size limits.
//
// Arguments:
//   - ctx context.Context: for template lookup.
//   - oldObj runtime.Object: previous Engram state.
//   - newObj runtime.Object: proposed Engram state.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if type assertion fails or validation errors exist.
func (v *EngramCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	engram, ok := newObj.(*bubushv1alpha1.Engram)
	if !ok {
		return nil, fmt.Errorf("expected a Engram object for the newObj but got %T", newObj)
	}
	engramlog.Info("Validation for Engram upon update", "name", engram.GetName())

	if err := v.validateEngram(ctx, engram); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator for Engram deletion.
//
// Behavior:
//   - Always allows deletion (no-op validation).
//   - Exists as scaffold placeholder; delete verb not enabled in annotation.
//
// Arguments:
//   - ctx context.Context: unused.
//   - obj runtime.Object: expected to be *bubushv1alpha1.Engram.
//
// Returns:
//   - nil, nil (deletion always allowed).
func (v *EngramCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return ValidateDeleteResource[*bubushv1alpha1.Engram](obj, "Engram", engramlog, nil)
}

// validateEngram performs basic invariants validation for Engram specs.
//
// Behavior:
//   - Skips validation if Engram is being deleted (allows finalizer removal).
//   - Validates templateRef.name is set.
//   - Validates referenced EngramTemplate exists.
//   - Validates with block is a JSON object and within size limits.
//   - Validates with block against template's configSchema if defined.
//   - Aggregates multiple validation errors for comprehensive feedback.
//
// Arguments:
//   - ctx context.Context: for template lookup.
//   - engram *bubushv1alpha1.Engram: the Engram to validate.
//
// Returns:
//   - nil if all validations pass.
//   - Aggregated field.ErrorList containing all validation failures.
func (v *EngramCustomValidator) validateEngram(ctx context.Context, engram *bubushv1alpha1.Engram) error {
	if engram.GetDeletionTimestamp() != nil {
		// Allow finalizer removals even if the referenced template has been deleted.
		// The reconciler will handle cleanup without needing template validation.
		return nil
	}

	agg := validation.NewAggregator()

	if err := requireTemplateRef(engram); err != nil {
		agg.AddFieldError("spec.templateRef.name", conditions.ReasonTemplateNotFound, err.Error())
	}

	// Only proceed with template lookup if templateRef is valid
	var template *v1alpha1.EngramTemplate
	if engram.Spec.TemplateRef.Name != "" {
		var err error
		template, err = fetchEngramTemplate(ctx, v.Client, engram.Spec.TemplateRef.Name)
		if err != nil {
			agg.AddFieldError("spec.templateRef.name", conditions.ReasonTemplateNotFound, err.Error())
		}
	}

	// Validate with block if template was found
	if template != nil {
		if err := validateWithBlock(engram, v.Config, template); err != nil {
			agg.AddFieldError("spec.with", conditions.ReasonValidationFailed, err.Error())
		}
	}

	if agg.HasErrors() {
		return agg.ToFieldErrors().ToAggregate()
	}
	return nil
}

// requireTemplateRef ensures the Engram has a non-empty templateRef.name.
//
// Arguments:
//   - engram *bubushv1alpha1.Engram: the Engram to validate.
//
// Returns:
//   - nil if templateRef.name is set.
//   - Error if templateRef.name is empty.
func requireTemplateRef(engram *bubushv1alpha1.Engram) error {
	if engram.Spec.TemplateRef.Name == "" {
		return fmt.Errorf("spec.templateRef.name is required")
	}
	return nil
}

// fetchEngramTemplate retrieves the cluster-scoped EngramTemplate by name.
//
// Behavior:
//   - Fetches the EngramTemplate from the API server.
//   - Returns user-friendly error if template does not exist.
//
// Arguments:
//   - ctx context.Context: for API call.
//   - c client.Client: Kubernetes client.
//   - name string: EngramTemplate name.
//
// Returns:
//   - *v1alpha1.EngramTemplate if found.
//   - Error if not found or API call fails.
func fetchEngramTemplate(ctx context.Context, c client.Client, name string) (*v1alpha1.EngramTemplate, error) {
	var template v1alpha1.EngramTemplate
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: ""}, &template); err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("EngramTemplate '%s' not found", name)
		}
		return nil, fmt.Errorf("failed to get EngramTemplate '%s': %w", name, err)
	}
	return &template, nil
}

// validateWithBlock validates the Engram's spec.with block.
//
// Behavior:
//   - Skips validation if with block is empty.
//   - Ensures with block is a JSON object (not array or primitive).
//   - Ensures with block doesn't exceed configured max inline size.
//   - Validates against template's configSchema if defined.
//
// Arguments:
//   - engram *bubushv1alpha1.Engram: the Engram to validate.
//   - cfg *config.ControllerConfig: for max inline size; may be nil.
//   - template *v1alpha1.EngramTemplate: for configSchema validation.
//
// Returns:
//   - nil if with block is valid or absent.
//   - Error describing the validation failure.
func validateWithBlock(engram *bubushv1alpha1.Engram, cfg *config.ControllerConfig, template *v1alpha1.EngramTemplate) error {
	if engram.Spec.With == nil || len(engram.Spec.With.Raw) == 0 {
		return nil
	}
	b := TrimLeadingSpace(engram.Spec.With.Raw)
	if err := EnsureJSONObject("spec.with", b); err != nil {
		return err
	}
	maxBytes := PickMaxInlineBytes(cfg)
	if err := EnforceMaxBytes("spec.with", engram.Spec.With.Raw, maxBytes, "Provide large payloads via object storage and references instead of inlining"); err != nil {
		return err
	}
	if template.Spec.ConfigSchema != nil && len(template.Spec.ConfigSchema.Raw) > 0 {
		if err := validateJSONAgainstSchema(engram.Spec.With.Raw, template.Spec.ConfigSchema.Raw, "EngramTemplate"); err != nil {
			return err
		}
	}
	return nil
}
