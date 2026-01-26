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
var impulselog = logf.Log.WithName("impulse-resource")

type ImpulseWebhook struct {
	client.Client
	Config *config.ControllerConfig
}

// SetupWebhookWithManager registers the webhook for Impulse in the manager.
//
// Behavior:
//   - Stores the manager's client for template lookups during validation.
//   - Creates both defaulter and validator webhooks for Impulse resources.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//
// Returns:
//   - nil on success.
//   - Error if webhook registration fails.
func (wh *ImpulseWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	wh.Client = mgr.GetClient()

	return ctrl.NewWebhookManagedBy(mgr).For(&bubushv1alpha1.Impulse{}).
		WithValidator(&ImpulseCustomValidator{Client: wh.Client, Config: wh.Config}).
		WithDefaulter(&ImpulseCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-bubustack-io-v1alpha1-impulse,mutating=true,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=impulses,verbs=create;update,versions=v1alpha1,name=mimpulse-v1alpha1.kb.io,admissionReviewVersions=v1

// ImpulseCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Impulse when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type ImpulseCustomDefaulter struct {
	// No fields needed; Impulse defaults are intentionally minimal.
}

var _ webhook.CustomDefaulter = &ImpulseCustomDefaulter{}

// Default implements webhook.CustomDefaulter for Impulse resources.
//
// Behavior:
//   - Currently a no-op; Impulse defaults are intentionally minimal.
//   - Controller handles defaulting at reconcile time.
//
// Arguments:
//   - ctx context.Context: unused but required by interface.
//   - obj runtime.Object: expected to be *bubushv1alpha1.Impulse.
//
// Returns:
//   - nil on success.
//   - Error if obj is not an Impulse.
func (d *ImpulseCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	return DefaultResource[*bubushv1alpha1.Impulse](obj, "Impulse", impulselog, nil)
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-bubustack-io-v1alpha1-impulse,mutating=false,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=impulses,verbs=create;update,versions=v1alpha1,name=vimpulse-v1alpha1.kb.io,admissionReviewVersions=v1

// ImpulseCustomValidator struct is responsible for validating the Impulse resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type ImpulseCustomValidator struct {
	Client client.Client
	Config *config.ControllerConfig
}

var _ webhook.CustomValidator = &ImpulseCustomValidator{}

// ValidateCreate implements webhook.CustomValidator for Impulse creation.
//
// Behavior:
//   - Type-asserts obj to Impulse and validates spec.
//   - Validates required fields, with/mapping blocks, and workload mode.
//
// Arguments:
//   - ctx context.Context: for template lookup.
//   - obj runtime.Object: expected to be *bubushv1alpha1.Impulse.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if type assertion fails or validation errors exist.
func (v *ImpulseCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return ValidateCreateResource[*bubushv1alpha1.Impulse](ctx, obj, "Impulse", impulselog, v.validateImpulse)
}

// ValidateUpdate implements webhook.CustomValidator for Impulse updates.
//
// Behavior:
//   - Type-asserts newObj to Impulse and validates spec.
//   - Validates required fields, with/mapping blocks, and workload mode.
//
// Arguments:
//   - ctx context.Context: for template lookup.
//   - oldObj runtime.Object: previous Impulse state.
//   - newObj runtime.Object: proposed Impulse state.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if type assertion fails or validation errors exist.
func (v *ImpulseCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	impulse, ok := newObj.(*bubushv1alpha1.Impulse)
	if !ok {
		return nil, fmt.Errorf("expected a Impulse object for the newObj but got %T", newObj)
	}
	impulselog.Info("Validation for Impulse upon update", "name", impulse.GetName())

	if err := v.validateImpulse(ctx, impulse); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator for Impulse deletion.
//
// Behavior:
//   - Always allows deletion (no-op validation).
//   - Exists as scaffold placeholder; delete verb not enabled in annotation.
//
// Arguments:
//   - ctx context.Context: unused.
//   - obj runtime.Object: expected to be *bubushv1alpha1.Impulse.
//
// Returns:
//   - nil, nil (deletion always allowed).
func (v *ImpulseCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return ValidateDeleteResource[*bubushv1alpha1.Impulse](obj, "Impulse", impulselog, nil)
}

// validateImpulse performs basic invariants validation for Impulse specs.
//
// Behavior:
//   - Validates templateRef.name and storyRef.name are set.
//   - Validates with/mapping blocks are JSON objects (not array/primitive).
//   - Validates workload.mode is not "job" (Impulse must be always-on).
//   - Validates with/mapping against template schemas if defined.
//   - Aggregates multiple validation errors for comprehensive feedback.
//
// Arguments:
//   - ctx context.Context: for template lookup.
//   - impulse *bubushv1alpha1.Impulse: the Impulse to validate.
//
// Returns:
//   - nil if all validations pass.
//   - Aggregated field.ErrorList containing all validation failures.
func (v *ImpulseCustomValidator) validateImpulse(ctx context.Context, impulse *bubushv1alpha1.Impulse) error {
	agg := validation.NewAggregator()

	// Validate required fields first
	if impulse.Spec.TemplateRef.Name == "" {
		agg.AddFieldError("spec.templateRef.name", conditions.ReasonTemplateNotFound, "spec.templateRef.name is required")
	}
	if impulse.Spec.StoryRef.Name == "" {
		agg.AddFieldError("spec.storyRef.name", conditions.ReasonStoryReferenceInvalid, "spec.storyRef.name is required")
	}

	// Only proceed with template lookup if templateRef is valid
	var template *v1alpha1.ImpulseTemplate
	if impulse.Spec.TemplateRef.Name != "" {
		var err error
		template, err = v.fetchTemplate(ctx, impulse.Spec.TemplateRef.Name)
		if err != nil {
			agg.AddFieldError("spec.templateRef.name", conditions.ReasonTemplateNotFound, err.Error())
		}
	}

	// Validate with/mapping blocks if template was found
	if template != nil {
		if err := v.validateWithBlock(impulse, template); err != nil {
			agg.AddFieldError("spec.with", conditions.ReasonValidationFailed, err.Error())
		}
		if err := v.validateMappingBlock(impulse, template); err != nil {
			agg.AddFieldError("spec.mapping", conditions.ReasonValidationFailed, err.Error())
		}
	}

	// Validate workload mode regardless of template
	if err := v.validateWorkloadMode(impulse); err != nil {
		agg.AddFieldError("spec.workload.mode", conditions.ReasonValidationFailed, err.Error())
	}

	if agg.HasErrors() {
		return agg.ToFieldErrors().ToAggregate()
	}
	return nil
}

// fetchTemplate retrieves the cluster-scoped ImpulseTemplate by name.
//
// Behavior:
//   - Fetches the ImpulseTemplate from the API server.
//   - Returns user-friendly error if template does not exist.
//
// Arguments:
//   - ctx context.Context: for API call.
//   - name string: ImpulseTemplate name.
//
// Returns:
//   - *v1alpha1.ImpulseTemplate if found.
//   - Error if not found or API call fails.
func (v *ImpulseCustomValidator) fetchTemplate(ctx context.Context, name string) (*v1alpha1.ImpulseTemplate, error) {
	var template v1alpha1.ImpulseTemplate
	if err := v.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: ""}, &template); err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("ImpulseTemplate '%s' not found", name)
		}
		return nil, fmt.Errorf("failed to get ImpulseTemplate '%s': %w", name, err)
	}
	return &template, nil
}

// validateWithBlock validates the Impulse's spec.with block.
//
// Behavior:
//   - Skips validation if with block is empty.
//   - Ensures with block is a JSON object (not array or primitive).
//   - Ensures with block doesn't exceed configured max inline size.
//   - Validates against template's configSchema if defined.
//
// Arguments:
//   - impulse *bubushv1alpha1.Impulse: the Impulse to validate.
//   - template *v1alpha1.ImpulseTemplate: for configSchema validation.
//
// Returns:
//   - nil if with block is valid or absent.
//   - Error describing the validation failure.
func (v *ImpulseCustomValidator) validateWithBlock(impulse *bubushv1alpha1.Impulse, template *v1alpha1.ImpulseTemplate) error {
	if impulse.Spec.With == nil || len(impulse.Spec.With.Raw) == 0 {
		return nil
	}
	b := TrimLeadingSpace(impulse.Spec.With.Raw)
	if err := EnsureJSONObject("spec.with", b); err != nil {
		return err
	}
	maxBytes := PickMaxInlineBytes(v.Config)
	if err := EnforceMaxBytes("spec.with", impulse.Spec.With.Raw, maxBytes, "Provide large payloads via object storage and references instead of inlining"); err != nil {
		return err
	}
	if template.Spec.ConfigSchema != nil && len(template.Spec.ConfigSchema.Raw) > 0 {
		if err := validateJSONAgainstSchema(impulse.Spec.With.Raw, template.Spec.ConfigSchema.Raw, "ImpulseTemplate"); err != nil {
			return err
		}
	}
	return nil
}

// validateMappingBlock validates the Impulse's spec.mapping block.
//
// Behavior:
//   - Skips validation if mapping block is empty.
//   - Ensures mapping block is a JSON object (not array or primitive).
//   - Ensures mapping block doesn't exceed configured max inline size.
//   - Validates against template's contextSchema if defined.
//
// Arguments:
//   - impulse *bubushv1alpha1.Impulse: the Impulse to validate.
//   - template *v1alpha1.ImpulseTemplate: for contextSchema validation.
//
// Returns:
//   - nil if mapping block is valid or absent.
//   - Error describing the validation failure.
func (v *ImpulseCustomValidator) validateMappingBlock(impulse *bubushv1alpha1.Impulse, template *v1alpha1.ImpulseTemplate) error {
	if impulse.Spec.Mapping == nil || len(impulse.Spec.Mapping.Raw) == 0 {
		return nil
	}
	b := TrimLeadingSpace(impulse.Spec.Mapping.Raw)
	if err := EnsureJSONObject("spec.mapping", b); err != nil {
		return err
	}
	maxBytes := PickMaxInlineBytes(v.Config)
	if err := EnforceMaxBytes("spec.mapping", impulse.Spec.Mapping.Raw, maxBytes, "Provide large payloads via object storage and references instead of inlining"); err != nil {
		return err
	}
	if template.Spec.ContextSchema != nil && len(template.Spec.ContextSchema.Raw) > 0 {
		if err := validateJSONAgainstSchema(impulse.Spec.Mapping.Raw, template.Spec.ContextSchema.Raw, "ImpulseTemplate"); err != nil {
			return err
		}
	}
	return nil
}

// validateWorkloadMode ensures Impulse workload mode is not "job".
//
// Behavior:
//   - Rejects Impulses with workload.mode set to "job".
//   - Impulses must be long-running (always-on) processes.
//
// Arguments:
//   - impulse *bubushv1alpha1.Impulse: the Impulse to validate.
//
// Returns:
//   - nil if workload mode is valid or not set.
//   - Error if mode is "job".
func (v *ImpulseCustomValidator) validateWorkloadMode(impulse *bubushv1alpha1.Impulse) error {
	if impulse.Spec.Workload != nil && impulse.Spec.Workload.Mode == "job" {
		return fmt.Errorf("spec.workload.mode must not be 'job' for Impulse (must be always-on)")
	}
	return nil
}
