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
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
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
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

// SetupWebhookWithManager registers the webhook for Engram in the manager.
//
// Behavior:
//   - Creates both defaulter and validator webhooks for Engram resources.
//   - Uses mgr.GetAPIReader() for template lookups to avoid cache staleness.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//
// Returns:
//   - nil on success.
//   - Error if webhook registration fails.
func (wh *EngramWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&bubushv1alpha1.Engram{}).
		WithValidator(&EngramCustomValidator{
			Client:        mgr.GetAPIReader(),
			Config:        wh.Config,
			ConfigManager: wh.ConfigManager,
		}).
		WithDefaulter(&EngramCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-bubustack-io-v1alpha1-engram,mutating=true,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=engrams,verbs=create;update,versions=v1alpha1,name=mengram-v1alpha1.kb.io,admissionReviewVersions=v1

// EngramCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Engram when those are created or updated.
//
// +kubebuilder:object:generate=false
// NOTE: This is an internal webhook helper and is not registered in the API scheme.
// It does not implement runtime.Object, so no DeepCopy generation is needed or applied.
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
// +kubebuilder:object:generate=false
// NOTE: This is an internal webhook helper and is not registered in the API scheme.
// It does not implement runtime.Object, so no DeepCopy generation is needed or applied.
type EngramCustomValidator struct {
	Client        client.Reader
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

// controllerConfig returns the live controller configuration when a ConfigManager
// is present, falling back to the static snapshot or compiled defaults otherwise.
func (v *EngramCustomValidator) controllerConfig() *config.ControllerConfig {
	return ResolveControllerConfig(engramlog, v.ConfigManager, v.Config)
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
	engram, ok := obj.(*bubushv1alpha1.Engram)
	if !ok {
		return nil, fmt.Errorf("expected a Engram object but got %T", obj)
	}
	engramlog.Info("Validation for Engram upon creation", "name", engram.GetName())
	return v.validateEngram(ctx, engram)
}

// ValidateUpdate implements webhook.CustomValidator for Engram updates.
//
// Behavior:
//   - Skips validation for resources undergoing deletion (DeletionTimestamp set).
//   - Rejects spec changes while deletion is in progress (only finalizer edits allowed).
//   - Skips validation when spec is semantically unchanged; normalises RawExtension
//     fields before comparison to prevent false mismatches after SSA round-trips.
//   - Validates templateRef.name is set and template exists.
//   - Validates with block is a JSON object and within size limits.
//
// Arguments:
//   - ctx context.Context: for template lookup.
//   - oldObj runtime.Object: previous Engram state.
//   - newObj runtime.Object: proposed Engram state.
//
// Returns:
//   - nil, nil if validation passes or is skipped.
//   - nil, error if type assertion fails or validation errors exist.
func (v *EngramCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	engram, ok := newObj.(*bubushv1alpha1.Engram)
	if !ok {
		return nil, fmt.Errorf("expected a Engram object for the newObj but got %T", newObj)
	}

	// Check deletion first, before logging, to suppress noisy log entries for
	// resources undergoing deletion.
	if engram.DeletionTimestamp != nil {
		oldEngram, ok := oldObj.(*bubushv1alpha1.Engram)
		if !ok {
			return nil, fmt.Errorf("expected a Engram object for the oldObj but got %T", oldObj)
		}
		if reflect.DeepEqual(normalizeEngramSpec(oldEngram.Spec), normalizeEngramSpec(engram.Spec)) {
			return nil, nil
		}
		return nil, fmt.Errorf("spec updates are not permitted while deletion is in progress; only metadata.finalizers changes are allowed")
	}

	engramlog.Info("Validation for Engram upon update", "name", engram.GetName())

	// Skip validation if the spec hasn't changed (metadata-only update).
	// Normalise RawExtension fields first to avoid false mismatches caused by
	// byte-level JSON differences (e.g. whitespace or key ordering) that arise
	// after Server-Side Apply or other API server round-trips.
	if oldEngram, ok := oldObj.(*bubushv1alpha1.Engram); ok {
		if reflect.DeepEqual(normalizeEngramSpec(oldEngram.Spec), normalizeEngramSpec(engram.Spec)) {
			return nil, nil
		}
	}

	return v.validateEngram(ctx, engram)
}

// normalizeEngramSpec returns a copy of spec with RawExtension fields in canonical JSON form.
// Used by ValidateUpdate's equality checks to prevent false mismatches after SSA round-trips.
func normalizeEngramSpec(spec bubushv1alpha1.EngramSpec) bubushv1alpha1.EngramSpec {
	out := spec
	out.With = normalizeRawExtension(spec.With)
	return out
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
//   - Validates templateRef.name is set.
//   - Validates referenced EngramTemplate exists.
//   - Validates spec.mode against template supportedModes when set.
//   - Validates with block is a JSON object and within size limits.
//   - Validates with block against template's configSchema if defined.
//   - Validates secrets against the template secretSchema (required + unknown).
//   - Aggregates multiple validation errors for comprehensive feedback.
//
// Arguments:
//   - ctx context.Context: for template lookup.
//   - engram *bubushv1alpha1.Engram: the Engram to validate.
//
// Returns:
//   - Warnings and nil if all validations pass.
//   - Warnings and aggregated field.ErrorList containing all validation failures.
func (v *EngramCustomValidator) validateEngram(ctx context.Context, engram *bubushv1alpha1.Engram) (admission.Warnings, error) {
	var warnings admission.Warnings
	agg := validation.NewAggregator()

	if err := requireTemplateRef(engram); err != nil {
		agg.AddFieldError("spec.templateRef.name", conditions.ReasonTemplateNotFound, err.Error())
	}

	// Only proceed with template lookup if templateRef is valid.
	// Trim whitespace before the guard and lookup so a name like "  " is
	// treated as absent rather than producing a misleading "not found" error.
	var template *v1alpha1.EngramTemplate
	if templateName := strings.TrimSpace(engram.Spec.TemplateRef.Name); templateName != "" {
		var err error
		template, err = fetchEngramTemplate(ctx, v.Client, templateName)
		if err != nil {
			agg.AddFieldError("spec.templateRef.name", conditions.ReasonTemplateNotFound, err.Error())
		}
	}
	if template != nil && strings.TrimSpace(engram.Spec.TemplateRef.Version) != "" && strings.TrimSpace(template.Spec.Version) != strings.TrimSpace(engram.Spec.TemplateRef.Version) {
		agg.AddFieldError("spec.templateRef.version", conditions.ReasonValidationFailed, fmt.Sprintf("templateRef.version %q does not match template's actual version %q", engram.Spec.TemplateRef.Version, template.Spec.Version))
	}

	// Validate with block if template was found
	if template != nil {
		if err := v.validateSupportedModes(engram, template); err != nil {
			agg.AddFieldError("spec.mode", conditions.ReasonValidationFailed, err.Error())
		}
		if err := validateWithBlock(engram, v.controllerConfig(), template); err != nil {
			agg.AddFieldError("spec.with", conditions.ReasonValidationFailed, err.Error())
		}
		secretWarnings, err := v.validateSecrets(ctx, agg, engram, template)
		warnings = append(warnings, secretWarnings...)
		if err != nil {
			return warnings, err
		}
	}

	if agg.HasErrors() {
		return warnings, agg.ToFieldErrors().ToAggregate()
	}
	return warnings, nil
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
	if strings.TrimSpace(engram.Spec.TemplateRef.Name) == "" {
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
//   - c client.Reader: Kubernetes reader (non-cached is recommended).
//   - name string: EngramTemplate name.
//
// Returns:
//   - *v1alpha1.EngramTemplate if found.
//   - Error if not found or API call fails.
func fetchEngramTemplate(ctx context.Context, c client.Reader, name string) (*v1alpha1.EngramTemplate, error) {
	var template v1alpha1.EngramTemplate
	webhookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := c.Get(webhookCtx, types.NamespacedName{Name: name, Namespace: ""}, &template); err != nil {
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
	if err := EnforceMaxBytes("spec.with", b, maxBytes, "Provide large payloads via object storage and references instead of inlining"); err != nil {
		return err
	}
	if template.Spec.ConfigSchema != nil && len(template.Spec.ConfigSchema.Raw) > 0 {
		if err := validateJSONAgainstSchema(b, template.Spec.ConfigSchema.Raw, "EngramTemplate"); err != nil {
			return err
		}
	}
	return nil
}

func (v *EngramCustomValidator) validateSupportedModes(engram *bubushv1alpha1.Engram, template *v1alpha1.EngramTemplate) error {
	mode := engram.Spec.Mode
	if mode == "" {
		return nil
	}
	supportedModes := make([]string, 0, len(template.Spec.SupportedModes))
	for _, supported := range template.Spec.SupportedModes {
		if supported == mode {
			return nil
		}
		supportedModes = append(supportedModes, string(supported))
	}
	return fmt.Errorf("workload mode %q is not supported by template (supportedModes: %s)", mode, strings.Join(supportedModes, ", "))
}

func (v *EngramCustomValidator) validateSecrets(ctx context.Context, agg *validation.Aggregator, engram *bubushv1alpha1.Engram, template *v1alpha1.EngramTemplate) (admission.Warnings, error) {
	var warnings admission.Warnings
	if agg == nil || engram == nil || template == nil {
		return warnings, nil
	}
	templateSecrets := template.Spec.SecretSchema
	if len(templateSecrets) == 0 {
		for key := range engram.Spec.Secrets {
			agg.AddFieldError("spec.secrets."+key, conditions.ReasonValidationFailed,
				fmt.Sprintf("secret %q is not defined in template secretSchema", key))
		}
		return warnings, nil
	}
	for key, def := range templateSecrets {
		if !def.Required {
			continue
		}
		if engram.Spec.Secrets == nil {
			agg.AddFieldError("spec.secrets."+key, conditions.ReasonValidationFailed,
				fmt.Sprintf("secret %q is required by template", key))
			continue
		}
		if _, ok := engram.Spec.Secrets[key]; !ok {
			agg.AddFieldError("spec.secrets."+key, conditions.ReasonValidationFailed,
				fmt.Sprintf("secret %q is required by template", key))
		}
	}
	for key := range engram.Spec.Secrets {
		if _, ok := templateSecrets[key]; !ok {
			agg.AddFieldError("spec.secrets."+key, conditions.ReasonValidationFailed,
				fmt.Sprintf("secret %q is not defined in template secretSchema", key))
		}
	}
	if v.Client == nil {
		return warnings, nil
	}
	if engram.Namespace == "" {
		return warnings, nil
	}
	for key, name := range engram.Spec.Secrets {
		if _, ok := templateSecrets[key]; !ok {
			continue
		}
		if strings.TrimSpace(name) == "" {
			warnings = append(warnings, fmt.Sprintf("secret %q references an empty Secret name", key))
			continue
		}
		if err := fetchSecret(ctx, v.Client, engram.Namespace, name); err != nil {
			if errors.IsNotFound(err) {
				warnings = append(warnings, fmt.Sprintf("secret %q references Secret %q in namespace %q not found yet; the controller will retry", key, name, engram.Namespace))
				continue
			}
			return warnings, fmt.Errorf("failed to validate secret %q (%s/%s): %w", key, engram.Namespace, name, err)
		}
	}
	return warnings, nil
}

func fetchSecret(ctx context.Context, c client.Reader, namespace, name string) error {
	var secret corev1.Secret
	webhookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.Get(webhookCtx, types.NamespacedName{Namespace: namespace, Name: name}, &secret)
}
