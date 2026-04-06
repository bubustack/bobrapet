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
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bobrapet/pkg/validation"
)

// log is for logging in this package.
var impulselog = logf.Log.WithName("impulse-resource")

type ImpulseWebhook struct {
	client.Client
	APIReader     client.Reader
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

// SetupWebhookWithManager registers the webhook for Impulse in the manager.
//
// Behavior:
//   - Stores the manager's API reader and cached client for validation lookups.
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
	wh.APIReader = mgr.GetAPIReader()

	return ctrl.NewWebhookManagedBy(mgr, &bubushv1alpha1.Impulse{}).
		WithValidator(&ImpulseCustomValidator{Client: wh.Client, APIReader: wh.APIReader, Config: wh.Config, ConfigManager: wh.ConfigManager}).
		WithDefaulter(&ImpulseCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-bubustack-io-v1alpha1-impulse,mutating=true,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=impulses,verbs=create;update,versions=v1alpha1,name=mimpulse-v1alpha1.kb.io,admissionReviewVersions=v1

// ImpulseCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Impulse when those are created or updated.
//
// NOTE: This is an internal webhook helper and is not registered in the API scheme.
// It does not implement runtime.Object, so no DeepCopy generation is needed or applied.
type ImpulseCustomDefaulter struct {
	// No fields needed; Impulse defaults are intentionally minimal.
}

var _ admission.Defaulter[*bubushv1alpha1.Impulse] = &ImpulseCustomDefaulter{}

// Default implements webhook.CustomDefaulter for Impulse resources.
//
// Behavior:
//   - Currently a no-op; Impulse defaults are intentionally minimal.
//   - Controller handles defaulting at reconcile time.
//
// Arguments:
//   - ctx context.Context: unused but required by interface.
//   - obj *bubushv1alpha1.Impulse: the Impulse to default.
//
// Returns:
//   - nil always (no defaulting logic).
func (d *ImpulseCustomDefaulter) Default(_ context.Context, obj *bubushv1alpha1.Impulse) error {
	impulselog.Info("Defaulting Impulse", "name", obj.GetName())
	return nil
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-bubustack-io-v1alpha1-impulse,mutating=false,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=impulses,verbs=create;update,versions=v1alpha1,name=vimpulse-v1alpha1.kb.io,admissionReviewVersions=v1

// ImpulseCustomValidator struct is responsible for validating the Impulse resource
// when it is created, updated, or deleted.
//
// NOTE: This is an internal webhook helper and is not registered in the API scheme.
// It does not implement runtime.Object, so no DeepCopy generation is needed or applied.
type ImpulseCustomValidator struct {
	Client        client.Client
	APIReader     client.Reader
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

var _ admission.Validator[*bubushv1alpha1.Impulse] = &ImpulseCustomValidator{}

// controllerConfig returns the live controller configuration when a ConfigManager
// is present, falling back to the static snapshot or compiled defaults otherwise.
func (v *ImpulseCustomValidator) controllerConfig() *config.ControllerConfig {
	return ResolveControllerConfig(impulselog, v.ConfigManager, v.Config)
}

func (v *ImpulseCustomValidator) lookupReader() client.Reader {
	if v.APIReader != nil {
		return v.APIReader
	}
	return v.Client
}

// ValidateCreate implements webhook.CustomValidator for Impulse creation.
//
// Behavior:
//   - Validates spec fields, with/mapping blocks, and workload mode.
//
// Arguments:
//   - ctx context.Context: for template lookup.
//   - impulse *bubushv1alpha1.Impulse: the Impulse being created.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if validation errors exist.
func (v *ImpulseCustomValidator) ValidateCreate(ctx context.Context, impulse *bubushv1alpha1.Impulse) (admission.Warnings, error) {
	impulselog.Info("Validation for Impulse upon creation", "name", impulse.GetName())
	warnings, err := v.validateImpulse(ctx, impulse)
	return warnings, err
}

// ValidateUpdate implements webhook.CustomValidator for Impulse updates.
//
// Behavior:
//   - Validates spec fields, with/mapping blocks, and workload mode.
//   - Short-circuits for deletion or spec-unchanged updates.
//
// Arguments:
//   - ctx context.Context: for template lookup.
//   - oldObj *bubushv1alpha1.Impulse: previous Impulse state.
//   - newObj *bubushv1alpha1.Impulse: proposed Impulse state.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if validation errors exist.
func (v *ImpulseCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *bubushv1alpha1.Impulse) (admission.Warnings, error) {
	impulselog.Info("Validation for Impulse upon update", "name", newObj.GetName())

	// Allow metadata-only updates during deletion (e.g., finalizer removal)
	if newObj.DeletionTimestamp != nil {
		return nil, nil
	}

	// Skip validation if the spec hasn't changed (metadata-only update).
	// Use normalizeImpulseSpec to canonicalize RawExtension fields before comparison:
	// reflect.DeepEqual on raw []byte is byte-level and can produce false mismatches
	// when semantically identical JSON differs only in formatting (e.g. after SSA round-trips).
	if reflect.DeepEqual(normalizeImpulseSpec(oldObj.Spec), normalizeImpulseSpec(newObj.Spec)) {
		return nil, nil
	}

	warnings, err := v.validateImpulse(ctx, newObj)
	return warnings, err
}

// ValidateDelete implements webhook.CustomValidator for Impulse deletion.
//
// Behavior:
//   - Always allows deletion (no-op validation).
//   - Exists as scaffold placeholder; delete verb not enabled in annotation.
//
// Arguments:
//   - ctx context.Context: unused.
//   - obj *bubushv1alpha1.Impulse: the Impulse being deleted.
//
// Returns:
//   - nil, nil (deletion always allowed).
func (v *ImpulseCustomValidator) ValidateDelete(_ context.Context, obj *bubushv1alpha1.Impulse) (admission.Warnings, error) {
	impulselog.Info("Validation for Impulse upon deletion", "name", obj.GetName())
	return nil, nil
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
//
//nolint:gocyclo // complex by design
func (v *ImpulseCustomValidator) validateImpulse(ctx context.Context, impulse *bubushv1alpha1.Impulse) (admission.Warnings, error) {
	agg := validation.NewAggregator()
	var warnings admission.Warnings
	reader := v.lookupReader()
	cfg := v.controllerConfig()

	// Validate required fields first
	if impulse.Spec.TemplateRef.Name == "" {
		agg.AddFieldError("spec.templateRef.name", conditions.ReasonTemplateNotFound, "spec.templateRef.name is required")
	}
	if impulse.Spec.StoryRef.Name == "" {
		agg.AddFieldError("spec.storyRef.name", conditions.ReasonStoryReferenceInvalid, "spec.storyRef.name is required")
	}
	if impulse.Spec.StoryRef.Name != "" {
		refCtx, refCancel := context.WithTimeout(ctx, 5*time.Second)
		defer refCancel()
		targetNamespace := refs.ResolveNamespace(impulse, &impulse.Spec.StoryRef.ObjectReference)
		if reader == nil &&
			strings.TrimSpace(targetNamespace) != "" &&
			strings.TrimSpace(targetNamespace) != strings.TrimSpace(impulse.Namespace) &&
			ResolveReferencePolicy(cfg) == config.ReferenceCrossNamespacePolicyGrant {
			agg.AddFieldError("spec.storyRef.namespace", conditions.ReasonStoryReferenceInvalid, "webhook reader is not configured for ReferenceGrant evaluation")
		} else if err := ValidateCrossNamespaceReference(
			refCtx,
			reader,
			cfg,
			impulse,
			"bubustack.io",
			"Impulse",
			"bubustack.io",
			"Story",
			targetNamespace,
			impulse.Spec.StoryRef.Name,
			"StoryRef",
		); err != nil {
			agg.AddFieldError("spec.storyRef.namespace", conditions.ReasonStoryReferenceInvalid, err.Error())
		}
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
	if template != nil && strings.TrimSpace(impulse.Spec.TemplateRef.Version) != "" && strings.TrimSpace(template.Spec.Version) != strings.TrimSpace(impulse.Spec.TemplateRef.Version) {
		agg.AddFieldError("spec.templateRef.version", conditions.ReasonValidationFailed, fmt.Sprintf("templateRef.version %q does not match template's actual version %q", impulse.Spec.TemplateRef.Version, template.Spec.Version))
	}

	// Story existence is a warning, not a hard error. The controller handles
	// "Story not found" during reconciliation (sets PhaseBlocked + emits event).
	// This avoids rejecting batch-applied resources where Story and Impulse are
	// created in the same kubectl apply.
	if impulse.Spec.StoryRef.Name != "" && reader != nil {
		storyCtx, storyCancel := context.WithTimeout(ctx, 5*time.Second)
		defer storyCancel()
		story := &bubushv1alpha1.Story{}
		storyKey := impulse.Spec.StoryRef.ToNamespacedName(impulse)
		if err := reader.Get(storyCtx, storyKey, story); err != nil {
			if apierrors.IsNotFound(err) {
				warnings = append(warnings, fmt.Sprintf("referenced Story '%s' not found yet; the controller will retry until it exists", storyKey.String()))
			} else {
				warnings = append(warnings, fmt.Sprintf("could not verify Story '%s': %v; the controller will retry", storyKey.String(), err))
			}
		} else if strings.TrimSpace(impulse.Spec.StoryRef.Version) != "" && strings.TrimSpace(story.Spec.Version) != strings.TrimSpace(impulse.Spec.StoryRef.Version) {
			warnings = append(warnings, fmt.Sprintf("Story version mismatch: expected %q, got %q", impulse.Spec.StoryRef.Version, story.Spec.Version))
		}
	}

	// Validate with/mapping blocks if template was found
	if template != nil {
		if err := v.validateSupportedModes(impulse, template); err != nil {
			agg.AddFieldError("spec.workload.mode", conditions.ReasonValidationFailed, err.Error())
		}
		if err := v.validateWithBlock(impulse, template); err != nil {
			agg.AddFieldError("spec.with", conditions.ReasonValidationFailed, err.Error())
		}
		if err := v.validateMappingBlock(impulse, template); err != nil {
			agg.AddFieldError("spec.mapping", conditions.ReasonValidationFailed, err.Error())
		}
		v.validateSecrets(agg, impulse, template)
	}

	// Validate workload mode regardless of template
	if err := v.validateWorkloadMode(impulse); err != nil {
		agg.AddFieldError("spec.workload.mode", conditions.ReasonValidationFailed, err.Error())
	}

	v.validateDeliveryPolicy(agg, impulse.Spec.DeliveryPolicy)
	v.validateThrottlePolicy(agg, impulse.Spec.Throttle)

	if agg.HasErrors() {
		return warnings, agg.ToFieldErrors().ToAggregate()
	}
	return warnings, nil
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
	reader := v.lookupReader()
	if reader == nil {
		return nil, fmt.Errorf("webhook reader is not configured")
	}
	var template v1alpha1.ImpulseTemplate
	webhookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := reader.Get(webhookCtx, types.NamespacedName{Name: name, Namespace: ""}, &template); err != nil {
		if apierrors.IsNotFound(err) {
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
	maxBytes := PickMaxInlineBytes(v.controllerConfig())
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
//   - Does not validate against template.Spec.ContextSchema: mapping values are
//     evaluated at runtime against the live event context, not a static payload,
//     so schema validation at admission time would reject valid template expressions.
//
// Arguments:
//   - impulse *bubushv1alpha1.Impulse: the Impulse to validate.
//   - template *v1alpha1.ImpulseTemplate: reserved for future contextSchema validation; unused today.
//
// Returns:
//   - nil if mapping block is valid or absent.
//   - Error describing the validation failure.
func (v *ImpulseCustomValidator) validateMappingBlock(impulse *bubushv1alpha1.Impulse, _ *v1alpha1.ImpulseTemplate) error {
	if impulse.Spec.Mapping == nil || len(impulse.Spec.Mapping.Raw) == 0 {
		return nil
	}
	b := TrimLeadingSpace(impulse.Spec.Mapping.Raw)
	if err := EnsureJSONObject("spec.mapping", b); err != nil {
		return err
	}
	maxBytes := PickMaxInlineBytes(v.controllerConfig())
	if err := EnforceMaxBytes("spec.mapping", impulse.Spec.Mapping.Raw, maxBytes, "Provide large payloads via object storage and references instead of inlining"); err != nil {
		return err
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

//nolint:gocyclo // complex by design
func (v *ImpulseCustomValidator) validateDeliveryPolicy(agg *validation.Aggregator, policy *bubushv1alpha1.TriggerDeliveryPolicy) {
	if agg == nil || policy == nil {
		return
	}

	if policy.Dedupe != nil {
		mode := ""
		if policy.Dedupe.Mode != nil {
			mode = strings.ToLower(strings.TrimSpace(string(*policy.Dedupe.Mode)))
		}
		keyTemplate := ""
		if policy.Dedupe.KeyTemplate != nil {
			keyTemplate = strings.TrimSpace(*policy.Dedupe.KeyTemplate)
		}

		if mode == string(bubushv1alpha1.TriggerDedupeKey) {
			if keyTemplate == "" {
				agg.AddFieldError("spec.deliveryPolicy.dedupe.keyTemplate", conditions.ReasonValidationFailed,
					"keyTemplate is required when dedupe.mode is 'key'")
			}
		} else if keyTemplate != "" {
			agg.AddFieldError("spec.deliveryPolicy.dedupe.keyTemplate", conditions.ReasonValidationFailed,
				"keyTemplate is only valid when dedupe.mode is 'key'")
		}
	}

	if policy.Retry != nil {
		if policy.Retry.MaxAttempts != nil && *policy.Retry.MaxAttempts < 0 {
			agg.AddFieldError("spec.deliveryPolicy.retry.maxAttempts", conditions.ReasonValidationFailed,
				"maxAttempts must be >= 0")
		}

		var baseDelay time.Duration
		if policy.Retry.BaseDelay != nil {
			raw := strings.TrimSpace(*policy.Retry.BaseDelay)
			parsed, err := parseNonNegativeDuration(raw)
			if err != nil {
				agg.AddFieldError("spec.deliveryPolicy.retry.baseDelay", conditions.ReasonValidationFailed,
					fmt.Sprintf("invalid baseDelay: %s", err.Error()))
			} else {
				baseDelay = parsed
			}
		}

		var maxDelay time.Duration
		if policy.Retry.MaxDelay != nil {
			raw := strings.TrimSpace(*policy.Retry.MaxDelay)
			parsed, err := parseNonNegativeDuration(raw)
			if err != nil {
				agg.AddFieldError("spec.deliveryPolicy.retry.maxDelay", conditions.ReasonValidationFailed,
					fmt.Sprintf("invalid maxDelay: %s", err.Error()))
			} else {
				maxDelay = parsed
			}
		}

		// Zero is explicitly allowed for either delay: maxDelay=0 means "no cap" per the
		// SDK convention, so we only cross-validate when both values are positive.
		if baseDelay > 0 && maxDelay > 0 && maxDelay < baseDelay {
			agg.AddFieldError("spec.deliveryPolicy.retry.maxDelay", conditions.ReasonValidationFailed,
				"maxDelay must be greater than or equal to baseDelay")
		}
	}
}

func (v *ImpulseCustomValidator) validateThrottlePolicy(agg *validation.Aggregator, throttle *bubushv1alpha1.TriggerThrottlePolicy) {
	if agg == nil || throttle == nil {
		return
	}
	if throttle.MaxInFlight != nil && *throttle.MaxInFlight < 0 {
		agg.AddFieldError("spec.throttle.maxInFlight", conditions.ReasonValidationFailed,
			"maxInFlight must be >= 0")
	}
	if throttle.RatePerSecond != nil && *throttle.RatePerSecond < 0 {
		agg.AddFieldError("spec.throttle.ratePerSecond", conditions.ReasonValidationFailed,
			"ratePerSecond must be >= 0")
	}
	if throttle.Burst != nil && *throttle.Burst < 0 {
		agg.AddFieldError("spec.throttle.burst", conditions.ReasonValidationFailed,
			"burst must be >= 0")
	}
}

func (v *ImpulseCustomValidator) validateSupportedModes(impulse *bubushv1alpha1.Impulse, template *v1alpha1.ImpulseTemplate) error {
	if impulse == nil || template == nil {
		return nil
	}
	mode := enums.WorkloadModeDeployment
	if impulse.Spec.Workload != nil && impulse.Spec.Workload.Mode != "" {
		mode = impulse.Spec.Workload.Mode
	}
	// Job mode is intentionally skipped here: the Impulse-level constraint
	// "mode must not be job" is enforced separately by validateWorkloadMode,
	// which runs unconditionally. Checking it against SupportedModes would
	// produce a second, redundant error on the same field.
	if mode == enums.WorkloadModeJob {
		return nil
	}
	if slices.Contains(template.Spec.SupportedModes, mode) {
		return nil
	}
	supportedModes := make([]string, 0, len(template.Spec.SupportedModes))
	for _, supported := range template.Spec.SupportedModes {
		supportedModes = append(supportedModes, string(supported))
	}
	return fmt.Errorf("workload mode %q is not supported by template (supportedModes: %s)", mode, strings.Join(supportedModes, ", "))
}

func (v *ImpulseCustomValidator) validateSecrets(agg *validation.Aggregator, impulse *bubushv1alpha1.Impulse, template *v1alpha1.ImpulseTemplate) {
	if agg == nil || impulse == nil || template == nil {
		return
	}
	templateSecrets := template.Spec.SecretSchema
	if len(templateSecrets) == 0 {
		for key := range impulse.Spec.Secrets {
			agg.AddFieldError("spec.secrets."+key, conditions.ReasonValidationFailed,
				fmt.Sprintf("secret %q is not defined in template secretSchema", key))
		}
		return
	}
	for key, def := range templateSecrets {
		if !def.Required {
			continue
		}
		if impulse.Spec.Secrets == nil {
			agg.AddFieldError("spec.secrets."+key, conditions.ReasonValidationFailed,
				fmt.Sprintf("secret %q is required by template", key))
			continue
		}
		if _, ok := impulse.Spec.Secrets[key]; !ok {
			agg.AddFieldError("spec.secrets."+key, conditions.ReasonValidationFailed,
				fmt.Sprintf("secret %q is required by template", key))
		}
	}
	for key := range impulse.Spec.Secrets {
		if _, ok := templateSecrets[key]; !ok {
			agg.AddFieldError("spec.secrets."+key, conditions.ReasonValidationFailed,
				fmt.Sprintf("secret %q is not defined in template secretSchema", key))
		}
	}
}

func parseNonNegativeDuration(raw string) (time.Duration, error) {
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse duration: %w", err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("duration must be >= 0")
	}
	return parsed, nil
}

// normalizeImpulseSpec returns a copy of spec with RawExtension fields canonicalized.
// This is used by ValidateUpdate's spec-equality check to prevent false mismatches
// caused by byte-level JSON formatting differences (e.g. whitespace or key ordering)
// that can arise after Server-Side Apply or other API server round-trips.
func normalizeImpulseSpec(spec bubushv1alpha1.ImpulseSpec) bubushv1alpha1.ImpulseSpec {
	out := spec
	out.With = normalizeRawExtension(spec.With)
	out.Mapping = normalizeRawExtension(spec.Mapping)
	return out
}

// normalizeRawExtension returns a copy of r with its Raw bytes in canonical JSON form
// (unmarshalled then re-marshalled). Returns r unchanged when Raw is empty or on any error.
func normalizeRawExtension(r *runtime.RawExtension) *runtime.RawExtension {
	if r == nil || len(r.Raw) == 0 {
		return r
	}
	var v any
	if err := json.Unmarshal(r.Raw, &v); err != nil {
		return r
	}
	b, err := json.Marshal(v)
	if err != nil {
		return r
	}
	return &runtime.RawExtension{Raw: b}
}
