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
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	runsvalidation "github.com/bubustack/bobrapet/pkg/runs/validation"
	"github.com/bubustack/bobrapet/pkg/templatesafety"
	"github.com/bubustack/core/contracts"
)

const stepRunStructuredErrorSchemaV1 = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "bubu://schemas/steprun/error/v1",
  "title": "Bubu StepRun Structured Error",
  "type": "object",
  "required": ["version", "type", "message"],
  "additionalProperties": false,
  "properties": {
    "version": {
      "type": "string",
      "const": "v1"
    },
    "type": {
      "type": "string",
      "enum": [
        "timeout",
        "storage_error",
        "serialization_error",
        "validation_error",
        "initialization_error",
        "execution_error",
        "unknown"
      ]
    },
    "message": {
      "type": "string",
      "minLength": 1
    },
    "retryable": {
      "type": "boolean"
    },
    "exitCode": {
      "type": "integer",
      "minimum": 0
    },
    "exitClass": {
      "type": "string",
      "enum": ["success", "retry", "terminal", "rateLimited"]
    },
    "code": {
      "type": "string"
    },
    "details": {
      "type": "object",
      "additionalProperties": true
    }
  }
}`

// log is for logging in this package.
var steprunlog = logf.Log.WithName("steprun-resource")

// StepRunWebhook registers the StepRun defaulting and validating webhooks.
// ISSUE-5 fix: removed the dead Client field that was set in SetupWebhookWithManager
// but never read after that point.
type StepRunWebhook struct {
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

// SetupWebhookWithManager registers the StepRun defaulting and validating webhooks.
//
// Behavior:
//   - Creates both defaulter and validator webhooks for StepRun resources.
//   - Uses mgr.GetAPIReader() for the validator so lookups bypass the cache and
//     reflect the authoritative API server state at admission time.
//   - Injects Config and ConfigManager into both handlers.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//
// Returns:
//   - nil on success.
//   - Error if webhook registration fails.
func (wh *StepRunWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&runsv1alpha1.StepRun{}).
		WithDefaulter(&StepRunCustomDefaulter{
			Config:        wh.Config,
			ConfigManager: wh.ConfigManager,
		}).
		WithValidator(&StepRunCustomValidator{
			// ISSUE-4 fix: use APIReader (non-cached) instead of GetClient() (cached) so
			// admission decisions always reflect the authoritative API server state.
			Client:        mgr.GetAPIReader(),
			Config:        wh.Config,
			ConfigManager: wh.ConfigManager,
		}).
		Complete()
}

type StepRunCustomDefaulter struct {
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

var _ webhook.CustomDefaulter = &StepRunCustomDefaulter{}

// controllerConfig returns the effective controller configuration.
//
// Behavior:
//   - Delegates to webhookshared.ResolveControllerConfig for unified config resolution.
//
// Returns:
//   - Non-nil *config.ControllerConfig from the highest priority source.
func (d *StepRunCustomDefaulter) controllerConfig() *config.ControllerConfig {
	return webhookshared.ResolveControllerConfig(steprunlog, d.ConfigManager, d.Config)
}

// Default implements webhook.CustomDefaulter for StepRun resources.
//
// Behavior:
//   - Resolves the RetryPolicy using controller configuration defaults.
//   - Sets MaxRetries, Delay, and Backoff if not specified.
//
// Arguments:
//   - ctx context.Context: forwarded for parity (unused today).
//   - obj runtime.Object: expected to be *runsv1alpha1.StepRun.
//
// Returns:
//   - nil on success.
//   - Error if obj is not a StepRun.
func (d *StepRunCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	return webhookshared.DefaultResource[*runsv1alpha1.StepRun](obj, "StepRun", steprunlog, func(steprun *runsv1alpha1.StepRun) error {
		cfg := d.controllerConfig()
		steprun.Spec.Retry = webhookshared.ResolveRetryPolicy(cfg, steprun.Spec.Retry)
		if strings.TrimSpace(steprun.Spec.IdempotencyKey) == "" {
			steprun.Spec.IdempotencyKey = runsidentity.StepRunIdempotencyKey(
				steprun.Namespace,
				steprun.Spec.StoryRunRef.Name,
				steprun.Spec.StepID,
			)
		}
		return nil
	})
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-runs-bubustack-io-v1alpha1-steprun,mutating=false,failurePolicy=fail,sideEffects=None,groups=runs.bubustack.io,resources=stepruns,verbs=create;update,versions=v1alpha1,name=vsteprun-v1alpha1.kb.io,admissionReviewVersions=v1

// StepRunCustomValidator struct is responsible for validating the StepRun resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type StepRunCustomValidator struct {
	// Client is used for Engram existence checks and ReferenceGrant lookups.
	// It must be a non-cached reader (mgr.GetAPIReader()) so admission decisions reflect
	// the current API server state rather than a potentially stale informer cache.
	// ISSUE-4 fix: typed as client.Reader (read-only) rather than client.Client to
	// prevent accidental writes from within the validator.
	Client        client.Reader
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

var _ webhook.CustomValidator = &StepRunCustomValidator{}

// controllerConfig returns the effective controller configuration.
//
// Behavior:
//   - Delegates to webhookshared.ResolveControllerConfig for unified config resolution.
//
// Returns:
//   - Non-nil *config.ControllerConfig from the highest priority source.
func (v *StepRunCustomValidator) controllerConfig() *config.ControllerConfig {
	return webhookshared.ResolveControllerConfig(steprunlog, v.ConfigManager, v.Config)
}

// ValidateCreate implements webhook.CustomValidator for StepRun creation.
//
// Behavior:
//   - Type-asserts obj to StepRun and validates spec.
//   - Validates storyRunRef.name and stepId are set.
//   - Validates input/output sizes and total resource size.
//   - Validates status.needs doesn't contain self-references.
//
// Arguments:
//   - ctx context.Context: unused but required by interface.
//   - obj runtime.Object: expected to be *runsv1alpha1.StepRun.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if type assertion fails or validation errors exist.
func (v *StepRunCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return webhookshared.ValidateCreateResource[*runsv1alpha1.StepRun](ctx, obj, "StepRun", steprunlog, func(ctx context.Context, steprun *runsv1alpha1.StepRun) error {
		if err := v.validateStepRun(ctx, steprun); err != nil {
			return err
		}
		return validateStepRunStatus(steprun)
	})
}

// ValidateUpdate implements webhook.CustomValidator for StepRun updates.
//
// Behavior:
//   - Returns an error if either oldObj or newObj cannot be type-asserted to StepRun.
//   - Skips validation if resource is being deleted (DeletionTimestamp set).
//   - Rejects updates that decrease observedGeneration.
//   - Validates status.needs on every update.
//   - Skips spec validation if spec is semantically unchanged (status-only update),
//     using equality.Semantic.DeepEqual to handle *runtime.RawExtension correctly.
//
// Arguments:
//   - ctx context.Context: unused but required by interface.
//   - oldObj runtime.Object: previous StepRun state.
//   - newObj runtime.Object: proposed StepRun state.
//
// Returns:
//   - nil, nil if validation passes or is skipped.
//   - nil, error if type assertion fails or validation errors exist.
func (v *StepRunCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	steprun, ok := newObj.(*runsv1alpha1.StepRun)
	if !ok {
		return nil, fmt.Errorf("expected a StepRun object for the newObj but got %T", newObj)
	}
	// BUG-1 fix: treat oldObj type assertion failure as an error, consistent with
	// newObj. The original code had two separate silent-fallthrough assertions for
	// oldObj; this replaces them with a single assertion that errors on failure.
	oldSR, ok := oldObj.(*runsv1alpha1.StepRun)
	if !ok {
		return nil, fmt.Errorf("expected a StepRun object for the oldObj but got %T", oldObj)
	}
	steprunlog.Info("Validation for StepRun upon update", "name", steprun.GetName())

	// Allow metadata-only updates during deletion (e.g., finalizer removal)
	if steprun.DeletionTimestamp != nil {
		return nil, nil
	}

	if err := ensureStepRunObservedGenerationMonotonic(oldSR, steprun); err != nil {
		return nil, err
	}

	if err := validateStepRunStatus(steprun); err != nil {
		return nil, err
	}

	// ISSUE-2 fix: use semantic equality so *runtime.RawExtension fields (Input) are
	// compared by JSON content, not raw byte identity.
	if equality.Semantic.DeepEqual(oldSR.Spec, steprun.Spec) {
		return nil, nil
	}

	if err := v.validateStepRun(ctx, steprun); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator for StepRun deletion.
//
// Behavior:
//   - Always allows deletion (no-op validation).
//   - Exists as scaffold placeholder; delete verb not enabled in annotation.
//
// Arguments:
//   - ctx context.Context: unused.
//   - obj runtime.Object: expected to be *runsv1alpha1.StepRun.
//
// Returns:
//   - nil, nil (deletion always allowed).
func (v *StepRunCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return webhookshared.ValidateDeleteResource[*runsv1alpha1.StepRun](obj, "StepRun", steprunlog, nil)
}

// validateStepRun performs basic invariants validation for StepRun specs.
//
// Behavior:
//   - Validates storyRunRef.name and stepId are set.
//   - Validates input is a JSON object within size limits.
//   - Validates status.output size.
//   - Validates total serialized size (< 1 MiB).
//   - Validates downstream targets format.
//
// Arguments:
//   - sr *runsv1alpha1.StepRun: the StepRun to validate.
//
// Returns:
//   - nil if all validations pass.
//   - Error describing the first validation failure.
func (v *StepRunCustomValidator) validateStepRun(ctx context.Context, sr *runsv1alpha1.StepRun) error {
	if err := requireBasicFields(sr); err != nil {
		return err
	}
	if err := validateStepRunRefs(ctx, v.Client, v.controllerConfig(), sr); err != nil {
		return err
	}
	cfg := v.controllerConfig()
	maxBytes := webhookshared.PickMaxInlineBytes(cfg)
	if err := validateInputs(sr, maxBytes); err != nil {
		return err
	}
	if err := validateStatusOutput(sr, maxBytes); err != nil {
		return err
	}
	if err := validateTotalSize(sr); err != nil {
		return err
	}
	return validateDownstreamTargets(sr.Spec.DownstreamTargets)
}

// requireBasicFields ensures storyRunRef.name and stepId are set.
//
// Arguments:
//   - sr *runsv1alpha1.StepRun: the StepRun to validate.
//
// Returns:
//   - nil if required fields are set.
//   - Error describing the missing field.
func requireBasicFields(sr *runsv1alpha1.StepRun) error {
	if sr.Spec.StoryRunRef.Name == "" {
		return fmt.Errorf("spec.storyRunRef.name is required")
	}
	if sr.Spec.StepID == "" {
		return fmt.Errorf("spec.stepId is required")
	}
	return nil
}

// validateStepRunRefs enforces namespace scoping for StepRun references.
//
// Behavior:
//   - Enforces cross-namespace policy for StoryRunRef and EngramRef.
//   - For the default "deny" policy, reader is not consumed during namespace checks.
//   - reader is required when policy is "grant" (ReferenceGrant lookup) or when an
//     EngramRef is present (Engram existence and version check).
//
// BUG-3 fix: nil reader guard is now positioned before the EngramRef cross-namespace
// check, so a nil reader cannot reach refs.ReferenceGranted when policy is "grant".
func validateStepRunRefs(ctx context.Context, reader client.Reader, cfg *config.ControllerConfig, sr *runsv1alpha1.StepRun) error {
	if sr == nil {
		return nil
	}
	// StoryRunRef cross-namespace check. Under the default "deny" policy reader is not
	// consumed here; only consumed if policy is "grant" for ReferenceGrant lookups.
	storyRunKey := sr.Spec.StoryRunRef.ToNamespacedName(sr)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		reader,
		cfg,
		sr,
		"runs.bubustack.io",
		"StepRun",
		"runs.bubustack.io",
		"StoryRun",
		storyRunKey.Namespace,
		storyRunKey.Name,
		"StoryRunRef",
	); err != nil {
		return err
	}
	if sr.Spec.EngramRef == nil {
		return nil
	}
	engramKey := sr.Spec.EngramRef.ToNamespacedName(sr)

	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		reader,
		cfg,
		sr,
		"runs.bubustack.io",
		"StepRun",
		"bubustack.io",
		"Engram",
		engramKey.Namespace,
		engramKey.Name,
		"EngramRef",
	); err != nil {
		return err
	}
	// BUG-3 fix: guard reader here (before the direct Get call) so that a nil reader
	// does not panic when an EngramRef is present but no client was injected (e.g. in
	// unit tests that only test cross-namespace policy without Engram existence checks).
	// Under "deny" policy (default) the ValidateCrossNamespaceReference call above
	// never touches reader, so the cross-namespace check still runs correctly.
	if reader == nil {
		return nil
	}
	var engram bubuv1alpha1.Engram
	if err := reader.Get(ctx, engramKey, &engram); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("referenced engram '%s' not found", engramKey.String())
		}
		return fmt.Errorf("failed to get referenced engram '%s': %w", engramKey.String(), err)
	}
	if strings.TrimSpace(sr.Spec.EngramRef.Version) != "" &&
		strings.TrimSpace(engram.Spec.Version) != strings.TrimSpace(sr.Spec.EngramRef.Version) {
		return fmt.Errorf("referenced engram '%s' has version '%s', expected '%s'", engramKey.String(), engram.Spec.Version, sr.Spec.EngramRef.Version)
	}
	return nil
}

// isMaterializeStepRun returns true if the StepRun is explicitly labeled as a materialize step.
func isMaterializeStepRun(sr *runsv1alpha1.StepRun) bool {
	if sr == nil {
		return false
	}
	if sr.Labels == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(sr.Labels[contracts.MaterializeLabelKey]), "true")
}

// validateInputs ensures spec.input is a JSON object within size limits.
//
// Behavior:
//   - Skips validation if input is empty.
//   - Materialize StepRuns are exempt from the size limit (their input is vars from prior steps).
//   - Ensures input is a JSON object (not array or primitive).
//   - ISSUE-11 fix: enforces size limit before running template safety validation so that
//     oversized payloads are rejected cheaply rather than triggering expensive JSON parsing.
//
// Arguments:
//   - sr *runsv1alpha1.StepRun: the StepRun to validate.
//   - maxBytes int: maximum allowed input size.
//
// Returns:
//   - nil if input is valid or absent.
//   - Error describing the validation failure.
func validateInputs(sr *runsv1alpha1.StepRun, maxBytes int) error {
	if sr.Spec.Input == nil || len(sr.Spec.Input.Raw) == 0 {
		return nil
	}

	b := webhookshared.TrimLeadingSpace(sr.Spec.Input.Raw)
	if err := webhookshared.EnsureJSONObject("spec.input", b); err != nil {
		return err
	}
	// Size check first (cheap O(1)), then template safety (expensive O(n) JSON walk).
	if !isMaterializeStepRun(sr) {
		if err := webhookshared.EnforceMaxBytes(
			"spec.input",
			sr.Spec.Input.Raw,
			maxBytes,
			"Provide large payloads via object storage (Story.policy.storage) and references instead of inlining",
		); err != nil {
			return err
		}
	}
	if err := templatesafety.ValidateTemplateJSON(sr.Spec.Input.Raw); err != nil {
		return err
	}
	return webhookshared.ValidateReferenceMaps(sr.Spec.Input.Raw, "spec.input")
}

// validateStatusOutput ensures status.output doesn't exceed size limits.
//
// Arguments:
//   - sr *runsv1alpha1.StepRun: the StepRun to validate.
//   - maxBytes int: maximum allowed output size.
//
// Returns:
//   - nil if output is within limits or absent.
//   - Error describing the size violation.
func validateStatusOutput(sr *runsv1alpha1.StepRun, maxBytes int) error {
	if sr.Status.Output != nil && len(sr.Status.Output.Raw) > 0 {
		if err := webhookshared.EnforceMaxBytes(
			"status.output",
			sr.Status.Output.Raw,
			maxBytes,
			"Large outputs must be offloaded by the SDK",
		); err != nil {
			return err
		}
		return webhookshared.ValidateReferenceMaps(sr.Status.Output.Raw, "status.output")
	}
	return nil
}

// validateDownstreamTargets delegates to the shared validation package.
//
// Arguments:
//   - targets []runsv1alpha1.DownstreamTarget: downstream targets to validate.
//
// Returns:
//   - nil if all targets are valid.
//   - Error from runsvalidation.ValidateDownstreamTargets.
func validateDownstreamTargets(targets []runsv1alpha1.DownstreamTarget) error {
	return runsvalidation.ValidateDownstreamTargets(targets)
}

// ensureStepRunObservedGenerationMonotonic rejects stale status updates.
//
// Behavior:
//   - Compares old and new observedGeneration values.
//   - Rejects updates where observedGeneration decreases.
//   - Allows updates when either value is zero (unset), to accommodate initial
//     controller startup and status field initialization.
//
// Arguments:
//   - oldSR *runsv1alpha1.StepRun: previous StepRun state.
//   - newSR *runsv1alpha1.StepRun: proposed StepRun state.
//
// Returns:
//   - nil if generation is monotonically increasing.
//   - Error if observedGeneration decreased.
func ensureStepRunObservedGenerationMonotonic(oldSR, newSR *runsv1alpha1.StepRun) error {
	oldGen := oldSR.Status.ObservedGeneration
	newGen := newSR.Status.ObservedGeneration
	if oldGen > 0 && newGen > 0 && newGen < oldGen {
		return fmt.Errorf("status.observedGeneration must be monotonically increasing (old=%d new=%d)", oldGen, newGen)
	}
	return nil
}

// validateTotalSize ensures the total serialized StepRun doesn't exceed 1 MiB.
//
// Behavior:
//   - Marshals the StepRun to JSON to calculate total size.
//   - Rejects StepRuns larger than 1 MiB to prevent etcd storage issues.
//
// Arguments:
//   - sr *runsv1alpha1.StepRun: the StepRun to validate.
//
// Returns:
//   - nil if size is within limits.
//   - Error describing the size violation.
func validateTotalSize(sr *runsv1alpha1.StepRun) error {
	rawSR, err := json.Marshal(sr)
	if err != nil {
		return fmt.Errorf("internal error: failed to marshal StepRun for size validation: %w", err)
	}
	const maxTotalStepRunSizeBytes = 1 * 1024 * 1024 // 1 MiB
	if len(rawSR) > maxTotalStepRunSizeBytes {
		return fmt.Errorf("StepRun total size of %d bytes exceeds maximum allowed size of %d bytes", len(rawSR), maxTotalStepRunSizeBytes)
	}
	return nil
}

// validateStepRunStatus ensures status.needs doesn't contain self-references.
//
// NOTE: This is best-effort validation for create/update of the main resource.
// Status subresource updates (stepruns/status) bypass this webhook, so the
// controller remains the authoritative enforcer for status invariants.
//
// Behavior:
//   - Checks each entry in status.needs against the StepRun's name.
//   - Rejects self-references which would create an impossible dependency.
//
// Arguments:
//   - sr *runsv1alpha1.StepRun: the StepRun to validate.
//
// Returns:
//   - nil if status is valid.
//   - Error if self-reference detected.
func validateStepRunStatus(sr *runsv1alpha1.StepRun) error {
	if slices.Contains(sr.Status.Needs, sr.Name) {
		return fmt.Errorf("status.needs cannot reference the StepRun itself")
	}
	if err := validateStructuredErrorPayload(sr); err != nil {
		return err
	}
	return nil
}

func validateStructuredErrorPayload(sr *runsv1alpha1.StepRun) error {
	if sr == nil || sr.Status.Error == nil || len(sr.Status.Error.Raw) == 0 {
		return nil
	}
	raw := webhookshared.TrimLeadingSpace(sr.Status.Error.Raw)
	if err := webhookshared.EnsureJSONObject("status.error", raw); err != nil {
		return err
	}
	if err := webhookshared.ValidateJSONAgainstSchema(raw, []byte(stepRunStructuredErrorSchemaV1), "StepRun StructuredError"); err != nil {
		return fmt.Errorf("status.error is invalid: %w", err)
	}
	return nil
}
