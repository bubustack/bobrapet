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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	runsvalidation "github.com/bubustack/bobrapet/pkg/runs/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// log is for logging in this package.
var steprunlog = logf.Log.WithName("steprun-resource")

type StepRunWebhook struct {
	Client        client.Client
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

// SetupWebhookWithManager registers the StepRun defaulting and validating webhooks.
//
// Behavior:
//   - Stores the manager's client for downstream validation lookups.
//   - Creates both defaulter and validator webhooks for StepRun resources.
//   - Injects Config and ConfigManager into both handlers.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//
// Returns:
//   - nil on success.
//   - Error if webhook registration fails.
func (wh *StepRunWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	wh.Client = mgr.GetClient()

	return ctrl.NewWebhookManagedBy(mgr).
		For(&runsv1alpha1.StepRun{}).
		WithDefaulter(&StepRunCustomDefaulter{
			Config:        wh.Config,
			ConfigManager: wh.ConfigManager,
		}).
		WithValidator(&StepRunCustomValidator{
			Client:        wh.Client,
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
	Client        client.Client
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
	return webhookshared.ValidateCreateResource[*runsv1alpha1.StepRun](ctx, obj, "StepRun", steprunlog, func(_ context.Context, steprun *runsv1alpha1.StepRun) error {
		if err := v.validateStepRun(steprun); err != nil {
			return err
		}
		return validateStepRunStatus(steprun)
	})
}

// ValidateUpdate implements webhook.CustomValidator for StepRun updates.
//
// Behavior:
//   - Skips validation if resource is being deleted (DeletionTimestamp set).
//   - Rejects updates that decrease observedGeneration.
//   - Validates status.needs on every update.
//   - Skips spec validation if spec is unchanged (status-only update).
//
// Arguments:
//   - ctx context.Context: unused but required by interface.
//   - oldObj runtime.Object: previous StepRun state.
//   - newObj runtime.Object: proposed StepRun state.
//
// Returns:
//   - nil, nil if validation passes or is skipped.
//   - nil, error if type assertion fails or validation errors exist.
func (v *StepRunCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	steprun, ok := newObj.(*runsv1alpha1.StepRun)
	if !ok {
		return nil, fmt.Errorf("expected a StepRun object for the newObj but got %T", newObj)
	}
	steprunlog.Info("Validation for StepRun upon update", "name", steprun.GetName())

	// Allow metadata-only updates during deletion (e.g., finalizer removal)
	if steprun.DeletionTimestamp != nil {
		return nil, nil
	}

	if oldSr, ok := oldObj.(*runsv1alpha1.StepRun); ok {
		if err := ensureStepRunObservedGenerationMonotonic(oldSr, steprun); err != nil {
			return nil, err
		}
	}

	if err := validateStepRunStatus(steprun); err != nil {
		return nil, err
	}

	if oldSr, ok := oldObj.(*runsv1alpha1.StepRun); ok {
		// Skip further validation if only status changed
		if reflect.DeepEqual(oldSr.Spec, steprun.Spec) {
			return nil, nil
		}
	}

	if err := v.validateStepRun(steprun); err != nil {
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
func (v *StepRunCustomValidator) validateStepRun(sr *runsv1alpha1.StepRun) error {
	if err := requireBasicFields(sr); err != nil {
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

// validateInputs ensures spec.input is a JSON object within size limits.
//
// Behavior:
//   - Skips validation if input is empty.
//   - Ensures input is a JSON object (not array or primitive).
//   - Ensures input doesn't exceed maxBytes.
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
	return webhookshared.EnforceMaxBytes(
		"spec.input",
		sr.Spec.Input.Raw,
		maxBytes,
		"Provide large payloads via object storage (Story.policy.storage) and references instead of inlining",
	)
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
		return webhookshared.EnforceMaxBytes(
			"status.output",
			sr.Status.Output.Raw,
			maxBytes,
			"Large outputs must be offloaded by the SDK",
		)
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
//   - Allows updates when either value is zero (unset).
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
	for _, name := range sr.Status.Needs {
		if name == sr.Name {
			return fmt.Errorf("status.needs cannot reference the StepRun itself")
		}
	}
	return nil
}
