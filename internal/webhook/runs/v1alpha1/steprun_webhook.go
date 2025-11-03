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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// nolint:unused
// log is for logging in this package.
var steprunlog = logf.Log.WithName("steprun-resource")

type StepRunWebhook struct {
	Client        client.Client
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

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

func (d *StepRunCustomDefaulter) controllerConfig() *config.ControllerConfig {
	if d.ConfigManager != nil {
		if cfg := d.ConfigManager.GetControllerConfig(); cfg != nil {
			return cfg
		}
	}
	if d.Config != nil {
		return d.Config
	}
	return config.DefaultControllerConfig()
}

func (d *StepRunCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	steprun, ok := obj.(*runsv1alpha1.StepRun)
	if !ok {
		return fmt.Errorf("expected a StepRun object but got %T", obj)
	}
	steprunlog.Info("Defaulting StepRun", "name", steprun.GetName())

	cfg := d.controllerConfig()
	steprun.Spec.Retry = webhookshared.ResolveRetryPolicy(cfg, steprun.Spec.Retry)

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
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

func (v *StepRunCustomValidator) controllerConfig() *config.ControllerConfig {
	if v.ConfigManager != nil {
		if cfg := v.ConfigManager.GetControllerConfig(); cfg != nil {
			return cfg
		}
	}
	if v.Config != nil {
		return v.Config
	}
	return config.DefaultControllerConfig()
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type StepRun.
func (v *StepRunCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	steprun, ok := obj.(*runsv1alpha1.StepRun)
	if !ok {
		return nil, fmt.Errorf("expected a StepRun object but got %T", obj)
	}
	steprunlog.Info("Validation for StepRun upon creation", "name", steprun.GetName())

	if err := v.validateStepRun(steprun); err != nil {
		return nil, err
	}
	if err := validateStepRunStatus(steprun); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type StepRun.
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

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type StepRun.
func (v *StepRunCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	steprun, ok := obj.(*runsv1alpha1.StepRun)
	if !ok {
		return nil, fmt.Errorf("expected a StepRun object but got %T", obj)
	}
	steprunlog.Info("Validation for StepRun upon deletion", "name", steprun.GetName())

	return nil, nil
}

// validateStepRun performs basic invariants for StepRun specs.
// - spec.storyRunRef.name required
// - spec.stepId required
// - If Input present, must be a JSON object
func (v *StepRunCustomValidator) validateStepRun(sr *runsv1alpha1.StepRun) error {
	if err := requireBasicFields(sr); err != nil {
		return err
	}
	cfg := v.controllerConfig()
	maxBytes := pickMaxInlineBytes(cfg)
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

func requireBasicFields(sr *runsv1alpha1.StepRun) error {
	if sr.Spec.StoryRunRef.Name == "" {
		return fmt.Errorf("spec.storyRunRef.name is required")
	}
	if sr.Spec.StepID == "" {
		return fmt.Errorf("spec.stepId is required")
	}
	return nil
}

func pickMaxInlineBytes(cfg *config.ControllerConfig) int {
	if cfg == nil {
		cfg = config.DefaultControllerConfig()
	}
	maxBytes := cfg.Engram.EngramControllerConfig.DefaultMaxInlineSize
	if maxBytes == 0 {
		maxBytes = 1024
	}
	return maxBytes
}

func validateInputs(sr *runsv1alpha1.StepRun, maxBytes int) error {
	if sr.Spec.Input == nil || len(sr.Spec.Input.Raw) == 0 {
		return nil
	}
	b := sr.Spec.Input.Raw
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\n' || b[0] == '\t' || b[0] == '\r') {
		b = b[1:]
	}
	if len(b) > 0 && b[0] != '{' {
		return fmt.Errorf("spec.input must be a JSON object")
	}
	if len(sr.Spec.Input.Raw) > maxBytes {
		return fmt.Errorf("spec.input too large (%d bytes > %d). Provide large payloads via object storage (Story.policy.storage) and references instead of inlining", len(sr.Spec.Input.Raw), maxBytes)
	}
	return nil
}

func validateStatusOutput(sr *runsv1alpha1.StepRun, maxBytes int) error {
	if sr.Status.Output != nil && len(sr.Status.Output.Raw) > maxBytes {
		return fmt.Errorf("status.output is too large (%d bytes > %d). Large outputs must be offloaded by the SDK", len(sr.Status.Output.Raw), maxBytes)
	}
	return nil
}

func validateDownstreamTargets(targets []runsv1alpha1.DownstreamTarget) error {
	for idx, tgt := range targets {
		count := 0
		if tgt.GRPCTarget != nil {
			count++
		}
		if tgt.Terminate != nil {
			count++
		}
		if count != 1 {
			return fmt.Errorf("spec.downstreamTargets[%d] must set exactly one of grpc or terminate", idx)
		}
	}
	return nil
}

func ensureStepRunObservedGenerationMonotonic(oldSR, newSR *runsv1alpha1.StepRun) error {
	oldGen := oldSR.Status.ObservedGeneration
	newGen := newSR.Status.ObservedGeneration
	if oldGen > 0 && newGen > 0 && newGen < oldGen {
		return fmt.Errorf("status.observedGeneration must be monotonically increasing (old=%d new=%d)", oldGen, newGen)
	}
	return nil
}

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

func validateStepRunStatus(sr *runsv1alpha1.StepRun) error {
	for _, name := range sr.Status.Needs {
		if name == sr.Name {
			return fmt.Errorf("status.needs cannot reference the StepRun itself")
		}
	}
	return nil
}
