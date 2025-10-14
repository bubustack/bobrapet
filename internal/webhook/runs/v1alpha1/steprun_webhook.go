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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// nolint:unused
// log is for logging in this package.
var steprunlog = logf.Log.WithName("steprun-resource")

type StepRunWebhook struct {
	Client client.Client
	Config *config.ControllerConfig
}

func (wh *StepRunWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	wh.Client = mgr.GetClient()
	operatorConfigManager := config.NewOperatorConfigManager(mgr.GetClient(), "bobrapet-system", "bobrapet-operator-config")
	wh.Config = operatorConfigManager.GetControllerConfig()

	return ctrl.NewWebhookManagedBy(mgr).
		For(&runsv1alpha1.StepRun{}).
		WithValidator(&StepRunCustomValidator{
			Client: wh.Client,
			Config: wh.Config,
		}).
		Complete()
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
	Client client.Client
	Config *config.ControllerConfig
}

var _ webhook.CustomValidator = &StepRunCustomValidator{}

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
	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type StepRun.
func (v *StepRunCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	steprun, ok := newObj.(*runsv1alpha1.StepRun)
	if !ok {
		return nil, fmt.Errorf("expected a StepRun object for the newObj but got %T", newObj)
	}
	steprunlog.Info("Validation for StepRun upon update", "name", steprun.GetName())

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
	if sr.Spec.StoryRunRef.Name == "" {
		return fmt.Errorf("spec.storyRunRef.name is required")
	}
	if sr.Spec.StepID == "" {
		return fmt.Errorf("spec.stepId is required")
	}

	maxBytes := v.Config.Engram.EngramControllerConfig.DefaultMaxInlineSize
	if maxBytes == 0 {
		maxBytes = 1024 // Fallback to a safe default if config is not loaded
	}

	if sr.Spec.Input != nil && len(sr.Spec.Input.Raw) > 0 {
		b := sr.Spec.Input.Raw
		for len(b) > 0 && (b[0] == ' ' || b[0] == '\n' || b[0] == '\t' || b[0] == '\r') {
			b = b[1:]
		}
		if len(b) > 0 && b[0] != '{' {
			return fmt.Errorf("spec.input must be a JSON object")
		}
		// Enforce an upper bound for inline input size to align with Engram/Impulse validation.
		// Large payloads must be handled via SDK offload and storage policies.
		if len(sr.Spec.Input.Raw) > maxBytes {
			return fmt.Errorf("spec.input too large (%d bytes > %d). Provide large payloads via object storage (Story.policy.storage) and references instead of inlining", len(sr.Spec.Input.Raw), maxBytes)
		}
	}

	// Validate status.output size on updates. This is the critical check.
	if sr.Status.Output != nil && len(sr.Status.Output.Raw) > maxBytes {
		return fmt.Errorf("status.output is too large (%d bytes > %d). Large outputs must be offloaded by the SDK", len(sr.Status.Output.Raw), maxBytes)
	}

	// Add total object size validation on update to prevent etcd errors
	rawSR, err := json.Marshal(sr)
	if err != nil {
		// This should not happen on a valid object
		return fmt.Errorf("internal error: failed to marshal StepRun for size validation: %w", err)
	}

	// Use a safe, hardcoded limit slightly below the typical 1.5MiB etcd limit.
	const maxTotalStepRunSizeBytes = 1 * 1024 * 1024 // 1 MiB
	if len(rawSR) > maxTotalStepRunSizeBytes {
		return fmt.Errorf("StepRun total size of %d bytes exceeds maximum allowed size of %d bytes", len(rawSR), maxTotalStepRunSizeBytes)
	}

	return nil
}
