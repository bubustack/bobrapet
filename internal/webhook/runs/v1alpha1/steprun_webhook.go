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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

// nolint:unused
// log is for logging in this package.
var steprunlog = logf.Log.WithName("steprun-resource")

// SetupStepRunWebhookWithManager registers the webhook for StepRun in the manager.
func SetupStepRunWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&runsv1alpha1.StepRun{}).
		WithValidator(&StepRunCustomValidator{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-runs-bubu-sh-v1alpha1-steprun,mutating=false,failurePolicy=fail,sideEffects=None,groups=runs.bubu.sh,resources=stepruns,verbs=create;update,versions=v1alpha1,name=vsteprun-v1alpha1.kb.io,admissionReviewVersions=v1

// StepRunCustomValidator struct is responsible for validating the StepRun resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type StepRunCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &StepRunCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type StepRun.
func (v *StepRunCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	steprun, ok := obj.(*runsv1alpha1.StepRun)
	if !ok {
		return nil, fmt.Errorf("expected a StepRun object but got %T", obj)
	}
	steprunlog.Info("Validation for StepRun upon creation", "name", steprun.GetName())

	return v.validateStepRun(steprun)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type StepRun.
func (v *StepRunCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	steprun, ok := newObj.(*runsv1alpha1.StepRun)
	if !ok {
		return nil, fmt.Errorf("expected a StepRun object for the newObj but got %T", newObj)
	}
	steprunlog.Info("Validation for StepRun upon update", "name", steprun.GetName())

	return v.validateStepRun(steprun)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type StepRun.
func (v *StepRunCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	steprun, ok := obj.(*runsv1alpha1.StepRun)
	if !ok {
		return nil, fmt.Errorf("expected a StepRun object but got %T", obj)
	}
	steprunlog.Info("Validation for StepRun upon deletion", "name", steprun.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}

func (v *StepRunCustomValidator) validateStepRun(sr *runsv1alpha1.StepRun) (admission.Warnings, error) {
	if sr.Spec.StoryRunRef == "" {
		return nil, fmt.Errorf("spec.storyRunRef is required")
	}
	if sr.Spec.StepID == "" {
		return nil, fmt.Errorf("spec.stepId is required")
	}
	if sr.Spec.Timeout != "" {
		// basic duration validation (ms|s|m|h)
		if !durationLike(sr.Spec.Timeout) {
			return nil, fmt.Errorf("spec.timeout must be a valid duration (e.g., 5m, 30s)")
		}
	}
	return nil, nil
}

func durationLike(s string) bool {
	// very small fast-path check without importing time.ParseDuration (keeps webhook lean):
	// suffix must be one of ms,s,m,h and at least one leading digit
	if len(s) < 2 {
		return false
	}
	last := s[len(s)-1]
	if last != 's' && last != 'm' && last != 'h' { // allow 's','m','h'; 'ms' handled below
		if len(s) >= 3 && s[len(s)-2:] == "ms" {
			last = 's' // treat as valid
		} else {
			return false
		}
	}
	// ensure there is at least one digit before the suffix
	for i := 0; i < len(s)-1; i++ {
		if s[i] >= '0' && s[i] <= '9' {
			return true
		}
	}
	return false
}
