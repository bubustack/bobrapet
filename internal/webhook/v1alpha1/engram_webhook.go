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

	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
)

// nolint:unused
// log is for logging in this package.
var engramlog = logf.Log.WithName("engram-resource")

// SetupEngramWebhookWithManager registers the webhook for Engram in the manager.
func SetupEngramWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&bubushv1alpha1.Engram{}).
		WithValidator(&EngramCustomValidator{}).
		WithDefaulter(&EngramCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-bubu-sh-v1alpha1-engram,mutating=true,failurePolicy=fail,sideEffects=None,groups=bubu.sh,resources=engrams,verbs=create;update,versions=v1alpha1,name=mengram-v1alpha1.kb.io,admissionReviewVersions=v1

// EngramCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Engram when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type EngramCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

var _ webhook.CustomDefaulter = &EngramCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Engram.
func (d *EngramCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	engram, ok := obj.(*bubushv1alpha1.Engram)

	if !ok {
		return fmt.Errorf("expected an Engram object but got %T", obj)
	}
	engramlog.Info("Defaulting for Engram", "name", engram.GetName())

	// TODO(user): fill in your defaulting logic.

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-bubu-sh-v1alpha1-engram,mutating=false,failurePolicy=fail,sideEffects=None,groups=bubu.sh,resources=engrams,verbs=create;update,versions=v1alpha1,name=vengram-v1alpha1.kb.io,admissionReviewVersions=v1

// EngramCustomValidator struct is responsible for validating the Engram resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type EngramCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &EngramCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Engram.
func (v *EngramCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	engram, ok := obj.(*bubushv1alpha1.Engram)
	if !ok {
		return nil, fmt.Errorf("expected a Engram object but got %T", obj)
	}
	engramlog.Info("Validation for Engram upon creation", "name", engram.GetName())

	// TODO(user): fill in your validation logic upon object creation.

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Engram.
func (v *EngramCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	engram, ok := newObj.(*bubushv1alpha1.Engram)
	if !ok {
		return nil, fmt.Errorf("expected a Engram object for the newObj but got %T", newObj)
	}
	engramlog.Info("Validation for Engram upon update", "name", engram.GetName())

	// TODO(user): fill in your validation logic upon object update.

	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Engram.
func (v *EngramCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	engram, ok := obj.(*bubushv1alpha1.Engram)
	if !ok {
		return nil, fmt.Errorf("expected a Engram object but got %T", obj)
	}
	engramlog.Info("Validation for Engram upon deletion", "name", engram.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}
