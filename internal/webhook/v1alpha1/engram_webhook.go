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
)

// nolint:unused
// log is for logging in this package.
var engramlog = logf.Log.WithName("engram-resource")

type EngramWebhook struct {
	client.Client
	Config *config.ControllerConfig
}

// SetupWebhookWithManager registers the webhook for Engram in the manager.
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

	return nil
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

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Engram.
func (v *EngramCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	engram, ok := obj.(*bubushv1alpha1.Engram)
	if !ok {
		return nil, fmt.Errorf("expected a Engram object but got %T", obj)
	}
	engramlog.Info("Validation for Engram upon creation", "name", engram.GetName())

	if err := v.validateEngram(ctx, engram); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Engram.
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

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Engram.
func (v *EngramCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	engram, ok := obj.(*bubushv1alpha1.Engram)
	if !ok {
		return nil, fmt.Errorf("expected a Engram object but got %T", obj)
	}
	engramlog.Info("Validation for Engram upon deletion", "name", engram.GetName())

	return nil, nil
}

// validateEngram performs basic invariants validation for Engram specs.
// - templateRef.name must be set (enforced by CRD; double-check presence)
// - if With is present, it must be a JSON object (not array/primitive)
func (v *EngramCustomValidator) validateEngram(ctx context.Context, engram *bubushv1alpha1.Engram) error {
	if err := requireTemplateRef(engram); err != nil {
		return err
	}
	template, err := fetchEngramTemplate(ctx, v.Client, engram.Spec.TemplateRef.Name)
	if err != nil {
		return err
	}
	if err := validateWithBlock(engram, v.Config, template); err != nil {
		return err
	}
	return nil
}

func requireTemplateRef(engram *bubushv1alpha1.Engram) error {
	if engram.Spec.TemplateRef.Name == "" {
		return fmt.Errorf("spec.templateRef.name is required")
	}
	return nil
}

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

func validateWithBlock(engram *bubushv1alpha1.Engram, cfg *config.ControllerConfig, template *v1alpha1.EngramTemplate) error {
	if engram.Spec.With == nil || len(engram.Spec.With.Raw) == 0 {
		return nil
	}
	b := trimLeadingSpace(engram.Spec.With.Raw)
	if err := ensureJSONObject("spec.with", b); err != nil {
		return err
	}
	maxBytes := pickMaxInline(cfg)
	if err := enforceMaxBytes("spec.with", engram.Spec.With.Raw, maxBytes); err != nil {
		return err
	}
	if template.Spec.ConfigSchema != nil && len(template.Spec.ConfigSchema.Raw) > 0 {
		if err := validateJSONAgainstSchema(engram.Spec.With.Raw, template.Spec.ConfigSchema.Raw, "EngramTemplate"); err != nil {
			return err
		}
	}
	return nil
}
