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

	"github.com/xeipuuv/gojsonschema"
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
var impulselog = logf.Log.WithName("impulse-resource")

type ImpulseWebhook struct {
	client.Client
	Config *config.ControllerConfig
}

// SetupWebhookWithManager registers the webhook for Impulse in the manager.
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
	// TODO(user): Add more fields as needed for defaulting
}

var _ webhook.CustomDefaulter = &ImpulseCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Impulse.
func (d *ImpulseCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	impulse, ok := obj.(*bubushv1alpha1.Impulse)

	if !ok {
		return fmt.Errorf("expected an Impulse object but got %T", obj)
	}
	impulselog.Info("Defaulting for Impulse", "name", impulse.GetName())

	return nil
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

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Impulse.
func (v *ImpulseCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	impulse, ok := obj.(*bubushv1alpha1.Impulse)
	if !ok {
		return nil, fmt.Errorf("expected a Impulse object but got %T", obj)
	}
	impulselog.Info("Validation for Impulse upon creation", "name", impulse.GetName())

	if err := v.validateImpulse(ctx, impulse); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Impulse.
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

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Impulse.
func (v *ImpulseCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	impulse, ok := obj.(*bubushv1alpha1.Impulse)
	if !ok {
		return nil, fmt.Errorf("expected a Impulse object but got %T", obj)
	}
	impulselog.Info("Validation for Impulse upon deletion", "name", impulse.GetName())

	return nil, nil
}

// validateImpulse performs basic invariants validation for Impulse specs.
// - templateRef.name and storyRef.name must be set
// - with and mapping, if present, must be JSON objects (not array/primitive)
// - workload.mode must not be 'job' (impulse must be always-on)
func (v *ImpulseCustomValidator) validateImpulse(ctx context.Context, impulse *bubushv1alpha1.Impulse) error {
	if err := v.validateRequiredFields(impulse); err != nil {
		return err
	}
	template, err := v.fetchTemplate(ctx, impulse.Spec.TemplateRef.Name)
	if err != nil {
		return err
	}
	if err := v.validateWithBlock(impulse, template); err != nil {
		return err
	}
	if err := v.validateMappingBlock(impulse, template); err != nil {
		return err
	}
	if err := v.validateWorkloadMode(impulse); err != nil {
		return err
	}
	return nil
}

func (v *ImpulseCustomValidator) validateRequiredFields(impulse *bubushv1alpha1.Impulse) error {
	if impulse.Spec.TemplateRef.Name == "" {
		return fmt.Errorf("spec.templateRef.name is required")
	}
	if impulse.Spec.StoryRef.Name == "" {
		return fmt.Errorf("spec.storyRef.name is required")
	}
	return nil
}

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

func (v *ImpulseCustomValidator) validateWithBlock(impulse *bubushv1alpha1.Impulse, template *v1alpha1.ImpulseTemplate) error {
	if impulse.Spec.With == nil || len(impulse.Spec.With.Raw) == 0 {
		return nil
	}
	b := impulse.Spec.With.Raw
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\n' || b[0] == '\t' || b[0] == '\r') {
		b = b[1:]
	}
	if len(b) > 0 && b[0] != '{' {
		return fmt.Errorf("spec.with must be a JSON object")
	}
	maxBytes := v.Config.Engram.EngramControllerConfig.DefaultMaxInlineSize
	if maxBytes == 0 {
		maxBytes = 1024
	}
	if len(impulse.Spec.With.Raw) > maxBytes {
		return fmt.Errorf("spec.with too large (%d bytes). Provide large payloads via object storage and references instead of inlining", len(impulse.Spec.With.Raw))
	}
	if template.Spec.ConfigSchema != nil && len(template.Spec.ConfigSchema.Raw) > 0 {
		schemaLoader := gojsonschema.NewStringLoader(string(template.Spec.ConfigSchema.Raw))
		documentLoader := gojsonschema.NewStringLoader(string(impulse.Spec.With.Raw))
		result, err := gojsonschema.Validate(schemaLoader, documentLoader)
		if err != nil {
			return fmt.Errorf("error validating spec.with against ImpulseTemplate schema: %w", err)
		}
		if !result.Valid() {
			var errs []string
			for _, desc := range result.Errors() {
				errs = append(errs, desc.String())
			}
			return fmt.Errorf("spec.with is invalid against ImpulseTemplate schema: %v", errs)
		}
	}
	return nil
}

func (v *ImpulseCustomValidator) validateMappingBlock(impulse *bubushv1alpha1.Impulse, template *v1alpha1.ImpulseTemplate) error {
	if impulse.Spec.Mapping == nil || len(impulse.Spec.Mapping.Raw) == 0 {
		return nil
	}
	b := impulse.Spec.Mapping.Raw
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\n' || b[0] == '\t' || b[0] == '\r') {
		b = b[1:]
	}
	if len(b) > 0 && b[0] != '{' {
		return fmt.Errorf("spec.mapping must be a JSON object")
	}
	maxBytes := v.Config.Engram.EngramControllerConfig.DefaultMaxInlineSize
	if maxBytes == 0 {
		maxBytes = 1024
	}
	if len(impulse.Spec.Mapping.Raw) > maxBytes {
		return fmt.Errorf("spec.mapping too large (%d bytes). Provide large payloads via object storage and references instead of inlining", len(impulse.Spec.Mapping.Raw))
	}
	if template.Spec.ContextSchema != nil && len(template.Spec.ContextSchema.Raw) > 0 {
		schemaLoader := gojsonschema.NewStringLoader(string(template.Spec.ContextSchema.Raw))
		documentLoader := gojsonschema.NewStringLoader(string(impulse.Spec.Mapping.Raw))
		result, err := gojsonschema.Validate(schemaLoader, documentLoader)
		if err != nil {
			return fmt.Errorf("error validating spec.mapping against ImpulseTemplate schema: %w", err)
		}
		if !result.Valid() {
			var errs []string
			for _, desc := range result.Errors() {
				errs = append(errs, desc.String())
			}
			return fmt.Errorf("spec.mapping is invalid against ImpulseTemplate schema: %v", errs)
		}
	}
	return nil
}

func (v *ImpulseCustomValidator) validateWorkloadMode(impulse *bubushv1alpha1.Impulse) error {
	if impulse.Spec.Workload != nil && impulse.Spec.Workload.Mode == "job" {
		return fmt.Errorf("spec.workload.mode must not be 'job' for Impulse (must be always-on)")
	}
	return nil
}
