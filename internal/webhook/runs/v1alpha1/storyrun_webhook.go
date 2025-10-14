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

	"sigs.k8s.io/controller-runtime/pkg/client"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/xeipuuv/gojsonschema"
	"k8s.io/apimachinery/pkg/api/errors"
)

// nolint:unused
// log is for logging in this package.
var storyrunlog = logf.Log.WithName("storyrun-resource")

type StoryRunWebhook struct {
	Client client.Client
	Config *config.ControllerConfig
}

func (wh *StoryRunWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	wh.Client = mgr.GetClient()

	return ctrl.NewWebhookManagedBy(mgr).
		For(&runsv1alpha1.StoryRun{}).
		WithValidator(&StoryRunCustomValidator{
			Client: mgr.GetClient(),
			Config: wh.Config,
		}).
		Complete()
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-runs-bubustack-io-v1alpha1-storyrun,mutating=false,failurePolicy=fail,sideEffects=None,groups=runs.bubustack.io,resources=storyruns,verbs=create;update,versions=v1alpha1,name=vstoryrun-v1alpha1.kb.io,admissionReviewVersions=v1

// StoryRunCustomValidator struct is responsible for validating the StoryRun resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type StoryRunCustomValidator struct {
	Client client.Client
	Config *config.ControllerConfig
}

var _ webhook.CustomValidator = &StoryRunCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type StoryRun.
func (v *StoryRunCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	storyrun, ok := obj.(*runsv1alpha1.StoryRun)
	if !ok {
		return nil, fmt.Errorf("expected a StoryRun object but got %T", obj)
	}
	storyrunlog.Info("Validation for StoryRun upon creation", "name", storyrun.GetName())

	if err := v.validateStoryRun(ctx, storyrun); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type StoryRun.
func (v *StoryRunCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	storyrun, ok := newObj.(*runsv1alpha1.StoryRun)
	if !ok {
		return nil, fmt.Errorf("expected a StoryRun object for the newObj but got %T", newObj)
	}
	storyrunlog.Info("Validation for StoryRun upon update", "name", storyrun.GetName())

	if err := v.validateStoryRun(ctx, storyrun); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type StoryRun.
func (v *StoryRunCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	storyrun, ok := obj.(*runsv1alpha1.StoryRun)
	if !ok {
		return nil, fmt.Errorf("expected a StoryRun object but got %T", obj)
	}
	storyrunlog.Info("Validation for StoryRun upon deletion", "name", storyrun.GetName())

	return nil, nil
}

// validateStoryRun performs basic invariants for StoryRun specs.
// - spec.storyRef.name required
// - inputs, if present, must be JSON object
func (v *StoryRunCustomValidator) validateStoryRun(ctx context.Context, sr *runsv1alpha1.StoryRun) error {
	if sr.Spec.StoryRef.Name == "" {
		return fmt.Errorf("spec.storyRef.name is required")
	}

	// Fetch the referenced Story to validate against its schema
	story := &bubuv1alpha1.Story{}
	storyKey := sr.Spec.StoryRef.ToNamespacedName(sr)
	if err := v.Client.Get(ctx, storyKey, story); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("referenced story '%s' not found", storyKey.String())
		}
		return fmt.Errorf("failed to get referenced story '%s': %w", storyKey.String(), err)
	}

	if sr.Spec.Inputs != nil && len(sr.Spec.Inputs.Raw) > 0 {
		b := sr.Spec.Inputs.Raw
		for len(b) > 0 && (b[0] == ' ' || b[0] == '\n' || b[0] == '\t' || b[0] == '\r') {
			b = b[1:]
		}
		if len(b) > 0 && b[0] != '{' {
			return fmt.Errorf("spec.inputs must be a JSON object")
		}

		// Enforce an upper bound for inline input size to avoid oversized API server payloads.
		// Use EngramControllerConfig.DefaultMaxInlineSize for a single cap.
		maxBytes := v.Config.Engram.EngramControllerConfig.DefaultMaxInlineSize
		if maxBytes == 0 {
			maxBytes = 1024 // Fallback
		}
		if len(sr.Spec.Inputs.Raw) > maxBytes {
			return fmt.Errorf("spec.inputs is too large (%d bytes > %d). Provide large inputs via an offloading mechanism instead of inlining", len(sr.Spec.Inputs.Raw), maxBytes)
		}
	}

	// Validate inputs against the Story's input schema
	if story.Spec.InputsSchema != nil && len(story.Spec.InputsSchema.Raw) > 0 {
		schemaLoader := gojsonschema.NewStringLoader(string(story.Spec.InputsSchema.Raw))
		var documentLoader gojsonschema.JSONLoader
		if sr.Spec.Inputs != nil && len(sr.Spec.Inputs.Raw) > 0 {
			documentLoader = gojsonschema.NewStringLoader(string(sr.Spec.Inputs.Raw))
		} else {
			// If inputs are nil or empty, validate against an empty JSON object.
			// This correctly handles cases where the schema requires certain fields.
			documentLoader = gojsonschema.NewStringLoader("{}")
		}

		result, err := gojsonschema.Validate(schemaLoader, documentLoader)
		if err != nil {
			return fmt.Errorf("error validating spec.inputs against Story schema: %w", err)
		}
		if !result.Valid() {
			var errs []string
			for _, desc := range result.Errors() {
				errs = append(errs, desc.String())
			}
			return fmt.Errorf("spec.inputs is invalid against Story schema: %v", errs)
		}
	}

	return nil
}
