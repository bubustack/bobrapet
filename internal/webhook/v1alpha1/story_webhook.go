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
	"time"

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
	"github.com/bubustack/bobrapet/pkg/enums"
)

// nolint:unused
// log is for logging in this package.
var storylog = logf.Log.WithName("story-resource")

// StoryWebhook sets up the webhook for Story in the manager.
type StoryWebhook struct {
	client.Client
	Config *config.ControllerConfig
}

// SetupWebhookWithManager registers the webhook for Story in the manager.
func (wh *StoryWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	wh.Client = mgr.GetClient()

	operatorConfigManager := config.NewOperatorConfigManager(mgr.GetClient(), "bobrapet-system", "bobrapet-operator-config")
	wh.Config = operatorConfigManager.GetControllerConfig()
	return ctrl.NewWebhookManagedBy(mgr).For(&bubushv1alpha1.Story{}).
		WithValidator(&StoryCustomValidator{
			Client: wh.Client,
			Config: wh.Config,
		}).
		WithDefaulter(&StoryCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-bubustack-io-v1alpha1-story,mutating=true,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=stories,verbs=create;update,versions=v1alpha1,name=mstory-v1alpha1.kb.io,admissionReviewVersions=v1

// StoryCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Story when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type StoryCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

var _ webhook.CustomDefaulter = &StoryCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Story.
func (d *StoryCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	story, ok := obj.(*bubushv1alpha1.Story)

	if !ok {
		return fmt.Errorf("expected an Story object but got %T", obj)
	}
	storylog.Info("Defaulting for Story", "name", story.GetName())

	return nil
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-bubustack-io-v1alpha1-story,mutating=false,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=stories,verbs=create;update,versions=v1alpha1,name=vstory-v1alpha1.kb.io,admissionReviewVersions=v1

// StoryCustomValidator struct is responsible for validating the Story resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type StoryCustomValidator struct {
	Client client.Client
	Config *config.ControllerConfig
}

var _ webhook.CustomValidator = &StoryCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Story.
func (v *StoryCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	story, ok := obj.(*bubushv1alpha1.Story)
	if !ok {
		return nil, fmt.Errorf("expected a Story object but got %T", obj)
	}
	storylog.Info("Validation for Story upon creation", "name", story.GetName())

	webhookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := v.validateStory(webhookCtx, story); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Story.
func (v *StoryCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	story, ok := newObj.(*bubushv1alpha1.Story)
	if !ok {
		return nil, fmt.Errorf("expected a Story object for the newObj but got %T", newObj)
	}
	storylog.Info("Validation for Story upon update", "name", story.GetName())

	webhookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := v.validateStory(webhookCtx, story); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Story.
func (v *StoryCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	story, ok := obj.(*bubushv1alpha1.Story)
	if !ok {
		return nil, fmt.Errorf("expected a Story object but got %T", obj)
	}
	storylog.Info("Validation for Story upon deletion", "name", story.GetName())

	return nil, nil
}

// validateStory performs basic invariants validation for Story specs.
// - Each step name must be unique and DNS-1123 compatible (enforced by CRD, but we double-check uniqueness)
// - If step.Ref is set, step.Type must be empty; if step.Type is set, Ref must be nil
// - All items in step.Needs must reference existing step names (no self-dependency)
// - For streaming pattern PerStory, referenced Engrams must be long-running modes is enforced at reconcile; here we only validate shape
func (v *StoryCustomValidator) validateStory(ctx context.Context, story *bubushv1alpha1.Story) error {
	// Add a total size check to prevent etcd overload.
	rawStory, err := json.Marshal(story)
	if err != nil {
		// This should not happen with valid Kubernetes objects.
		return fmt.Errorf("internal error: failed to marshal story for size validation: %w", err)
	}
	// Use a safe, hardcoded limit slightly below the typical 1.5MiB etcd limit.
	const maxTotalStorySizeBytes = 1 * 1024 * 1024 // 1 MiB
	if len(rawStory) > maxTotalStorySizeBytes {
		return fmt.Errorf("story size of %d bytes exceeds maximum allowed size of %d bytes", len(rawStory), maxTotalStorySizeBytes)
	}

	seen := make(map[string]struct{})
	maxSize := v.Config.MaxStoryWithBlockSizeBytes
	if maxSize <= 0 {
		maxSize = config.DefaultControllerConfig().MaxStoryWithBlockSizeBytes
	}

	if story.Spec.Output != nil && len(story.Spec.Output.Raw) > maxSize {
		return fmt.Errorf("story 'output' block size of %d bytes exceeds maximum allowed size of %d bytes", len(story.Spec.Output.Raw), maxSize)
	}

	for i := range story.Spec.Steps {
		s := &story.Spec.Steps[i]
		if _, exists := seen[s.Name]; exists {
			return fmt.Errorf("duplicate step name '%s'", s.Name)
		}
		seen[s.Name] = struct{}{}
		if s.Ref != nil && s.Type != "" {
			return fmt.Errorf("step '%s' must not set both ref and type", s.Name)
		}
		if s.Ref == nil && s.Type == "" {
			return fmt.Errorf("step '%s' must set either ref or type", s.Name)
		}

		for _, dep := range s.Needs {
			if dep == s.Name {
				return fmt.Errorf("step '%s' cannot depend on itself in needs", s.Name)
			}
			// second pass below validates that all dependencies exist; no-op here to avoid SA4006
		}

		if s.Ref != nil {
			if err := v.validateEngramStep(ctx, story.Namespace, s); err != nil {
				return err
			}
		}

		if s.With != nil && len(s.With.Raw) > maxSize {
			return fmt.Errorf("step '%s': 'with' block size of %d bytes exceeds maximum allowed size of %d bytes", s.Name, len(s.With.Raw), maxSize)
		}

		// Validate the 'with' block for known primitive types that require a specific schema.
		if s.Type == enums.StepTypeExecuteStory {
			if s.With == nil {
				return fmt.Errorf("step '%s' of type 'executeStory' requires a 'with' block", s.Name)
			}
			var withConfig struct {
				StoryRef struct {
					Name string `json:"name"`
				} `json:"storyRef"`
			}
			if err := json.Unmarshal(s.With.Raw, &withConfig); err != nil {
				return fmt.Errorf("step '%s' has an invalid 'with' block for type 'executeStory': %w", s.Name, err)
			}
			if withConfig.StoryRef.Name == "" {
				return fmt.Errorf("step '%s' of type 'executeStory' requires 'with.storyRef.name' to be set", s.Name)
			}
		}
	}

	// Second pass: ensure all needs exist
	stepNames := make(map[string]struct{}, len(story.Spec.Steps))
	for i := range story.Spec.Steps {
		stepNames[story.Spec.Steps[i].Name] = struct{}{}
	}
	for i := range story.Spec.Steps {
		s := &story.Spec.Steps[i]
		for _, dep := range s.Needs {
			if _, ok := stepNames[dep]; !ok {
				return fmt.Errorf("step '%s' lists unknown dependency '%s' in needs", s.Name, dep)
			}
		}
	}
	return nil
}

func (v *StoryCustomValidator) validateEngramStep(ctx context.Context, namespace string, step *bubushv1alpha1.Step) error {
	// Fetch the Engram
	var engram bubushv1alpha1.Engram
	engramKey := types.NamespacedName{Name: step.Ref.Name, Namespace: namespace}
	if step.Ref.Namespace != nil && *step.Ref.Namespace != "" {
		engramKey.Namespace = *step.Ref.Namespace
	}
	if err := v.Client.Get(ctx, engramKey, &engram); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("step '%s' references engram '%s' which does not exist in namespace '%s'", step.Name, engramKey.Name, engramKey.Namespace)
		}
		return fmt.Errorf("failed to get engram for step '%s': %w", step.Name, err)
	}

	// Fetch the EngramTemplate
	var template v1alpha1.EngramTemplate
	if err := v.Client.Get(ctx, types.NamespacedName{Name: engram.Spec.TemplateRef.Name, Namespace: ""}, &template); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("step '%s' references engram '%s' which in turn references EngramTemplate '%s' that was not found", step.Name, engram.Name, engram.Spec.TemplateRef.Name)
		}
		return fmt.Errorf("failed to get EngramTemplate for step '%s': %w", step.Name, err)
	}

	// Validate the step's 'with' block against the template's inputSchema.
	if step.With != nil && len(step.With.Raw) > 0 && template.Spec.InputSchema != nil && len(template.Spec.InputSchema.Raw) > 0 {
		schemaLoader := gojsonschema.NewStringLoader(string(template.Spec.InputSchema.Raw))
		documentLoader := gojsonschema.NewStringLoader(string(step.With.Raw))
		result, err := gojsonschema.Validate(schemaLoader, documentLoader)
		if err != nil {
			return fmt.Errorf("step '%s': error validating 'with' block against EngramTemplate schema: %w", step.Name, err)
		}
		if !result.Valid() {
			var errs []string
			for _, desc := range result.Errors() {
				errs = append(errs, desc.String())
			}
			return fmt.Errorf("step '%s': 'with' block is invalid against EngramTemplate schema: %v", step.Name, errs)
		}
	}

	return nil
}
