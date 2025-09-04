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
var storylog = logf.Log.WithName("story-resource")

// SetupStoryWebhookWithManager registers the webhook for Story in the manager.
func SetupStoryWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&bubushv1alpha1.Story{}).
		WithValidator(&StoryCustomValidator{}).
		WithDefaulter(&StoryCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-bubu-sh-v1alpha1-story,mutating=true,failurePolicy=fail,sideEffects=None,groups=bubu.sh,resources=stories,verbs=create;update,versions=v1alpha1,name=mstory-v1alpha1.kb.io,admissionReviewVersions=v1

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

	// No object-wide defaults. Field-level defaults are set via CRD markers.
	// Keep webhook defaulting minimal to avoid drift with CRD defaults.

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-bubu-sh-v1alpha1-story,mutating=false,failurePolicy=fail,sideEffects=None,groups=bubu.sh,resources=stories,verbs=create;update,versions=v1alpha1,name=vstory-v1alpha1.kb.io,admissionReviewVersions=v1

// StoryCustomValidator struct is responsible for validating the Story resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type StoryCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &StoryCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Story.
func (v *StoryCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	story, ok := obj.(*bubushv1alpha1.Story)
	if !ok {
		return nil, fmt.Errorf("expected a Story object but got %T", obj)
	}
	storylog.Info("Validation for Story upon creation", "name", story.GetName())

	return v.validateStory(story)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Story.
func (v *StoryCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	story, ok := newObj.(*bubushv1alpha1.Story)
	if !ok {
		return nil, fmt.Errorf("expected a Story object for the newObj but got %T", newObj)
	}
	storylog.Info("Validation for Story upon update", "name", story.GetName())

	return v.validateStory(story)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Story.
func (v *StoryCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	story, ok := obj.(*bubushv1alpha1.Story)
	if !ok {
		return nil, fmt.Errorf("expected a Story object but got %T", obj)
	}
	storylog.Info("Validation for Story upon deletion", "name", story.GetName())

	// No delete-time validation
	return nil, nil
}

// validateStory contains creation/update validation logic
func (v *StoryCustomValidator) validateStory(story *bubushv1alpha1.Story) (admission.Warnings, error) {
	if len(story.Spec.Steps) == 0 {
		return nil, fmt.Errorf("spec.steps must not be empty")
	}
	for _, s := range story.Spec.Steps {
		if s.Name == "" {
			return nil, fmt.Errorf("each step must have a non-empty name")
		}
		hasType := s.Type != nil && *s.Type != ""
		hasRef := s.Ref != nil && *s.Ref != ""
		if hasType == hasRef {
			return nil, fmt.Errorf("step %q must set exactly one of type or ref", s.Name)
		}
	}
	return nil, nil
}
