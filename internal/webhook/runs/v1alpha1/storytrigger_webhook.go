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
	"reflect"
	"strings"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

// log is for logging in this package.
var storytriggerlog = logf.Log.WithName("storytrigger-resource")

// StoryTriggerWebhook wires the StoryTrigger validator into the manager.
type StoryTriggerWebhook struct {
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

// SetupStoryTriggerWebhookWithManager registers the webhook for StoryTrigger in the manager.
func SetupStoryTriggerWebhookWithManager(mgr ctrl.Manager) error {
	return (&StoryTriggerWebhook{
		Config: config.DefaultControllerConfig(),
	}).SetupWebhookWithManager(mgr)
}

// SetupWebhookWithManager registers the StoryTrigger validating webhook with the manager.
func (wh *StoryTriggerWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &runsv1alpha1.StoryTrigger{}).
		WithValidator(&StoryTriggerCustomValidator{
			Client:        mgr.GetAPIReader(),
			Config:        wh.Config,
			ConfigManager: wh.ConfigManager,
		}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-runs-bubustack-io-v1alpha1-storytrigger,mutating=false,failurePolicy=fail,sideEffects=None,groups=runs.bubustack.io,resources=storytriggers,verbs=create;update,versions=v1alpha1,name=vstorytrigger-v1alpha1.kb.io,admissionReviewVersions=v1

// StoryTriggerCustomValidator struct is responsible for validating the StoryTrigger resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type StoryTriggerCustomValidator struct {
	Client        ctrlclient.Reader
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

var _ admission.Validator[*runsv1alpha1.StoryTrigger] = &StoryTriggerCustomValidator{}

func (v *StoryTriggerCustomValidator) controllerConfig() *config.ControllerConfig {
	return webhookshared.ResolveControllerConfig(storytriggerlog, v.ConfigManager, v.Config)
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type StoryTrigger.
func (v *StoryTriggerCustomValidator) ValidateCreate(ctx context.Context, obj *runsv1alpha1.StoryTrigger) (admission.Warnings, error) {
	storytriggerlog.Info("Validation for StoryTrigger upon creation", "name", obj.GetName())
	return nil, v.validateStoryTrigger(ctx, obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type StoryTrigger.
func (v *StoryTriggerCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *runsv1alpha1.StoryTrigger) (admission.Warnings, error) {
	storytriggerlog.Info("Validation for StoryTrigger upon update", "name", newObj.GetName())
	if err := v.validateStoryTrigger(ctx, newObj); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(oldObj.Spec, newObj.Spec) {
		return nil, fmt.Errorf("spec is immutable for StoryTrigger; create a new StoryTrigger for updated trigger requests")
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type StoryTrigger.
func (v *StoryTriggerCustomValidator) ValidateDelete(_ context.Context, obj *runsv1alpha1.StoryTrigger) (admission.Warnings, error) {
	storytriggerlog.Info("Validation for StoryTrigger upon deletion", "name", obj.GetName())
	return nil, nil
}

func (v *StoryTriggerCustomValidator) validateStoryTrigger(ctx context.Context, obj *runsv1alpha1.StoryTrigger) error {
	if err := requireStoryTriggerIdentity(obj); err != nil {
		return err
	}
	if err := validateStoryTriggerInputHash(obj); err != nil {
		return err
	}
	if err := validateStoryTriggerName(obj); err != nil {
		return err
	}
	return v.validateStoryTriggerStoryRef(ctx, obj)
}

func requireStoryTriggerIdentity(obj *runsv1alpha1.StoryTrigger) error {
	if obj == nil {
		return nil
	}
	if strings.TrimSpace(obj.Spec.StoryRef.Name) == "" {
		return fmt.Errorf("spec.storyRef.name is required")
	}

	mode := bubuv1alpha1.TriggerDedupeNone
	if obj.Spec.DeliveryIdentity.Mode != nil {
		mode = *obj.Spec.DeliveryIdentity.Mode
	}

	key := strings.TrimSpace(obj.Spec.DeliveryIdentity.Key)
	inputHash := strings.TrimSpace(obj.Spec.DeliveryIdentity.InputHash)
	submissionID := strings.TrimSpace(obj.Spec.DeliveryIdentity.SubmissionID)

	if submissionID == "" {
		return fmt.Errorf("spec.deliveryIdentity.submissionId is required")
	}
	switch mode {
	case bubuv1alpha1.TriggerDedupeNone:
		if key != "" {
			return fmt.Errorf("spec.deliveryIdentity.key must be empty when spec.deliveryIdentity.mode=%q", mode)
		}
	case bubuv1alpha1.TriggerDedupeToken, bubuv1alpha1.TriggerDedupeKey:
		if key == "" {
			return fmt.Errorf("spec.deliveryIdentity.key is required when spec.deliveryIdentity.mode=%q", mode)
		}
	}
	if key != "" && inputHash == "" {
		return fmt.Errorf("spec.deliveryIdentity.inputHash is required when spec.deliveryIdentity.key is set")
	}
	return nil
}

func validateStoryTriggerName(obj *runsv1alpha1.StoryTrigger) error {
	if obj == nil {
		return nil
	}
	if strings.TrimSpace(obj.Name) == "" {
		return fmt.Errorf("metadata.name is required for StoryTrigger")
	}
	if strings.TrimSpace(obj.GenerateName) != "" {
		return fmt.Errorf("metadata.generateName must be empty for StoryTrigger")
	}

	storyNamespace := obj.Spec.StoryRef.ToNamespacedName(obj).Namespace
	expected := runsidentity.DeriveStoryTriggerName(
		storyNamespace,
		obj.Spec.StoryRef.Name,
		strings.TrimSpace(obj.Spec.DeliveryIdentity.Key),
		strings.TrimSpace(obj.Spec.DeliveryIdentity.SubmissionID),
	)
	if obj.Name != expected {
		return fmt.Errorf("metadata.name must be %q for StoryTrigger (got %q)", expected, obj.Name)
	}
	return nil
}

func validateStoryTriggerInputHash(obj *runsv1alpha1.StoryTrigger) error {
	if obj == nil {
		return nil
	}
	expected := strings.TrimSpace(obj.Spec.DeliveryIdentity.InputHash)
	if expected == "" {
		return nil
	}
	actual, err := runsidentity.ComputeTriggerInputHashFromRawExtension(obj.Spec.Inputs)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("spec.deliveryIdentity.inputHash does not match spec.inputs")
	}
	return nil
}

func (v *StoryTriggerCustomValidator) validateStoryTriggerStoryRef(ctx context.Context, obj *runsv1alpha1.StoryTrigger) error {
	if obj == nil || v.Client == nil {
		return nil
	}

	cfg := v.controllerConfig()
	storyKey := obj.Spec.StoryRef.ToNamespacedName(obj)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		v.Client,
		cfg,
		obj,
		"runs.bubustack.io",
		"StoryTrigger",
		"bubustack.io",
		"Story",
		storyKey.Namespace,
		storyKey.Name,
		"StoryRef",
	); err != nil {
		return err
	}

	story := &bubuv1alpha1.Story{}
	if err := v.Client.Get(ctx, storyKey, story); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("referenced story '%s' not found", storyKey.String())
		}
		return fmt.Errorf("failed to get referenced story '%s': %w", storyKey.String(), err)
	}

	if expectedVersion := strings.TrimSpace(obj.Spec.StoryRef.Version); expectedVersion != "" &&
		strings.TrimSpace(story.Spec.Version) != expectedVersion {
		return fmt.Errorf("referenced story '%s' has version '%s', expected '%s'", storyKey.String(), story.Spec.Version, expectedVersion)
	}

	return nil
}
