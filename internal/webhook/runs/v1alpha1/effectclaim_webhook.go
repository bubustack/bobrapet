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
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var effectclaimlog = logf.Log.WithName("effectclaim-resource")

// EffectClaimWebhook wires the EffectClaim validator into the manager.
type EffectClaimWebhook struct {
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

// SetupEffectClaimWebhookWithManager registers the webhook for EffectClaim in the manager.
func SetupEffectClaimWebhookWithManager(mgr ctrl.Manager) error {
	return (&EffectClaimWebhook{
		Config: config.DefaultControllerConfig(),
	}).SetupWebhookWithManager(mgr)
}

// SetupWebhookWithManager registers the EffectClaim validating webhook with the manager.
func (wh *EffectClaimWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &runsv1alpha1.EffectClaim{}).
		WithValidator(&EffectClaimCustomValidator{
			Client:        mgr.GetAPIReader(),
			Config:        wh.Config,
			ConfigManager: wh.ConfigManager,
		}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-runs-bubustack-io-v1alpha1-effectclaim,mutating=false,failurePolicy=fail,sideEffects=None,groups=runs.bubustack.io,resources=effectclaims,verbs=create;update,versions=v1alpha1,name=veffectclaim-v1alpha1.kb.io,admissionReviewVersions=v1

// EffectClaimCustomValidator validates EffectClaim create and update operations.
type EffectClaimCustomValidator struct {
	Client        ctrlclient.Reader
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

var _ admission.Validator[*runsv1alpha1.EffectClaim] = &EffectClaimCustomValidator{}

func (v *EffectClaimCustomValidator) controllerConfig() *config.ControllerConfig {
	return webhookshared.ResolveControllerConfig(effectclaimlog, v.ConfigManager, v.Config)
}

func (v *EffectClaimCustomValidator) ValidateCreate(ctx context.Context, obj *runsv1alpha1.EffectClaim) (admission.Warnings, error) {
	effectclaimlog.Info("Validation for EffectClaim upon creation", "name", obj.GetName())
	return nil, v.validateEffectClaim(ctx, obj)
}

func (v *EffectClaimCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *runsv1alpha1.EffectClaim) (admission.Warnings, error) {
	effectclaimlog.Info("Validation for EffectClaim upon update", "name", newObj.GetName())
	if err := v.validateEffectClaim(ctx, newObj); err != nil {
		return nil, err
	}
	if err := validateEffectClaimIdentityImmutable(oldObj, newObj); err != nil {
		return nil, err
	}
	if err := validateEffectClaimCompletionTransition(oldObj, newObj); err != nil {
		return nil, err
	}
	return nil, nil
}

func (v *EffectClaimCustomValidator) ValidateDelete(_ context.Context, obj *runsv1alpha1.EffectClaim) (admission.Warnings, error) {
	effectclaimlog.Info("Validation for EffectClaim upon deletion", "name", obj.GetName())
	return nil, nil
}

func (v *EffectClaimCustomValidator) validateEffectClaim(ctx context.Context, obj *runsv1alpha1.EffectClaim) error {
	if err := requireEffectClaimIdentity(obj); err != nil {
		return err
	}
	if err := validateEffectClaimName(obj); err != nil {
		return err
	}
	return v.validateEffectClaimStepRunRef(ctx, obj)
}

//nolint:gocyclo // complex by design
func requireEffectClaimIdentity(obj *runsv1alpha1.EffectClaim) error {
	if obj == nil {
		return nil
	}
	if strings.TrimSpace(obj.Spec.StepRunRef.Name) == "" {
		return fmt.Errorf("spec.stepRunRef.name is required")
	}
	if strings.TrimSpace(obj.Spec.EffectKey) == "" {
		return fmt.Errorf("spec.effectKey is required")
	}
	if obj.Spec.CompletedAt != nil && strings.TrimSpace(string(obj.Spec.CompletionStatus)) == "" {
		return fmt.Errorf("spec.completionStatus is required when spec.completedAt is set")
	}
	if strings.TrimSpace(string(obj.Spec.CompletionStatus)) != "" && obj.Spec.CompletedAt == nil {
		return fmt.Errorf("spec.completedAt is required when spec.completionStatus is set")
	}
	if obj.Spec.RenewTime != nil && obj.Spec.AcquireTime == nil {
		return fmt.Errorf("spec.acquireTime is required when spec.renewTime is set")
	}
	if obj.Spec.AcquireTime != nil && obj.Spec.RenewTime != nil && obj.Spec.RenewTime.Time.Before(obj.Spec.AcquireTime.Time) {
		return fmt.Errorf("spec.renewTime cannot be before spec.acquireTime")
	}
	if obj.Spec.CompletedAt != nil && obj.Spec.AcquireTime != nil && obj.Spec.CompletedAt.Time.Before(obj.Spec.AcquireTime.Time) {
		return fmt.Errorf("spec.completedAt cannot be before spec.acquireTime")
	}
	if obj.Spec.CompletedAt != nil && obj.Spec.RenewTime != nil && obj.Spec.CompletedAt.Time.Before(obj.Spec.RenewTime.Time) {
		return fmt.Errorf("spec.completedAt cannot be before spec.renewTime")
	}
	return nil
}

func validateEffectClaimName(obj *runsv1alpha1.EffectClaim) error {
	if obj == nil {
		return nil
	}
	if strings.TrimSpace(obj.Name) == "" {
		return fmt.Errorf("metadata.name is required for EffectClaim")
	}
	if strings.TrimSpace(obj.GenerateName) != "" {
		return fmt.Errorf("metadata.generateName must be empty for EffectClaim")
	}

	stepRunNamespace := obj.Spec.StepRunRef.ToNamespacedName(obj).Namespace
	expected := runsidentity.DeriveEffectClaimName(
		stepRunNamespace,
		obj.Spec.StepRunRef.Name,
		strings.TrimSpace(obj.Spec.EffectKey),
	)
	if obj.Name != expected {
		return fmt.Errorf("metadata.name must be %q for EffectClaim (got %q)", expected, obj.Name)
	}
	return nil
}

func validateEffectClaimIdentityImmutable(oldObj, newObj *runsv1alpha1.EffectClaim) error {
	if oldObj == nil || newObj == nil {
		return nil
	}
	if !stepRunRefsEqual(oldObj.Spec.StepRunRef, newObj.Spec.StepRunRef) ||
		strings.TrimSpace(oldObj.Spec.EffectKey) != strings.TrimSpace(newObj.Spec.EffectKey) ||
		strings.TrimSpace(oldObj.Spec.IdempotencyKey) != strings.TrimSpace(newObj.Spec.IdempotencyKey) {
		return fmt.Errorf("spec identity fields are immutable for EffectClaim; create a new EffectClaim for a different StepRun/effect identity")
	}
	return nil
}

func stepRunRefsEqual(a, b refs.StepRunReference) bool {
	if strings.TrimSpace(a.Name) != strings.TrimSpace(b.Name) {
		return false
	}
	if stringPtrValue(a.Namespace) != stringPtrValue(b.Namespace) {
		return false
	}
	return uidPtrValue(a.UID) == uidPtrValue(b.UID)
}

func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func uidPtrValue(value *types.UID) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

func validateEffectClaimCompletionTransition(oldObj, newObj *runsv1alpha1.EffectClaim) error {
	if oldObj == nil || newObj == nil {
		return nil
	}

	if oldObj.Spec.CompletedAt != nil {
		if newObj.Spec.CompletedAt == nil {
			return fmt.Errorf("spec.completedAt cannot be cleared once set")
		}
		if newObj.Spec.CompletedAt.Time.Before(oldObj.Spec.CompletedAt.Time) {
			return fmt.Errorf("spec.completedAt cannot move backward")
		}
	}

	if oldObj.Spec.CompletionStatus == runsv1alpha1.EffectClaimCompletionStatusCompleted {
		if newObj.Spec.CompletionStatus != runsv1alpha1.EffectClaimCompletionStatusCompleted {
			return fmt.Errorf("spec.completionStatus cannot leave %q once set", runsv1alpha1.EffectClaimCompletionStatusCompleted)
		}
		if strings.TrimSpace(oldObj.Spec.HolderIdentity) != strings.TrimSpace(newObj.Spec.HolderIdentity) ||
			oldObj.Spec.LeaseDurationSeconds != newObj.Spec.LeaseDurationSeconds ||
			oldObj.Spec.LeaseTransitions != newObj.Spec.LeaseTransitions ||
			!microTimesEqual(oldObj.Spec.AcquireTime, newObj.Spec.AcquireTime) ||
			!microTimesEqual(oldObj.Spec.RenewTime, newObj.Spec.RenewTime) {
			return fmt.Errorf("completed EffectClaims cannot reassign holder or lease fields")
		}
	}

	return nil
}

func microTimesEqual(a, b *metav1.MicroTime) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.Time.Equal(b.Time)
	}
}

func (v *EffectClaimCustomValidator) validateEffectClaimStepRunRef(ctx context.Context, obj *runsv1alpha1.EffectClaim) error {
	if obj == nil || v.Client == nil {
		return nil
	}

	cfg := v.controllerConfig()
	stepRunKey := obj.Spec.StepRunRef.ToNamespacedName(obj)
	if stepRunKey.Namespace != obj.Namespace {
		return fmt.Errorf("cross-namespace StepRunRef is not allowed for EffectClaim")
	}
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		v.Client,
		cfg,
		obj,
		"runs.bubustack.io",
		"EffectClaim",
		"runs.bubustack.io",
		"StepRun",
		stepRunKey.Namespace,
		stepRunKey.Name,
		"StepRunRef",
	); err != nil {
		return err
	}

	stepRun := &runsv1alpha1.StepRun{}
	if err := v.Client.Get(ctx, stepRunKey, stepRun); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("referenced stepRun '%s' not found", stepRunKey.String())
		}
		return fmt.Errorf("failed to get referenced stepRun '%s': %w", stepRunKey.String(), err)
	}
	if obj.Spec.StepRunRef.UID != nil && stepRun.UID != *obj.Spec.StepRunRef.UID {
		return fmt.Errorf("referenced stepRun '%s' has uid '%s', expected '%s'", stepRunKey.String(), stepRun.UID, *obj.Spec.StepRunRef.UID)
	}

	return nil
}
