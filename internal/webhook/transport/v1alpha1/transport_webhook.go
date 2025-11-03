package v1alpha1

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	transportvalidation "github.com/bubustack/bobrapet/pkg/transport/validation"
)

var (
	transportLog        = logf.Log.WithName("transport-resource")
	transportBindingLog = logf.Log.WithName("transportbinding-resource")
)

// TransportWebhook registers admission webhooks for Transport resources.
type TransportWebhook struct{}

// SetupWebhookWithManager registers the Transport validating webhook with the manager.
//
// Behavior:
//   - Creates a validating webhook for Transport resources.
//   - Registers TransportValidator for validation logic.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//
// Returns:
//   - nil on success.
//   - Error if webhook registration fails.
func (w *TransportWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&transportv1alpha1.Transport{}).
		WithValidator(&TransportValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-transport-bubustack-io-v1alpha1-transport,mutating=false,failurePolicy=fail,sideEffects=None,groups=transport.bubustack.io,resources=transports,verbs=create;update,versions=v1alpha1,name=vtransport.kb.io,admissionReviewVersions=v1

// TransportValidator performs spec validation for Transport resources.
type TransportValidator struct{}

var _ webhook.CustomValidator = &TransportValidator{}

// ValidateCreate validates Transport creation requests.
//
// Behavior:
//   - Type-asserts obj to Transport and validates spec.
//   - Delegates to validateTransport for field-level checks.
//
// Arguments:
//   - ctx context.Context: unused but required by interface.
//   - obj runtime.Object: expected to be *transportv1alpha1.Transport.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if type assertion fails or validation errors exist.
func (v *TransportValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	transport, ok := obj.(*transportv1alpha1.Transport)
	if !ok {
		return nil, fmt.Errorf("expected Transport but got %T", obj)
	}
	transportLog.Info("validating transport create", "name", transport.GetName())

	if err := v.validateTransport(transport); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate validates Transport updates. Metadata-only updates during deletion are skipped.
//
// Behavior:
//   - Skips validation if resource is being deleted (DeletionTimestamp set).
//   - Skips validation if spec is unchanged (metadata-only update).
//   - Delegates to validateTransport for field-level checks.
//
// Arguments:
//   - ctx context.Context: unused but required by interface.
//   - oldObj runtime.Object: previous Transport state.
//   - newObj runtime.Object: proposed Transport state.
//
// Returns:
//   - nil, nil if validation passes or is skipped.
//   - nil, error if type assertion fails or validation errors exist.
func (v *TransportValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	newTransport, ok := newObj.(*transportv1alpha1.Transport)
	if !ok {
		return nil, fmt.Errorf("expected Transport but got %T", newObj)
	}
	transportLog.Info("validating transport update", "name", newTransport.GetName())

	if newTransport.DeletionTimestamp != nil {
		return nil, nil
	}
	if oldTransport, ok := oldObj.(*transportv1alpha1.Transport); ok {
		if reflect.DeepEqual(oldTransport.Spec, newTransport.Spec) {
			return nil, nil
		}
	}
	if err := v.validateTransport(newTransport); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete allows transport deletions.
//
// Behavior:
//   - Always allows deletion (no-op validation).
//
// Returns:
//   - nil, nil (deletion always allowed).
func (v *TransportValidator) ValidateDelete(context.Context, runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

// validateTransport performs spec validation using the shared transport validation package.
//
// Behavior:
//   - Delegates to transportvalidation.ValidateTransport for field-level checks.
//   - Aggregates validation errors into an API-compatible error response.
//
// Arguments:
//   - transport *transportv1alpha1.Transport: the Transport to validate.
//
// Returns:
//   - nil if validation passes.
//   - apierrors.Invalid with aggregated field errors otherwise.
func (v *TransportValidator) validateTransport(transport *transportv1alpha1.Transport) error {
	allErrs := transportvalidation.ValidateTransport(transport)
	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(
		transportv1alpha1.GroupVersion.WithKind("Transport").GroupKind(),
		transport.GetName(),
		allErrs,
	)
}

// TransportBindingWebhook registers admission webhook for TransportBinding resources.
type TransportBindingWebhook struct {
	client client.Client
}

// SetupWebhookWithManager registers the TransportBinding validating webhook with the manager.
//
// Behavior:
//   - Stores the manager's client for Transport lookups during validation.
//   - Creates a validating webhook for TransportBinding resources.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//
// Returns:
//   - nil on success.
//   - Error if webhook registration fails.
func (w *TransportBindingWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	w.client = mgr.GetClient()

	return ctrl.NewWebhookManagedBy(mgr).
		For(&transportv1alpha1.TransportBinding{}).
		WithValidator(&TransportBindingValidator{Client: w.client}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-transport-bubustack-io-v1alpha1-transportbinding,mutating=false,failurePolicy=fail,sideEffects=None,groups=transport.bubustack.io,resources=transportbindings,verbs=create;update,versions=v1alpha1,name=vtransportbinding.kb.io,admissionReviewVersions=v1

// TransportBindingValidator validates TransportBinding objects prior to persistence.
type TransportBindingValidator struct {
	Client client.Client
}

var _ webhook.CustomValidator = &TransportBindingValidator{}

// ValidateCreate validates new TransportBinding resources.
//
// Behavior:
//   - Type-asserts obj to TransportBinding and validates spec.
//   - Checks lane requirements, transportRef, driver, and codec support.
//
// Arguments:
//   - ctx context.Context: for Transport lookup timeout.
//   - obj runtime.Object: expected to be *transportv1alpha1.TransportBinding.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if type assertion fails or validation errors exist.
func (v *TransportBindingValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	binding, ok := obj.(*transportv1alpha1.TransportBinding)
	if !ok {
		return nil, fmt.Errorf("expected TransportBinding but got %T", obj)
	}
	transportBindingLog.Info("validating transportbinding create", "name", binding.GetName(), "namespace", binding.GetNamespace())
	return v.validate(ctx, binding)
}

// ValidateUpdate validates updates to TransportBinding resources.
//
// Behavior:
//   - Skips validation if resource is being deleted (DeletionTimestamp set).
//   - Skips validation if spec is unchanged (metadata-only update).
//   - Delegates to validate for full spec validation.
//
// Arguments:
//   - ctx context.Context: for Transport lookup timeout.
//   - oldObj runtime.Object: previous TransportBinding state.
//   - newObj runtime.Object: proposed TransportBinding state.
//
// Returns:
//   - nil, nil if validation passes or is skipped.
//   - nil, error if type assertion fails or validation errors exist.
func (v *TransportBindingValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	binding, ok := newObj.(*transportv1alpha1.TransportBinding)
	if !ok {
		return nil, fmt.Errorf("expected TransportBinding but got %T", newObj)
	}
	transportBindingLog.Info("validating transportbinding update", "name", binding.GetName(), "namespace", binding.GetNamespace())

	if binding.DeletionTimestamp != nil {
		return nil, nil
	}
	if oldBinding, ok := oldObj.(*transportv1alpha1.TransportBinding); ok {
		if reflect.DeepEqual(oldBinding.Spec, binding.Spec) {
			return nil, nil
		}
	}

	return v.validate(ctx, binding)
}

// ValidateDelete allows binding deletions.
//
// Behavior:
//   - Always allows deletion (no-op validation).
//
// Returns:
//   - nil, nil (deletion always allowed).
func (v *TransportBindingValidator) ValidateDelete(context.Context, runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

// validate performs comprehensive validation for TransportBinding resources.
//
// Behavior:
//   - Requires at least one lane (audio, video, or binary) to be specified.
//   - Requires transportRef to reference an existing Transport.
//   - Validates driver matches the referenced Transport's driver.
//   - Validates codec support against the Transport's capabilities.
//   - Uses a 5-second timeout for Transport lookups.
//
// Arguments:
//   - ctx context.Context: for API calls and cancellation.
//   - binding *transportv1alpha1.TransportBinding: the binding to validate.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, apierrors.Invalid with aggregated field errors otherwise.
func (v *TransportBindingValidator) validate(ctx context.Context, binding *transportv1alpha1.TransportBinding) (admission.Warnings, error) {
	if v.Client == nil {
		return nil, fmt.Errorf("webhook client is not initialized")
	}

	specPath := field.NewPath("spec")
	var allErrs field.ErrorList

	if !transportvalidation.HasAnyLane(binding) {
		allErrs = append(allErrs, field.Invalid(specPath, binding.Spec, "at least one of audio, video, or binary must be specified"))
	}

	transportName := strings.TrimSpace(binding.Spec.TransportRef)
	if transportName == "" {
		allErrs = append(allErrs, field.Required(specPath.Child("transportRef"), "transportRef must be provided"))
	}

	var transport transportv1alpha1.Transport
	if transportName != "" {
		webhookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := v.Client.Get(webhookCtx, types.NamespacedName{Name: transportName}, &transport); err != nil {
			if apierrors.IsNotFound(err) {
				allErrs = append(allErrs, field.NotFound(specPath.Child("transportRef"), transportName))
			} else {
				return nil, err
			}
		} else {
			if binding.Spec.Driver == "" {
				allErrs = append(allErrs, field.Required(specPath.Child("driver"), "driver must be provided"))
			} else if !strings.EqualFold(binding.Spec.Driver, transport.Spec.Driver) {
				allErrs = append(allErrs, field.Invalid(specPath.Child("driver"), binding.Spec.Driver, fmt.Sprintf("must match transport driver %q", transport.Spec.Driver)))
			}
			if err := transportvalidation.ValidateCodecSupport(binding, &transport); err != nil {
				allErrs = append(allErrs, field.Invalid(specPath, binding.Spec, err.Error()))
			}
		}
	}

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(
		transportv1alpha1.GroupVersion.WithKind("TransportBinding").GroupKind(),
		binding.GetName(),
		allErrs,
	)
}
