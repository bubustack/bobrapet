package v1alpha1

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
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

// transportLookupTimeout is the maximum time the binding webhook will wait for a
// Transport API lookup. It is a defensive cap; the parent admission context deadline
// takes effect first when it is shorter.
const transportLookupTimeout = 5 * time.Second

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
//   - Returns an error if either oldObj or newObj cannot be type-asserted to Transport.
//   - Skips validation if resource is being deleted (DeletionTimestamp set).
//   - Skips validation if spec is unchanged (metadata-only update), using semantic
//     equality to handle *runtime.RawExtension fields correctly.
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
	oldTransport, ok := oldObj.(*transportv1alpha1.Transport)
	if !ok {
		return nil, fmt.Errorf("expected Transport for old object but got %T", oldObj)
	}

	if newTransport.DeletionTimestamp != nil {
		return nil, nil
	}
	transportLog.Info("validating transport update", "name", newTransport.GetName())

	// Use semantic equality so *runtime.RawExtension fields (ConfigSchema, DefaultSettings)
	// are compared by JSON content rather than raw byte identity, which would produce false
	// positives for semantically identical payloads encoded differently.
	if equality.Semantic.DeepEqual(oldTransport.Spec, newTransport.Spec) {
		return nil, nil
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
func (v *TransportValidator) ValidateDelete(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	if transport, ok := obj.(*transportv1alpha1.Transport); ok {
		transportLog.Info("validating transport delete", "name", transport.GetName())
	}
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
type TransportBindingWebhook struct{}

// SetupWebhookWithManager registers the TransportBinding validating webhook with the manager.
//
// Behavior:
//   - Stores the manager's API reader for Transport lookups during validation.
//   - Creates a validating webhook for TransportBinding resources.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//
// Returns:
//   - nil on success.
//   - Error if webhook registration fails.
//
// Note: GetAPIReader() is used instead of the cached client so that Transport lookups
// hit the API server directly. The informer cache may not be fully synced when the
// first admission request arrives, and a stale cache could falsely report a
// recently-deleted Transport as present, allowing an invalid binding through.
//
// Throughput note: every TransportBinding CREATE and spec-changing UPDATE makes a live
// API call to look up the referenced Transport. For clusters with high binding churn
// (e.g., many StoryRun starts in parallel), this adds one synchronous API round-trip
// per admission request. If this becomes a bottleneck, consider a short-TTL in-process
// cache keyed by transport name.
func (w *TransportBindingWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&transportv1alpha1.TransportBinding{}).
		WithValidator(&TransportBindingValidator{Reader: mgr.GetAPIReader()}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-transport-bubustack-io-v1alpha1-transportbinding,mutating=false,failurePolicy=fail,sideEffects=None,groups=transport.bubustack.io,resources=transportbindings,verbs=create;update,versions=v1alpha1,name=vtransportbinding.kb.io,admissionReviewVersions=v1

// TransportBindingValidator validates TransportBinding objects prior to persistence.
type TransportBindingValidator struct {
	Reader client.Reader
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
//   - Returns an error if either oldObj or newObj cannot be type-asserted to TransportBinding.
//   - Skips all validation if resource is being deleted (DeletionTimestamp set).
//   - Rejects changes to immutable identity fields (transportRef, driver).
//   - Skips spec validation if spec is semantically unchanged (metadata-only update).
//   - Delegates to validate for full spec validation otherwise.
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
	oldBinding, ok := oldObj.(*transportv1alpha1.TransportBinding)
	if !ok {
		return nil, fmt.Errorf("expected TransportBinding for old object but got %T", oldObj)
	}

	if binding.DeletionTimestamp != nil {
		return nil, nil
	}
	transportBindingLog.Info("validating transportbinding update", "name", binding.GetName(), "namespace", binding.GetNamespace())

	// Enforce immutability of identity fields that determine which connector is targeted.
	// Changing them after creation would create a split-brain between the stored spec
	// and the live connector state.
	var immutableErrs field.ErrorList
	specPath := field.NewPath("spec")
	// Compare trimmed values to match the lookup semantics used in validate(): a binding
	// created with a whitespace-padded ref or driver (e.g. " livekit", " bobravoz-grpc")
	// must not be considered changed when updated to the normalised form.
	if strings.TrimSpace(binding.Spec.TransportRef) != strings.TrimSpace(oldBinding.Spec.TransportRef) {
		immutableErrs = append(immutableErrs, field.Forbidden(
			specPath.Child("transportRef"),
			"transportRef is immutable after creation",
		))
	}
	if strings.TrimSpace(binding.Spec.Driver) != strings.TrimSpace(oldBinding.Spec.Driver) {
		immutableErrs = append(immutableErrs, field.Forbidden(
			specPath.Child("driver"),
			"driver is immutable after creation",
		))
	}
	if len(immutableErrs) > 0 {
		return nil, apierrors.NewInvalid(
			transportv1alpha1.GroupVersion.WithKind("TransportBinding").GroupKind(),
			binding.GetName(),
			immutableErrs,
		)
	}

	// Skip full validation (and the Transport API lookup it entails) when the spec has not
	// changed. Semantic equality handles *runtime.RawExtension (RawSettings) correctly by
	// comparing JSON content rather than raw byte identity.
	if equality.Semantic.DeepEqual(oldBinding.Spec, binding.Spec) {
		return nil, nil
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
func (v *TransportBindingValidator) ValidateDelete(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	if binding, ok := obj.(*transportv1alpha1.TransportBinding); ok {
		transportBindingLog.Info("validating transportbinding delete", "name", binding.GetName(), "namespace", binding.GetNamespace())
	}
	return nil, nil
}

// validate performs comprehensive validation for TransportBinding resources.
//
// Behavior:
//   - Requires at least one lane (audio, video, or binary) to be specified.
//   - Requires transportRef to reference an existing Transport.
//   - Validates driver matches the referenced Transport's driver.
//   - Validates codec support against the Transport's capabilities, with field-level
//     error paths (spec.audio.codecs, spec.video.codecs, spec.binary.mimeTypes).
//   - Uses transportLookupTimeout as a defensive cap for Transport lookups.
//   - If deterministic field errors were already collected before the Transport lookup
//     and the lookup then fails with a transient error, the field errors are returned
//     rather than the infrastructure error. This ensures the user always receives
//     actionable feedback for issues they can fix immediately.
//
// Arguments:
//   - ctx context.Context: for API calls and cancellation.
//   - binding *transportv1alpha1.TransportBinding: the binding to validate.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, apierrors.Invalid with aggregated field errors otherwise.
//   - nil, error for unexpected API failures during Transport lookup, only when no
//     other field errors were collected before the lookup attempt.
func (v *TransportBindingValidator) validate(ctx context.Context, binding *transportv1alpha1.TransportBinding) (admission.Warnings, error) {
	if v.Reader == nil {
		return nil, fmt.Errorf("webhook reader is not initialized")
	}

	specPath := field.NewPath("spec")
	bindingDriver := strings.TrimSpace(binding.Spec.Driver)
	var allErrs field.ErrorList

	if !transportvalidation.HasAnyLane(binding) {
		allErrs = append(allErrs, field.Invalid(specPath, binding.Spec, "at least one of audio, video, or binary must be specified"))
	}

	// Validate driver presence unconditionally so the caller always gets a complete
	// picture of field errors regardless of whether the transport lookup can proceed.
	if bindingDriver == "" {
		allErrs = append(allErrs, field.Required(specPath.Child("driver"), "driver must be provided"))
	}

	transportName := strings.TrimSpace(binding.Spec.TransportRef)
	if transportName == "" {
		allErrs = append(allErrs, field.Required(specPath.Child("transportRef"), "transportRef must be provided"))
	}

	var transport transportv1alpha1.Transport
	if transportName != "" {
		webhookCtx, cancel := context.WithTimeout(ctx, transportLookupTimeout)
		defer cancel()

		if err := v.Reader.Get(webhookCtx, types.NamespacedName{Name: transportName}, &transport); err != nil {
			if apierrors.IsNotFound(err) {
				allErrs = append(allErrs, field.NotFound(specPath.Child("transportRef"), transportName))
			} else {
				// The Transport API lookup failed with a transient error (e.g. etcd
				// unavailable, context deadline exceeded). If we already collected
				// deterministic field errors above, return them now — the user can act
				// on them immediately without waiting for the infrastructure to recover.
				// Only propagate the transient error when there is nothing else to
				// report, so callers can distinguish "bad request" from "infra failure".
				if len(allErrs) > 0 {
					return nil, apierrors.NewInvalid(
						transportv1alpha1.GroupVersion.WithKind("TransportBinding").GroupKind(),
						binding.GetName(),
						allErrs,
					)
				}
				return nil, fmt.Errorf("failed to look up transport %q: %w", transportName, err)
			}
		} else {
			// Driver presence is already validated above; only check mismatch here.
			if bindingDriver != "" && bindingDriver != transport.Spec.Driver {
				allErrs = append(allErrs, field.Invalid(specPath.Child("driver"), binding.Spec.Driver, fmt.Sprintf("must match transport driver %q", transport.Spec.Driver)))
			}
			// Errors are anchored at spec.audio.codecs / spec.video.codecs /
			// spec.binary.mimeTypes rather than the spec root for actionable feedback.
			allErrs = append(allErrs, transportvalidation.ValidateCodecSupportFields(binding, &transport, specPath)...)
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
