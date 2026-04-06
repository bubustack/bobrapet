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

package runs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/bobrapet/pkg/storage"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

// StoryTriggerReconciler reconciles a StoryTrigger object
type StoryTriggerReconciler struct {
	client.Client
	APIReader      client.Reader
	Scheme         *runtime.Scheme
	ConfigResolver *config.Resolver
}

// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storytriggers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storytriggers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storytriggers/finalizers,verbs=update
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch

const (
	storyTriggerReasonStoryRunCreated      = "StoryRunCreated"
	storyTriggerReasonStoryRunReused       = "StoryRunReused"
	storyTriggerReasonStoryRunConflict     = "StoryRunConflict"
	storyTriggerReasonStoryVersionMismatch = "StoryVersionMismatch"
	storyTriggerReasonInputHashMismatch    = "InputHashMismatch"
)

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
//nolint:gocyclo // complex by design
func (r *StoryTriggerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	trigger := &runsv1alpha1.StoryTrigger{}
	if err := r.Get(ctx, req.NamespacedName, trigger); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if trigger.GetDeletionTimestamp() != nil {
		return ctrl.Result{}, nil
	}

	switch trigger.Status.Decision {
	case runsv1alpha1.StoryTriggerDecisionCreated,
		runsv1alpha1.StoryTriggerDecisionReused,
		runsv1alpha1.StoryTriggerDecisionRejected:
		if trigger.Status.ObservedGeneration == trigger.Generation {
			return ctrl.Result{}, nil
		}
	}

	if err := r.validateTriggerRequest(ctx, trigger); err != nil {
		return ctrl.Result{}, r.markRejected(ctx, trigger, conditions.ReasonValidationFailed, err.Error())
	}

	story, storyKey, err := r.fetchStory(ctx, trigger)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.markRejected(ctx, trigger, conditions.ReasonStoryNotFound, fmt.Sprintf("referenced Story %q was not found", storyKey.String()))
		}
		return ctrl.Result{}, err
	}
	if expectedVersion := strings.TrimSpace(trigger.Spec.StoryRef.Version); expectedVersion != "" &&
		strings.TrimSpace(story.Spec.Version) != expectedVersion {
		return ctrl.Result{}, r.markRejected(
			ctx,
			trigger,
			storyTriggerReasonStoryVersionMismatch,
			fmt.Sprintf("referenced Story %q has version %q, expected %q", storyKey.String(), story.Spec.Version, expectedVersion),
		)
	}

	inputHash, err := resolveTriggerInputHash(trigger)
	if err != nil {
		return ctrl.Result{}, r.markRejected(ctx, trigger, storyTriggerReasonInputHashMismatch, err.Error())
	}

	desired, err := r.prepareStoryRunForCreate(ctx, trigger, inputHash)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, desired); err != nil {
		if apierrors.IsAlreadyExists(err) {
			existing := &runsv1alpha1.StoryRun{}
			key := client.ObjectKeyFromObject(desired)
			if getErr := r.reader().Get(ctx, key, existing); getErr != nil {
				return ctrl.Result{}, getErr
			}
			if storyRunMatchesTrigger(existing, trigger, inputHash) {
				if storyRunOriginatesFromTrigger(existing, trigger) {
					log.Info("recovered previously created StoryRun for StoryTrigger", "storyTrigger", req.NamespacedName, "storyRun", key)
					return ctrl.Result{}, r.markResolved(ctx, trigger, runsv1alpha1.StoryTriggerDecisionCreated, storyTriggerReasonStoryRunCreated, fmt.Sprintf("recovered previously created StoryRun %q", key.String()), existing)
				}
				log.Info("reused existing StoryRun for StoryTrigger", "storyTrigger", req.NamespacedName, "storyRun", key)
				return ctrl.Result{}, r.markResolved(ctx, trigger, runsv1alpha1.StoryTriggerDecisionReused, storyTriggerReasonStoryRunReused, fmt.Sprintf("reused existing StoryRun %q", key.String()), existing)
			}
			return ctrl.Result{}, r.markRejected(ctx, trigger, storyTriggerReasonStoryRunConflict, fmt.Sprintf("existing StoryRun %q does not match StoryTrigger storyRef and inputs", key.String()))
		}
		if apierrors.IsInvalid(err) {
			return ctrl.Result{}, r.markRejected(ctx, trigger, conditions.ReasonValidationFailed, fmt.Sprintf("StoryRun admission rejected the request: %v", err))
		}
		return ctrl.Result{}, err
	}

	log.Info("created StoryRun for StoryTrigger", "storyTrigger", req.NamespacedName, "storyRun", client.ObjectKeyFromObject(desired))
	return ctrl.Result{}, r.markResolved(ctx, trigger, runsv1alpha1.StoryTriggerDecisionCreated, storyTriggerReasonStoryRunCreated, fmt.Sprintf("created StoryRun %q", client.ObjectKeyFromObject(desired).String()), desired)
}

func (r *StoryTriggerReconciler) validateTriggerRequest(ctx context.Context, trigger *runsv1alpha1.StoryTrigger) error {
	if err := requireStoryTriggerIdentity(trigger); err != nil {
		return err
	}
	if err := validateStoryTriggerName(trigger); err != nil {
		return err
	}
	return r.validateStoryRefAccess(ctx, trigger)
}

func (r *StoryTriggerReconciler) validateStoryRefAccess(ctx context.Context, trigger *runsv1alpha1.StoryTrigger) error {
	if trigger == nil {
		return nil
	}

	storyKey := trigger.Spec.StoryRef.ToNamespacedName(trigger)
	fromNamespace := strings.TrimSpace(trigger.Namespace)
	targetNamespace := strings.TrimSpace(storyKey.Namespace)
	if targetNamespace == "" || fromNamespace == "" || targetNamespace == fromNamespace {
		return nil
	}

	switch r.referencePolicy() {
	case config.ReferenceCrossNamespacePolicyAllow:
		return nil
	case config.ReferenceCrossNamespacePolicyGrant:
		allowed, err := refs.ReferenceGranted(ctx, r.reader(), refs.GrantCheck{
			FromGroup:     runsv1alpha1.GroupVersion.Group,
			FromKind:      "StoryTrigger",
			FromNamespace: fromNamespace,
			ToGroup:       bubuv1alpha1.GroupVersion.Group,
			ToKind:        "Story",
			ToNamespace:   targetNamespace,
			ToName:        strings.TrimSpace(storyKey.Name),
		})
		if err != nil {
			return fmt.Errorf("failed to evaluate ReferenceGrant for StoryRef: %w", err)
		}
		if !allowed {
			return fmt.Errorf("cross-namespace StoryRef reference to %s/%s is not permitted (no ReferenceGrant)", targetNamespace, storyKey.Name)
		}
		return nil
	default:
		return fmt.Errorf("cross-namespace StoryRef references are not allowed")
	}
}

func (r *StoryTriggerReconciler) fetchStory(ctx context.Context, trigger *runsv1alpha1.StoryTrigger) (*bubuv1alpha1.Story, types.NamespacedName, error) {
	storyKey := trigger.Spec.StoryRef.ToNamespacedName(trigger)
	story := &bubuv1alpha1.Story{}
	if err := r.reader().Get(ctx, storyKey, story); err != nil {
		return nil, storyKey, err
	}
	return story, storyKey, nil
}

func (r *StoryTriggerReconciler) reader() client.Reader {
	if r != nil && r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *StoryTriggerReconciler) referencePolicy() string {
	cfg := config.DefaultControllerConfig()
	if r != nil && r.ConfigResolver != nil {
		if operatorConfig := r.ConfigResolver.GetOperatorConfig(); operatorConfig != nil {
			resolved := operatorConfig.Controller.Clone()
			cfg = &resolved
		}
	}
	policy := strings.TrimSpace(cfg.ReferenceCrossNamespacePolicy)
	if policy == "" {
		return config.ReferenceCrossNamespacePolicyDeny
	}
	return policy
}

func resolveTriggerInputHash(trigger *runsv1alpha1.StoryTrigger) (string, error) {
	hash, err := runsidentity.ComputeTriggerInputHashFromRawExtension(trigger.Spec.Inputs)
	if err != nil {
		return "", fmt.Errorf("failed to compute trigger input hash: %w", err)
	}
	expected := strings.TrimSpace(trigger.Spec.DeliveryIdentity.InputHash)
	if expected != "" && expected != hash {
		return "", fmt.Errorf("spec.deliveryIdentity.inputHash does not match spec.inputs")
	}
	return hash, nil
}

func (r *StoryTriggerReconciler) prepareStoryRunForCreate(
	ctx context.Context,
	trigger *runsv1alpha1.StoryTrigger,
	inputHash string,
) (*runsv1alpha1.StoryRun, error) {
	desired := desiredStoryRunForTrigger(trigger, inputHash)
	if desired == nil || desired.Spec.Inputs == nil || len(desired.Spec.Inputs.Raw) == 0 {
		return desired, nil
	}

	maxInlineBytes := r.storyRunMaxInlineInputsSize()
	if maxInlineBytes <= 0 || len(desired.Spec.Inputs.Raw) <= maxInlineBytes {
		return desired, nil
	}

	var inputs any
	if err := json.Unmarshal(desired.Spec.Inputs.Raw, &inputs); err != nil {
		return nil, fmt.Errorf("failed to decode StoryTrigger inputs for StoryRun offload: %w", err)
	}

	if _, ok := inputs.(map[string]any); !ok {
		return nil, fmt.Errorf("StoryTrigger inputs must be a JSON object before creating a StoryRun")
	}

	manager, err := storage.SharedManager(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize shared storage for StoryRun inputs offload: %w", err)
	}
	dehydrated, err := manager.DehydrateInputsWithMaxInlineSize(ctx, inputs, desired.Name, maxInlineBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to offload StoryTrigger inputs for StoryRun create: %w", err)
	}

	raw, err := json.Marshal(dehydrated)
	if err != nil {
		return nil, fmt.Errorf("failed to encode dehydrated StoryTrigger inputs: %w", err)
	}
	desired.Spec.Inputs.Raw = raw
	return desired, nil
}

func (r *StoryTriggerReconciler) storyRunMaxInlineInputsSize() int {
	cfg := config.DefaultControllerConfig()
	if r != nil && r.ConfigResolver != nil {
		if operatorConfig := r.ConfigResolver.GetOperatorConfig(); operatorConfig != nil {
			resolved := operatorConfig.Controller.Clone()
			cfg = &resolved
		}
	}
	if cfg != nil && cfg.StoryRun.MaxInlineInputsSize > 0 {
		return cfg.StoryRun.MaxInlineInputsSize
	}
	return config.DefaultControllerConfig().StoryRun.MaxInlineInputsSize
}

func desiredStoryRunForTrigger(trigger *runsv1alpha1.StoryTrigger, inputHash string) *runsv1alpha1.StoryRun {
	identity := runsidentity.StoryTriggerIdentity(
		strings.TrimSpace(trigger.Spec.DeliveryIdentity.Key),
		strings.TrimSpace(trigger.Spec.DeliveryIdentity.SubmissionID),
	)
	storyNamespace := trigger.Spec.StoryRef.ToNamespacedName(trigger).Namespace

	var inputs *runtime.RawExtension
	if trigger.Spec.Inputs != nil {
		inputs = trigger.Spec.Inputs.DeepCopy()
	}

	var impulseRef *refs.ImpulseReference
	if trigger.Spec.ImpulseRef != nil {
		impulseRef = trigger.Spec.ImpulseRef.DeepCopy()
	}

	annotations := map[string]string{}
	annotations[runsidentity.StoryRunTriggerRequestNameAnnotation] = trigger.Name
	annotations[runsidentity.StoryRunTriggerRequestUIDAnnotation] = string(trigger.GetUID())
	if key := strings.TrimSpace(trigger.Spec.DeliveryIdentity.Key); key != "" {
		annotations[runsidentity.StoryRunTriggerTokenAnnotation] = key
		annotations[runsidentity.StoryRunTriggerInputHashAnnotation] = inputHash
	}

	return &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:        runsidentity.DeriveStoryRunName(storyNamespace, trigger.Spec.StoryRef.Name, identity),
			Namespace:   trigger.Namespace,
			Annotations: annotations,
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef:   *trigger.Spec.StoryRef.DeepCopy(),
			ImpulseRef: impulseRef,
			Inputs:     inputs,
		},
	}
}

func storyRunMatchesTrigger(existing *runsv1alpha1.StoryRun, trigger *runsv1alpha1.StoryTrigger, triggerHash string) bool {
	if existing == nil || trigger == nil {
		return false
	}

	if existing.Spec.StoryRef.ToNamespacedName(existing) != trigger.Spec.StoryRef.ToNamespacedName(trigger) {
		return false
	}
	if strings.TrimSpace(existing.Spec.StoryRef.Version) != strings.TrimSpace(trigger.Spec.StoryRef.Version) {
		return false
	}
	if !impulseRefsEqual(existing, trigger) {
		return false
	}

	existingHash, err := storyRunInputHash(existing)
	if err != nil {
		return false
	}
	return existingHash == triggerHash
}

func storyRunInputHash(existing *runsv1alpha1.StoryRun) (string, error) {
	if existing == nil {
		return "", fmt.Errorf("storyrun is nil")
	}
	if ann := strings.TrimSpace(existing.GetAnnotations()[runsidentity.StoryRunTriggerInputHashAnnotation]); ann != "" {
		return ann, nil
	}
	return runsidentity.ComputeTriggerInputHashFromRawExtension(existing.Spec.Inputs)
}

func impulseRefsEqual(existing *runsv1alpha1.StoryRun, trigger *runsv1alpha1.StoryTrigger) bool {
	switch {
	case existing == nil || trigger == nil:
		return false
	case existing.Spec.ImpulseRef == nil && trigger.Spec.ImpulseRef == nil:
		return true
	case existing.Spec.ImpulseRef == nil || trigger.Spec.ImpulseRef == nil:
		return false
	}

	if existing.Spec.ImpulseRef.ToNamespacedName(existing) != trigger.Spec.ImpulseRef.ToNamespacedName(trigger) {
		return false
	}
	if strings.TrimSpace(existing.Spec.ImpulseRef.Version) != strings.TrimSpace(trigger.Spec.ImpulseRef.Version) {
		return false
	}
	return uidPtrString(existing.Spec.ImpulseRef.UID) == uidPtrString(trigger.Spec.ImpulseRef.UID)
}

func uidPtrString(value *types.UID) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

func storyRunOriginatesFromTrigger(existing *runsv1alpha1.StoryRun, trigger *runsv1alpha1.StoryTrigger) bool {
	if existing == nil || trigger == nil {
		return false
	}
	ann := existing.GetAnnotations()
	if len(ann) == 0 {
		return false
	}
	if strings.TrimSpace(ann[runsidentity.StoryRunTriggerRequestNameAnnotation]) != trigger.Name {
		return false
	}
	uid := strings.TrimSpace(ann[runsidentity.StoryRunTriggerRequestUIDAnnotation])
	if uid == "" {
		return false
	}
	return uid == string(trigger.GetUID())
}

func requireStoryTriggerIdentity(trigger *runsv1alpha1.StoryTrigger) error {
	if trigger == nil {
		return nil
	}
	if strings.TrimSpace(trigger.Spec.StoryRef.Name) == "" {
		return fmt.Errorf("spec.storyRef.name is required")
	}

	mode := bubuv1alpha1.TriggerDedupeNone
	if trigger.Spec.DeliveryIdentity.Mode != nil {
		mode = *trigger.Spec.DeliveryIdentity.Mode
	}

	key := strings.TrimSpace(trigger.Spec.DeliveryIdentity.Key)
	inputHash := strings.TrimSpace(trigger.Spec.DeliveryIdentity.InputHash)
	submissionID := strings.TrimSpace(trigger.Spec.DeliveryIdentity.SubmissionID)

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

func validateStoryTriggerName(trigger *runsv1alpha1.StoryTrigger) error {
	if trigger == nil {
		return nil
	}
	if strings.TrimSpace(trigger.Name) == "" {
		return fmt.Errorf("metadata.name is required for StoryTrigger")
	}
	if strings.TrimSpace(trigger.GenerateName) != "" {
		return fmt.Errorf("metadata.generateName must be empty for StoryTrigger")
	}

	storyNamespace := trigger.Spec.StoryRef.ToNamespacedName(trigger).Namespace
	expected := runsidentity.DeriveStoryTriggerName(
		storyNamespace,
		trigger.Spec.StoryRef.Name,
		strings.TrimSpace(trigger.Spec.DeliveryIdentity.Key),
		strings.TrimSpace(trigger.Spec.DeliveryIdentity.SubmissionID),
	)
	if trigger.Name != expected {
		return fmt.Errorf("metadata.name must be %q for StoryTrigger (got %q)", expected, trigger.Name)
	}
	return nil
}

func (r *StoryTriggerReconciler) markResolved(
	ctx context.Context,
	trigger *runsv1alpha1.StoryTrigger,
	decision runsv1alpha1.StoryTriggerDecision,
	reason, message string,
	storyRun *runsv1alpha1.StoryRun,
) error {
	return kubeutil.RetryableStatusPatch(ctx, r.Client, trigger, func(obj client.Object) {
		current := obj.(*runsv1alpha1.StoryTrigger)
		now := metav1.Now()
		cm := conditions.NewConditionManager(current.Generation)

		if current.Status.AcceptedAt == nil {
			current.Status.AcceptedAt = &now
		}
		current.Status.CompletedAt = &now
		current.Status.ObservedGeneration = current.Generation
		current.Status.Decision = decision
		current.Status.Reason = reason
		current.Status.Message = message
		current.Status.StoryRunRef = storyRunRef(storyRun)

		cm.SetReadyCondition(&current.Status.Conditions, true, reason, message)
		cm.SetProgressingCondition(&current.Status.Conditions, false, reason, message)
		cm.SetDegradedCondition(&current.Status.Conditions, false, reason, message)
	})
}

func (r *StoryTriggerReconciler) markRejected(ctx context.Context, trigger *runsv1alpha1.StoryTrigger, reason, message string) error {
	return kubeutil.RetryableStatusPatch(ctx, r.Client, trigger, func(obj client.Object) {
		current := obj.(*runsv1alpha1.StoryTrigger)
		now := metav1.Now()
		cm := conditions.NewConditionManager(current.Generation)

		if current.Status.AcceptedAt == nil {
			current.Status.AcceptedAt = &now
		}
		current.Status.CompletedAt = &now
		current.Status.ObservedGeneration = current.Generation
		current.Status.Decision = runsv1alpha1.StoryTriggerDecisionRejected
		current.Status.Reason = reason
		current.Status.Message = message
		current.Status.StoryRunRef = nil

		cm.SetReadyCondition(&current.Status.Conditions, false, reason, message)
		cm.SetProgressingCondition(&current.Status.Conditions, false, reason, message)
		cm.SetDegradedCondition(&current.Status.Conditions, true, reason, message)
	})
}

func storyRunRef(storyRun *runsv1alpha1.StoryRun) *refs.StoryRunReference {
	if storyRun == nil {
		return nil
	}
	ref := &refs.StoryRunReference{
		ObjectReference: refs.ObjectReference{
			Name: storyRun.Name,
		},
	}
	if storyRun.Namespace != "" {
		namespace := storyRun.Namespace
		ref.Namespace = &namespace
	}
	if uid := storyRun.GetUID(); uid != "" {
		ref.UID = &uid
	}
	return ref
}

// SetupWithManager sets up the controller with the Manager.
func (r *StoryTriggerReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(opts).
		For(&runsv1alpha1.StoryTrigger{}).
		Named("runs-storytrigger").
		Complete(r)
}
