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
	"fmt"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// EffectClaimReconciler reconciles an EffectClaim object.
type EffectClaimReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=runs.bubustack.io,resources=effectclaims,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=effectclaims/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;list;watch

const (
	effectClaimReasonReserved       = "Reserved"
	effectClaimReasonReleased       = "Released"
	effectClaimReasonAbandoned      = "Abandoned"
	effectClaimReasonStepRunMissing = "StepRunMissing"
	effectClaimReasonStepRunDrift   = "StepRunUIDDrift"
)

// Reconcile keeps EffectClaim status canonical and ties retention to the owning StepRun.
//
//nolint:gocyclo // complex by design
func (r *EffectClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	claim := &runsv1alpha1.EffectClaim{}
	if err := r.Get(ctx, req.NamespacedName, claim); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if claim.GetDeletionTimestamp() != nil {
		return ctrl.Result{}, nil
	}

	stepRunKey := claim.Spec.StepRunRef.ToNamespacedName(claim)
	if stepRunKey.Namespace != claim.Namespace {
		log.Info("Deleting invalid EffectClaim with cross-namespace StepRunRef", "effectClaim", req.NamespacedName, "stepRun", stepRunKey)
		return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, claim))
	}

	stepRun, err := r.fetchReferencedStepRun(ctx, claim)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Deleting orphan EffectClaim because the referenced StepRun no longer exists", "effectClaim", req.NamespacedName, "stepRun", stepRunKey)
			return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, claim))
		}
		return ctrl.Result{}, err
	}
	if claim.Spec.StepRunRef.UID != nil && *claim.Spec.StepRunRef.UID != stepRun.UID {
		log.Info("Deleting drifted EffectClaim because the referenced StepRun UID no longer matches", "effectClaim", req.NamespacedName, "stepRun", stepRunKey, "expectedUID", *claim.Spec.StepRunRef.UID, "actualUID", stepRun.UID)
		return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, claim))
	}
	if changed, err := ensureEffectClaimOwnerReference(claim, stepRun, r.Scheme); err != nil {
		return ctrl.Result{}, err
	} else if changed {
		if err := r.Update(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
	}

	phase, reason, message := effectClaimLifecycle(claim)
	if err := kubeutil.RetryableStatusPatch(ctx, r.Client, claim, func(obj client.Object) {
		current := obj.(*runsv1alpha1.EffectClaim)
		current.Status.ObservedGeneration = current.Generation
		current.Status.Phase = phase

		cm := conditions.NewConditionManager(current.Generation)
		switch phase {
		case runsv1alpha1.EffectClaimPhaseCompleted:
			cm.SetReadyCondition(&current.Status.Conditions, true, reason, message)
			cm.SetProgressingCondition(&current.Status.Conditions, false, reason, message)
			cm.SetDegradedCondition(&current.Status.Conditions, false, reason, message)
		case runsv1alpha1.EffectClaimPhaseReserved:
			cm.SetReadyCondition(&current.Status.Conditions, false, reason, message)
			cm.SetProgressingCondition(&current.Status.Conditions, true, reason, message)
			cm.SetDegradedCondition(&current.Status.Conditions, false, reason, message)
		case runsv1alpha1.EffectClaimPhaseReleased:
			cm.SetReadyCondition(&current.Status.Conditions, false, reason, message)
			cm.SetProgressingCondition(&current.Status.Conditions, false, reason, message)
			cm.SetDegradedCondition(&current.Status.Conditions, false, reason, message)
		case runsv1alpha1.EffectClaimPhaseAbandoned:
			cm.SetReadyCondition(&current.Status.Conditions, false, reason, message)
			cm.SetProgressingCondition(&current.Status.Conditions, false, reason, message)
			cm.SetDegradedCondition(&current.Status.Conditions, true, reason, message)
		}
	}); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *EffectClaimReconciler) fetchReferencedStepRun(ctx context.Context, claim *runsv1alpha1.EffectClaim) (*runsv1alpha1.StepRun, error) {
	if claim == nil {
		return nil, nil
	}

	stepRunKey := claim.Spec.StepRunRef.ToNamespacedName(claim)
	stepRun := &runsv1alpha1.StepRun{}
	if err := r.Get(ctx, stepRunKey, stepRun); err != nil {
		if !apierrors.IsNotFound(err) || r.APIReader == nil {
			return nil, err
		}
		if err := r.APIReader.Get(ctx, stepRunKey, stepRun); err != nil {
			return nil, err
		}
	}

	return stepRun, nil
}

func ensureEffectClaimOwnerReference(claim *runsv1alpha1.EffectClaim, stepRun *runsv1alpha1.StepRun, scheme *runtime.Scheme) (bool, error) {
	if claim == nil || stepRun == nil || scheme == nil {
		return false, nil
	}
	for _, ref := range claim.GetOwnerReferences() {
		if ref.APIVersion == runsv1alpha1.GroupVersion.String() &&
			ref.Kind == "StepRun" &&
			ref.Name == stepRun.Name &&
			ref.UID == stepRun.UID {
			return false, nil
		}
	}
	if err := controllerutil.SetOwnerReference(stepRun, claim, scheme); err != nil {
		return false, fmt.Errorf("failed to set StepRun owner reference on EffectClaim: %w", err)
	}
	return true, nil
}

func effectClaimLifecycle(claim *runsv1alpha1.EffectClaim) (runsv1alpha1.EffectClaimPhase, string, string) {
	if claim == nil {
		return runsv1alpha1.EffectClaimPhaseReserved, effectClaimReasonReserved, "EffectClaim reservation is active"
	}

	switch claim.Spec.CompletionStatus {
	case runsv1alpha1.EffectClaimCompletionStatusCompleted:
		return runsv1alpha1.EffectClaimPhaseCompleted, conditions.ReasonCompleted, "EffectClaim completed and will be retained until the owning StepRun is cleaned up"
	case runsv1alpha1.EffectClaimCompletionStatusReleased:
		return runsv1alpha1.EffectClaimPhaseReleased, effectClaimReasonReleased, "EffectClaim released for retry and will be retained until the owning StepRun is cleaned up"
	case runsv1alpha1.EffectClaimCompletionStatusAbandoned:
		return runsv1alpha1.EffectClaimPhaseAbandoned, effectClaimReasonAbandoned, "EffectClaim abandoned and will be retained until the owning StepRun is cleaned up"
	default:
		return runsv1alpha1.EffectClaimPhaseReserved, effectClaimReasonReserved, "EffectClaim reservation is active"
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *EffectClaimReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(opts).
		For(&runsv1alpha1.EffectClaim{}).
		Named("runs-effectclaim").
		Complete(r)
}
