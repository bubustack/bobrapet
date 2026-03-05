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

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"

	rec "github.com/bubustack/bobrapet/pkg/reconcile"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// maxStepRunsToMarkPerEngramReconcile is a safety valve to avoid patching an unbounded
	// number of StepRuns in a single reconcile. This prevents controller timeouts and
	// reduces the chance of "mark child → fail parent status" partial progress problems.
	maxStepRunsToMarkPerEngramReconcile = 50

	// requeueAfterEngramBackfill is a short delay used when we detect there are more
	// StepRuns to mark than we processed in one reconcile.
	requeueAfterEngramBackfill = 750 * time.Millisecond

	// reasonTemplateRefInvalid is used when spec.templateRef is invalid (missing/empty).
	reasonTemplateRefInvalid = "TemplateRefInvalid"
)

var errEngramTemplateRefNameRequired = fmt.Errorf("spec.templateRef.name is required")

// EngramReconciler reconciles Engram resources.
//
// Responsibilities:
//   - Validate referenced EngramTemplate existence/resolution.
//   - Publish validation results, usage counts, and trigger statistics into Engram status.
//   - Emit warning events when template resolution fails (only on meaningful condition transitions).
//
// Anti-storm strategy:
//   - Engram uses GenerationChangedPredicate so status updates do not requeue itself.
//   - Story watch is generation-only (spec changes) so Story status updates don't fan-out storms.
//   - StepRun watch uses a predicate that ignores annotation-only updates, preventing loops
//     caused by trigger-token annotation writes performed during reconciliation.
//
// Observability:
//   - Structured logs enriched with name/namespace.
//   - Reconcile timing and errors recorded via metrics.
type EngramReconciler struct {
	config.ControllerDependencies
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=bubustack.io,resources=engrams,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams/finalizers,verbs=update
// +kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=engramtemplates,verbs=get;list;watch

// Reconcile validates the Engram's referenced EngramTemplate, computes Story usage and
// StepRun trigger statistics, and patches Engram.Status when changes are detected.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.0/pkg/reconcile
//
// Behavior:
//   - Validates the referenced EngramTemplate exists and is resolvable.
//   - Computes Story usage count via field index.
//   - Computes trigger count by marking StepRuns with annotation tokens.
//   - Patches Engram.Status with validation results, usage, and trigger counts.
//   - Emits warning events only when conditions meaningfully change.
//
// Arguments:
//   - ctx context.Context: controller context with optional timeout.
//   - req ctrl.Request: the Engram's namespace and name.
//
// Returns:
//   - ctrl.Result{}: empty on success.
//   - ctrl.Result{RequeueAfter: ...}: when trigger backfill is incomplete.
//   - error: for transient errors to activate controller-runtime backoff.
//
// Side Effects:
//   - Patches Engram.Status (ObservedGeneration, ValidationStatus/Errors, UsageCount, Triggers, Conditions).
//   - Patches StepRun metadata.annotations to add trigger-counting tokens.
func (r *EngramReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	log := logging.NewReconcileLogger(ctx, "engram").
		WithValues("name", req.Name, "namespace", req.Namespace)

	// Apply timeout only when configured. A zero timeout must NOT cancel immediately.
	timeout := time.Duration(0)
	if r.ConfigResolver != nil && r.ConfigResolver.GetOperatorConfig() != nil {
		timeout = r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	}
	ctx, finish := rec.StartControllerReconcile(ctx, "engram", timeout)
	defer func() { finish(err) }()
	if timeout > 0 {
		log.V(1).Info("Applying reconcile timeout", "timeout", timeout.String())
	}

	var engram bubushv1alpha1.Engram
	if getErr := r.Get(ctx, req.NamespacedName, &engram); getErr != nil {
		return ctrl.Result{}, client.IgnoreNotFound(getErr)
	}

	// Snapshot prior condition to avoid event spam.
	prevCondition := rec.SnapshotCondition(engram.Status.Conditions, conditions.ConditionTemplateResolved)

	// Validate template reference using shared helper.
	templateName := strings.TrimSpace(engram.Spec.TemplateRef.Name)
	_, validationResult := rec.ValidateTemplateRef(
		ctx,
		r.Client,
		templateName,
		"EngramTemplate",
		func() client.Object { return &catalogv1alpha1.EngramTemplate{} },
		client.ObjectKey{Name: templateName},
		errEngramTemplateRefNameRequired,
		reasonTemplateRefInvalid,
	)

	usageCount, usageErr := r.countEngramUsage(ctx, &engram)
	if usageErr != nil {
		log.Error(usageErr, "Failed to compute Engram usage count")
		return ctrl.Result{}, fmt.Errorf("count engram usage: %w", usageErr)
	}

	triggerCount, triggersPending, triggerErr := r.countEngramTriggersBounded(ctx, &engram, maxStepRunsToMarkPerEngramReconcile)
	if triggerErr != nil {
		log.Error(triggerErr, "Failed to compute Engram trigger count")
		return ctrl.Result{}, fmt.Errorf("count engram triggers: %w", triggerErr)
	}

	// Build desired status and patch only if changed.
	desiredStatus := engram.Status
	desiredStatus.ObservedGeneration = engram.Generation
	desiredStatus.ValidationStatus = validationResult.ValidationStatus
	desiredStatus.ValidationErrors = validationResult.ValidationErrors
	desiredStatus.UsageCount = usageCount
	desiredStatus.Triggers = triggerCount
	desiredStatus.Conditions = rec.ApplyTemplateCondition(engram.Status.Conditions, engram.Generation, validationResult)

	changed, statusErr := rec.PatchStatusIfChanged(ctx, r.Client, &engram, func(e *bubushv1alpha1.Engram) {
		e.Status = desiredStatus
	})
	if statusErr != nil {
		if apierrors.IsNotFound(statusErr) {
			return ctrl.Result{}, nil
		}
		log.Error(statusErr, "Failed to patch Engram status")
		return ctrl.Result{}, statusErr
	}
	if changed {
		engram.Status = desiredStatus
	}
	if !changed && r.statusFieldsMissing(ctx, &engram) {
		if err := kubeutil.RetryableStatusPatch(ctx, r.Client, &engram, func(obj client.Object) {
			e := obj.(*bubushv1alpha1.Engram)
			e.Status = desiredStatus
		}); err != nil {
			log.Error(err, "Failed to patch Engram status defaults")
			return ctrl.Result{}, err
		}
	}

	// Emit validation event only on meaningful condition transitions.
	rec.EmitValidationEvent(r.Recorder, &engram, validationResult, prevCondition)

	// Trigger backfill continues until we finish marking all StepRuns.
	if triggersPending {
		return ctrl.Result{RequeueAfter: requeueAfterEngramBackfill}, nil
	}

	// Requeue semantics for template validation:
	// - NotFound / invalid spec: no requeue (watches/spec fix will retrigger).
	// - Transient errors: return error to backoff.
	if !validationResult.IsValid() {
		if !validationResult.IsTransient {
			log.V(1).Info("Template validation failed; not requeueing (watch/spec fix will retrigger)",
				"reason", validationResult.ConditionReason)
			return ctrl.Result{}, nil
		}
		log.Error(validationResult.OriginalError, "Template resolution error; returning error for backoff")
		return ctrl.Result{}, validationResult.OriginalError
	}

	return ctrl.Result{}, nil
}

// SetupWithManager wires the controller watches and predicates.
//
// Behavior:
//   - Registers the controller for Engram resources with GenerationChangedPredicate.
//   - Watches EngramTemplates to requeue Engrams by indexed spec.templateRef.name.
//   - Watches Stories to requeue Engrams referenced by Story steps (deduped).
//   - Watches StepRuns with a predicate that ignores annotation-only updates.
//   - Uses generation-only predicates to prevent reconciliation storms.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//   - opts controller.Options: controller configuration (concurrency, etc.).
//
// Returns:
//   - error: nil on success, or controller setup error.
//
// Side Effects:
//   - Registers the event recorder for engram-controller.
func (r *EngramReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	r.Recorder = mgr.GetEventRecorderFor("engram-controller")

	mapTemplateToEngrams := func(ctx context.Context, obj client.Object) []reconcile.Request {
		var engrams bubushv1alpha1.EngramList
		reqs, err := refs.EnqueueByField(ctx, r.Client, &engrams, "spec.templateRef.name", obj.GetName())
		if err != nil {
			log := logging.NewControllerLogger(ctx, "engram-mapper")
			log.Error(err, "failed to list engrams for engramtemplate watch", "engramtemplate", obj.GetName())
			metrics.RecordControllerMapperFailure("engram", "EngramTemplate")
			return nil
		}
		return reqs
	}

	mapStepRunToEngram := func(ctx context.Context, obj client.Object) []reconcile.Request {
		stepRun, ok := obj.(*runsv1alpha1.StepRun)
		if !ok || stepRun.Spec.EngramRef == nil || stepRun.Spec.EngramRef.Name == "" {
			return nil
		}
		namespace := refs.ResolveNamespace(stepRun, &stepRun.Spec.EngramRef.ObjectReference)
		return []reconcile.Request{{
			NamespacedName: types.NamespacedName{Name: stepRun.Spec.EngramRef.Name, Namespace: namespace},
		}}
	}

	mapStoryToEngrams := func(ctx context.Context, obj client.Object) []reconcile.Request {
		story, ok := obj.(*bubushv1alpha1.Story)
		if !ok || len(story.Spec.Steps) == 0 {
			return nil
		}

		dedup := make(map[types.NamespacedName]struct{})
		for i := range story.Spec.Steps {
			step := &story.Spec.Steps[i]
			if step.Ref == nil || step.Ref.Name == "" {
				continue
			}
			ns := refs.ResolveNamespace(story, &step.Ref.ObjectReference)
			dedup[types.NamespacedName{Name: step.Ref.Name, Namespace: ns}] = struct{}{}
		}
		if len(dedup) == 0 {
			return nil
		}

		reqs := make([]reconcile.Request, 0, len(dedup))
		for nn := range dedup {
			reqs = append(reqs, reconcile.Request{NamespacedName: nn})
		}
		return reqs
	}

	genOnly := predicate.GenerationChangedPredicate{}

	return ctrl.NewControllerManagedBy(mgr).
		For(&bubushv1alpha1.Engram{}, builder.WithPredicates(genOnly)).
		Watches(&catalogv1alpha1.EngramTemplate{}, handler.EnqueueRequestsFromMapFunc(mapTemplateToEngrams), builder.WithPredicates(genOnly)).
		Watches(&bubushv1alpha1.Story{}, handler.EnqueueRequestsFromMapFunc(mapStoryToEngrams), builder.WithPredicates(genOnly)).
		Watches(&runsv1alpha1.StepRun{},
			handler.EnqueueRequestsFromMapFunc(mapStepRunToEngram),
			builder.WithPredicates(rec.StepRunEngramTriggerRelevantUpdates()),
		).
		WithOptions(opts).
		Complete(r)
}

// countEngramUsage counts how many Stories reference this Engram via step refs.
//
// Behavior:
//   - Uses the contracts.IndexStoryStepEngramRefs field index for efficient lookup.
//   - Builds the index key using refs.NamespacedKey(namespace, name).
//
// Arguments:
//   - ctx context.Context: propagated to the index lookup.
//   - engram *bubushv1alpha1.Engram: the Engram to count usage for.
//
// Returns:
//   - int32: number of Stories referencing the Engram.
//   - error: on list failures.
func (r *EngramReconciler) countEngramUsage(ctx context.Context, engram *bubushv1alpha1.Engram) (int32, error) {
	var stories bubushv1alpha1.StoryList
	key := refs.NamespacedKey(engram.Namespace, engram.Name)
	count, err := refs.CountByField(ctx, r.Client, &stories, contracts.IndexStoryStepEngramRefs, key)
	if err != nil {
		return 0, err
	}
	return int32(count), nil
}

// countEngramTriggersBounded updates trigger counters by marking StepRuns with a
// "counted" annotation token and incrementing Engram trigger totals only for newly
// marked StepRuns.
//
// Behavior:
//   - Lists StepRuns using the contracts.IndexStepRunEngramRef field index.
//   - Marks up to maxMarks StepRuns with the engram trigger token per reconcile.
//   - Increments total for each newly marked StepRun.
//   - Tolates NotFound errors for StepRuns that were deleted during processing.
//   - Sets pending=true when more StepRuns remain to be marked.
//
// Arguments:
//   - ctx context.Context: propagated to List and Patch operations.
//   - engram *bubushv1alpha1.Engram: the Engram whose StepRuns are inspected.
//   - maxMarks int: maximum number of StepRuns to patch per reconcile.
//
// Returns:
//   - total int64: the computed trigger total to persist into Engram.Status.Triggers.
//   - pending bool: true when additional StepRuns remain (caller should RequeueAfter).
//   - err error: on list/patch failures (NotFound is tolerated).
func (r *EngramReconciler) countEngramTriggersBounded(ctx context.Context, engram *bubushv1alpha1.Engram, maxMarks int) (total int64, pending bool, err error) {
	var stepRuns runsv1alpha1.StepRunList
	indexKey := refs.NamespacedKey(engram.Namespace, engram.Name)

	if err := r.List(ctx, &stepRuns, client.MatchingFields{contracts.IndexStepRunEngramRef: indexKey}); err != nil {
		return 0, false, err
	}

	total = engram.Status.Triggers
	markedThisRound := 0
	log := logging.NewReconcileLogger(ctx, "engram-trigger-backfill").
		WithValues("engram", engram.Name, "namespace", engram.Namespace)

	for i := range stepRuns.Items {
		if markedThisRound >= maxMarks {
			pending = true
			break
		}

		run := &stepRuns.Items[i]
		added, annErr := EnsureStepRunTriggerTokens(ctx, r.Client, types.NamespacedName{Namespace: run.Namespace, Name: run.Name}, contracts.StepRunTriggerTokenEngram)
		if annErr != nil {
			if apierrors.IsNotFound(annErr) {
				continue
			}
			return total, pending, fmt.Errorf("annotate StepRun %s for engram trigger tracking: %w", run.Name, annErr)
		}

		if len(added) > 0 {
			log.V(1).Info("Marked StepRun trigger token", "steprun", run.Name)
			total++
			markedThisRound++
		}
	}

	// If we hit maxMarks and there are still items left, request more backfill.
	if markedThisRound >= maxMarks && len(stepRuns.Items) > 0 {
		pending = true
	}

	log.V(1).Info("Trigger backfill iteration complete", "marked", markedThisRound, "pending", pending)
	metrics.RecordTriggerBackfill("engram", engram.Namespace, engram.Name, markedThisRound, pending)

	return total, pending, nil
}

func (r *EngramReconciler) statusFieldsMissing(ctx context.Context, engram *bubushv1alpha1.Engram) bool {
	if engram == nil {
		return false
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	var raw unstructured.Unstructured
	raw.SetGroupVersionKind(bubushv1alpha1.GroupVersion.WithKind("Engram"))
	if err := reader.Get(ctx, client.ObjectKeyFromObject(engram), &raw); err != nil {
		return false
	}
	_, hasUsage, _ := unstructured.NestedFieldNoCopy(raw.Object, "status", "usageCount")
	_, hasTriggers, _ := unstructured.NestedFieldNoCopy(raw.Object, "status", "triggers")
	return !hasUsage || !hasTriggers
}
