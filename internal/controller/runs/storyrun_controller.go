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
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"go.opentelemetry.io/otel/attribute"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/observability"
	rec "github.com/bubustack/bobrapet/pkg/reconcile"
	"github.com/bubustack/bobrapet/pkg/refs"
	runslist "github.com/bubustack/bobrapet/pkg/runs/list"
	runsstatus "github.com/bubustack/bobrapet/pkg/runs/status"
	"github.com/bubustack/core/contracts"
	bootstrapruntime "github.com/bubustack/core/runtime/bootstrap"
	stagemeta "github.com/bubustack/core/runtime/stage"
)

// formatDuration formats a duration in a human-readable way
// e.g., "10m 13s" instead of "10m13.972016716s"
const (
	// StoryRunFinalizer is the finalizer for StoryRun resources
	StoryRunFinalizer = "storyrun.bubustack.io/finalizer"
	// DefaultMaxInlineInputsSize is the default maximum size for inline inputs (5 KB).
	// This provides a safe fallback if webhooks are disabled and no operator config is set.
	// It's a balance between preventing etcd bloat and allowing reasonable input sizes.
	DefaultMaxInlineInputsSize = 5 * 1024
	// storyRunSDKStopMessage matches the status message set by the SDK StopStoryRun helper
	// (../bubu-sdk-go/k8s/client.go:703-727) when it terminates a StoryRun.
	storyRunSDKStopMessage = "StoryRun finished via SDK"
)

// StoryRunReconciler reconciles a StoryRun object
type StoryRunReconciler struct {
	config.ControllerDependencies
	rbacManager   *RBACManager
	dagReconciler *DAGReconciler
	Recorder      record.EventRecorder
}

// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=create;get;watch;list;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=create;get;watch;list;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=create;get;watch;list;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams,verbs=get;list;watch

// Reconcile is the main entry point for StoryRun reconciliation.
//
// Behavior:
//   - Applies optional reconcile timeout from operator config.
//   - Guards against oversized inputs.
//   - Handles deletion if needed.
//   - Ensures finalizer and RBAC are configured.
//   - Routes to DAG reconciliation for non-terminal phases.
//
// Arguments:
//   - ctx context.Context: for API calls and cancellation.
//   - req ctrl.Request: the reconcile request.
//
// Returns:
//   - ctrl.Result: reconcile result with possible requeue.
//   - error: non-nil on failures.
func (r *StoryRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	ctx, span := observability.StartSpan(ctx, "StoryRunReconciler.Reconcile",
		attribute.String("namespace", req.Namespace),
		attribute.String("storyrun", req.Name),
	)
	defer span.End()

	log := logging.NewReconcileLogger(ctx, "storyrun").WithValues("storyrun", req.NamespacedName)
	startTime := time.Now()
	defer func() {
		if err != nil {
			span.RecordError(err)
		}
		requeue := res.RequeueAfter > 0
		span.SetAttributes(
			attribute.Bool("requeue", requeue),
			attribute.String("requeue_after", res.RequeueAfter.String()),
		)
		metrics.RecordControllerReconcile("storyrun", time.Since(startTime), err)
	}()

	timeout := time.Duration(0)
	if r.ConfigResolver != nil && r.ConfigResolver.GetOperatorConfig() != nil {
		timeout = r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	}
	ctx, cancel := rec.WithTimeout(ctx, timeout)
	defer cancel()

	var srun runsv1alpha1.StoryRun
	if err := r.Get(ctx, req.NamespacedName, &srun); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	span.SetAttributes(
		attribute.String("story", srun.Spec.StoryRef.Name),
		attribute.String("phase", string(srun.Status.Phase)),
	)

	if handled, err := r.guardOversizedInputs(ctx, &srun, log); handled || err != nil {
		return ctrl.Result{}, err
	}
	if handled, result, err := r.handleDeletionIfNeeded(ctx, &srun); handled {
		return result, err
	}
	if err := r.ensureFinalizer(ctx, &srun, log); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.rbacManager.Reconcile(ctx, &srun); err != nil {
		log.Error(err, "Failed to reconcile RBAC")
		return ctrl.Result{}, err
	}
	if srun.Status.Phase.IsTerminal() {
		story, err := r.getStoryOptional(ctx, &srun)
		if err != nil {
			return ctrl.Result{}, err
		}
		return r.handleTerminalStoryRun(ctx, &srun, story, log)
	}

	return r.reconcileAfterSetup(ctx, &srun, log)
}

// reconcileAfterSetup handles StoryRun reconciliation after setup is complete.
//
// Behavior:
//   - Fetches Story or waits if not found.
//   - Initializes phase if empty.
//   - Delegates to DAG reconciler.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun.
//   - log *logging.ControllerLogger: for logging.
//
// Returns:
//   - ctrl.Result: reconcile result.
//   - error: non-nil on failures.
func (r *StoryRunReconciler) reconcileAfterSetup(ctx context.Context, srun *runsv1alpha1.StoryRun, log *logging.ControllerLogger) (ctrl.Result, error) {
	var story *bubushv1alpha1.Story
	handled, result, s, err := r.getStoryOrWait(ctx, srun, log)
	if handled || err != nil {
		return result, err
	}
	story = s

	if handled, err := r.initializePhaseIfEmpty(ctx, srun); handled || err != nil {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	return r.dagReconciler.Reconcile(ctx, srun, story)
}

// guardOversizedInputs enforces inline-inputs size limit.
//
// Behavior:
//   - Checks spec.inputs size against configured maximum.
//   - Fails StoryRun if size exceeds limit.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun.
//   - log *logging.ControllerLogger: for logging.
//
// Returns:
//   - bool: true if handled (oversized or error).
//   - error: non-nil on status patch failure.
func (r *StoryRunReconciler) guardOversizedInputs(ctx context.Context, srun *runsv1alpha1.StoryRun, log *logging.ControllerLogger) (bool, error) {
	// Prefer StoryRun-specific knob from operator config; fall back to sane default
	maxBytes := r.ConfigResolver.GetOperatorConfig().Controller.StoryRun.MaxInlineInputsSize
	if maxBytes <= 0 {
		maxBytes = DefaultMaxInlineInputsSize
	}
	if srun.Spec.Inputs != nil && len(srun.Spec.Inputs.Raw) > maxBytes {
		if srun.Status.Phase != enums.PhaseFailed {
			log.Info("StoryRun spec.inputs is too large, failing run", "size", len(srun.Spec.Inputs.Raw), "maxSize", maxBytes)
			failMsg := fmt.Sprintf("spec.inputs size %d bytes exceeds maximum of %d", len(srun.Spec.Inputs.Raw), maxBytes)
			if err := r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, failMsg); err != nil {
				log.Error(err, "Failed to set StoryRun status to Failed due to oversized inputs")
				return true, err
			}
		}
		return true, nil
	}
	return false, nil
}

// handleDeletionIfNeeded short-circuits for deleting StoryRuns.
//
// Behavior:
//   - Delegates to reconcileDelete if deletion timestamp set.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun.
//
// Returns:
//   - bool: true if deletion handling occurred.
//   - ctrl.Result: reconcile result.
//   - error: non-nil on failures.
func (r *StoryRunReconciler) handleDeletionIfNeeded(ctx context.Context, srun *runsv1alpha1.StoryRun) (bool, ctrl.Result, error) {
	return rec.ShortCircuitDeletion(ctx, srun, func(ctx context.Context) (ctrl.Result, error) {
		return r.reconcileDelete(ctx, srun)
	})
}

// ensureFinalizer adds controller finalizer for cleanup logic.
//
// Behavior:
//   - Adds finalizer via merge patch if not present.
//   - Logs when finalizer is added.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun.
//   - log *logging.ControllerLogger: for logging.
//
// Returns:
//   - error: non-nil on patch failure.
func (r *StoryRunReconciler) ensureFinalizer(ctx context.Context, srun *runsv1alpha1.StoryRun, log *logging.ControllerLogger) error {
	added, err := kubeutil.EnsureFinalizer(ctx, r.Client, srun, StoryRunFinalizer)
	if err != nil {
		log.Error(err, "Failed to add finalizer")
		return err
	}
	if added {
		log.Info("Added StoryRun finalizer")
	}
	return nil
}

// nextRequeueDelay calculates jittered requeue delay from operator config.
//
// Behavior:
//   - Uses operator config for base/max delay.
//   - Falls back to 5s-30s if config unavailable.
//
// Arguments:
//   - None.
//
// Returns:
//   - time.Duration: the requeue delay with jitter.
func (r *StoryRunReconciler) nextRequeueDelay() time.Duration {
	const fallbackBase = 5 * time.Second
	const fallbackMax = 30 * time.Second

	cfg := r.ConfigResolver.GetOperatorConfig()
	if cfg == nil {
		return rec.JitteredRequeueDelay(0, 0, fallbackBase, fallbackMax)
	}
	return rec.JitteredRequeueDelay(cfg.Controller.RequeueBaseDelay, cfg.Controller.RequeueMaxDelay, fallbackBase, fallbackMax)
}

// getStoryOrWait fetches Story or waits if not found.
//
// Behavior:
//   - Returns Story immediately if found.
//   - Updates status and requeues if Story not found.
//   - Propagates other errors for retry.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun.
//   - log *logging.ControllerLogger: for logging.
//
// Returns:
//   - bool: true if handled (waiting or error).
//   - ctrl.Result: reconcile result.
//   - *bubushv1alpha1.Story: the fetched Story.
//   - error: non-nil on failures.
func (r *StoryRunReconciler) getStoryOrWait(ctx context.Context, srun *runsv1alpha1.StoryRun, log *logging.ControllerLogger) (bool, ctrl.Result, *bubushv1alpha1.Story, error) {
	story, err := r.getStoryForRun(ctx, srun)
	if err == nil {
		return false, ctrl.Result{}, story, nil
	}
	if errors.IsNotFound(err) {
		log.Info("Story not found for StoryRun, will requeue", "story", srun.Spec.StoryRef.Name)
		if r.Recorder != nil {
			r.Recorder.Event(srun, "Warning", conditions.ReasonStoryNotFound, fmt.Sprintf("Waiting for Story '%s'", srun.Spec.StoryRef.ToNamespacedName(srun).String()))
		}
		statusErr := kubeutil.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
			sr := obj.(*runsv1alpha1.StoryRun)
			if sr.Status.Phase == "" {
				sr.Status.Phase = enums.PhasePending
			}
			cm := conditions.NewConditionManager(sr.Generation)
			message := fmt.Sprintf("Waiting for Story '%s' to be created", srun.Spec.StoryRef.ToNamespacedName(srun).String())
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonStoryNotFound, message)
			sr.Status.Message = message
		})
		if statusErr != nil {
			log.Error(statusErr, "Failed to update StoryRun status while waiting for Story")
			return true, ctrl.Result{}, nil, statusErr
		}
		return true, ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil, nil
	}
	log.Error(err, "Failed to get Story for StoryRun")
	// Propagate the error so controller-runtime retries instead of marking the run failed on a transient API error.
	return true, ctrl.Result{}, nil, err
}

// initializePhaseIfEmpty sets initial Running phase if phase is empty.
//
// Behavior:
//   - Returns early if phase already set.
//   - Sets phase to Running with initial message.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun.
//
// Returns:
//   - bool: true if phase was initialized.
//   - error: non-nil on status patch failure.
func (r *StoryRunReconciler) initializePhaseIfEmpty(ctx context.Context, srun *runsv1alpha1.StoryRun) (bool, error) {
	if srun.Status.Phase != "" {
		return false, nil
	}
	if err := r.setStoryRunPhase(ctx, srun, enums.PhaseRunning, "Starting StoryRun execution"); err != nil {
		return true, err
	}
	return true, nil
}

// reconcileDelete handles StoryRun deletion and resource cleanup.
//
// Behavior:
//   - Cleans up dependent StepRuns.
//   - Removes finalizer when cleanup is complete.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun being deleted.
//
// Returns:
//   - ctrl.Result: with requeue if cleanup pending.
//   - error: non-nil on failures.
func (r *StoryRunReconciler) reconcileDelete(ctx context.Context, srun *runsv1alpha1.StoryRun) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun").WithValues("storyrun", srun.Name)
	log.Info("Reconciling deletion for StoryRun")

	// List all StepRuns for this StoryRun
	hasDependents, _, _, err := r.cleanupStoryRunDependents(ctx, srun, log)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Once all children are gone, remove the finalizer.
	if !hasDependents {
		if controllerutil.ContainsFinalizer(srun, StoryRunFinalizer) {
			mergePatch := client.MergeFrom(srun.DeepCopy())
			controllerutil.RemoveFinalizer(srun, StoryRunFinalizer)
			// Use Patch for atomicity, avoiding race conditions with other updaters.
			// This is safer than Update for modifying metadata like finalizers.
			if err := r.Patch(ctx, srun, mergePatch); err != nil {
				log.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
			log.Info("Removed finalizer")
		}
	} else {
		// If children still exist, requeue to check on them later.
		return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
	}

	return ctrl.Result{}, nil
}

// getStoryForRun fetches the Story referenced by the StoryRun.
//
// Behavior:
//   - Validates storyRef.name is present.
//   - Uses refs.LoadNamespacedReference for fetch.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun.
//
// Returns:
//   - *bubushv1alpha1.Story: the fetched Story.
//   - error: non-nil on missing ref or fetch failure.
func (r *StoryRunReconciler) getStoryForRun(ctx context.Context, srun *runsv1alpha1.StoryRun) (*bubushv1alpha1.Story, error) {
	if strings.TrimSpace(srun.Spec.StoryRef.Name) == "" {
		return nil, fmt.Errorf("storyrun %s missing spec.storyRef.name", srun.Name)
	}
	story, key, err := refs.LoadNamespacedReference(
		ctx,
		r.Client,
		srun,
		&srun.Spec.StoryRef,
		func() *bubushv1alpha1.Story { return &bubushv1alpha1.Story{} },
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("get Story %s: %w", key.String(), err)
	}
	return story, nil
}

// getStoryOptional fetches Story, returning nil if not found.
//
// Behavior:
//   - Delegates to getStoryForRun.
//   - Returns nil for NotFound errors.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun.
//
// Returns:
//   - *bubushv1alpha1.Story: the Story, or nil if not found.
//   - error: non-nil on fetch failures other than NotFound.
func (r *StoryRunReconciler) getStoryOptional(ctx context.Context, srun *runsv1alpha1.StoryRun) (*bubushv1alpha1.Story, error) {
	story, err := r.getStoryForRun(ctx, srun)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return story, nil
}

// setStoryRunPhase updates StoryRun phase via shared status helper.
//
// Behavior:
//   - Delegates to runsstatus.PatchStoryRunPhase.
//   - Keeps Ready condition and duration accounting consistent.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun.
//   - phase enums.Phase: the new phase.
//   - message string: phase message.
//
// Returns:
//   - error: non-nil on patch failure.
func (r *StoryRunReconciler) setStoryRunPhase(ctx context.Context, srun *runsv1alpha1.StoryRun, phase enums.Phase, message string) error {
	return runsstatus.PatchStoryRunPhase(ctx, r.Client, srun, phase, message)
}

// cleanupStoryRunDependents removes StepRuns and StoryRuns labeled with the
// parent StoryRun name and returns true while anything remains
// (internal/controller/runs/storyrun_controller.go:333-379).
// Behavior:
//   - Uses runslist.StepRunsByStoryRun (contracts.StoryRunLabelKey=<parent>) to
//     list StepRuns and deletes those without a DeletionTimestamp, ignoring
//     NotFound races (internal/controller/runs/storyrun_controller.go:339-354;
//     pkg/runs/list/step_runs.go:9-33).
//   - Lists StoryRuns via contracts.ParentStoryRunLabel, skips the parent UID,
//     and deletes children with background propagation so nested workloads GC
//     after the StoryRun retires (internal/controller/runs/storyrun_controller.go:357-377).
//
// Arguments:
//   - ctx context.Context: used for all client operations.
//   - srun *runsv1alpha1.StoryRun: parent StoryRun providing namespace/name.
//   - log *logging.ControllerLogger: records list/delete outcomes.
//
// Returns:
//   - bool: true when dependents still exist (callers should requeue).
//   - int: count of StepRuns deleted in this sweep (for logging/metrics).
//   - int: count of child StoryRuns deleted in this sweep.
//   - error: list/delete failures.
//
// Side Effects:
//   - Deletes StepRuns and StoryRuns carrying the parent labels used by
//     StepExecutor, cleanupRunScopedRuntime, and the Stage-to-stage guard.
//
// Notes / Gotchas:
//   - StepRun deletes now also use background propagation to keep parity with
//     child StoryRuns so Jobs/Pods tied to either resource GC even when owner
//     references drift (internal/controller/runs/storyrun_controller.go:343-377).
func (r *StoryRunReconciler) cleanupStoryRunDependents(ctx context.Context, srun *runsv1alpha1.StoryRun, log *logging.ControllerLogger) (bool, int, int, error) {
	if srun == nil {
		return false, 0, 0, nil
	}
	meta := stagemeta.StoryRunMetadata(srun.Name, srun.Namespace)
	contract := bootstrapruntime.NewContractLogger(log.Logr(), "storyrun-cleanup").WithStage(meta)
	contract.Start("cleanup")

	stepSummary, err := r.deleteChildStepRuns(ctx, srun, meta, log, contract)
	if err != nil {
		return true, stepSummary.deleted, 0, err
	}

	storySummary, err := r.deleteChildStoryRuns(ctx, srun, meta, log, contract)
	if err != nil {
		return true, stepSummary.deleted, storySummary.deleted, err
	}

	r.recordDependentMetrics(srun, meta, log, stepSummary.deleted, storySummary.deleted)
	dependentFound := stepSummary.remaining || storySummary.remaining
	contract.Success("cleanup",
		"stepRuns", stepSummary.deleted,
		"childStoryRuns", storySummary.deleted,
		"dependentsRemaining", dependentFound,
	)
	return dependentFound, stepSummary.deleted, storySummary.deleted, nil
}

type cleanupSummary struct {
	deleted   int
	remaining bool
}

func (r *StoryRunReconciler) deleteChildStepRuns(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	meta stagemeta.Metadata,
	log *logging.ControllerLogger,
	contract bootstrapruntime.ContractLogger,
) (cleanupSummary, error) {
	var stepRunList runsv1alpha1.StepRunList
	if err := runslist.StepRunsByStoryRun(ctx, r.Client, srun.Namespace, srun.Name, nil, &stepRunList); err != nil {
		meta.Error(log.Logr(), err, "Failed to list StepRuns for cleanup")
		contract.Failure("cleanup", err)
		return cleanupSummary{remaining: true}, err
	}

	summary := cleanupSummary{remaining: len(stepRunList.Items) > 0}
	for i := range stepRunList.Items {
		sr := &stepRunList.Items[i]
		if sr.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, sr, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
				meta.Error(log.Logr(), err, "Failed to delete StepRun during cleanup", "stepRun", sr.Name)
				contract.Failure("cleanup", err, "phase", "delete-step", "stepRun", sr.Name)
				return summary, err
			}
			meta.Info(log.Logr(), "Deleted child StepRun", "stepRun", sr.Name)
			summary.deleted++
		}
	}
	return summary, nil
}

func (r *StoryRunReconciler) deleteChildStoryRuns(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	meta stagemeta.Metadata,
	log *logging.ControllerLogger,
	contract bootstrapruntime.ContractLogger,
) (cleanupSummary, error) {
	var childStoryRuns runsv1alpha1.StoryRunList
	if err := kubeutil.ListByLabels(ctx, r.Client, srun.Namespace, map[string]string{contracts.ParentStoryRunLabel: srun.Name}, &childStoryRuns); err != nil {
		meta.Error(log.Logr(), err, "Failed to list child StoryRuns for cleanup")
		contract.Failure("cleanup", err)
		return cleanupSummary{remaining: true}, err
	}

	summary := cleanupSummary{remaining: len(childStoryRuns.Items) > 0}
	for i := range childStoryRuns.Items {
		child := &childStoryRuns.Items[i]
		if child.UID == srun.UID || !child.DeletionTimestamp.IsZero() {
			continue
		}
		if err := r.Delete(ctx, child, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
			meta.Error(log.Logr(), err, "Failed to delete child StoryRun during cleanup", "childStoryRun", child.Name)
			contract.Failure("cleanup", err, "phase", "delete-storyrun", "childStoryRun", child.Name)
			return summary, err
		}
		meta.Info(log.Logr(), "Deleted child StoryRun", "childStoryRun", child.Name)
		summary.deleted++
	}
	return summary, nil
}

func (r *StoryRunReconciler) recordDependentMetrics(srun *runsv1alpha1.StoryRun, meta stagemeta.Metadata, log *logging.ControllerLogger, deletedStepRuns, deletedChildStoryRuns int) {
	if deletedStepRuns > 0 {
		metrics.RecordStoryRunDependentCleanup(srun.Namespace, srun.Name, "StepRun", deletedStepRuns)
	}
	if deletedChildStoryRuns > 0 {
		metrics.RecordStoryRunDependentCleanup(srun.Namespace, srun.Name, "StoryRun", deletedChildStoryRuns)
	}
	if deletedStepRuns > 0 || deletedChildStoryRuns > 0 {
		meta.Info(log.Logr(), "Deleted StoryRun dependents summary",
			"stepRuns", deletedStepRuns,
			"childStoryRuns", deletedChildStoryRuns)
	}
}

// handleTerminalStoryRun enforces post-completion cleanup by waiting for the child TTL,
// deleting dependent StepRuns/StoryRuns, stamping ChildrenCleanedAt, logging the cleanup
// summary, and deleting the StoryRun once the retention window expires
// (internal/controller/runs/storyrun_controller.go:375-566).
// Behavior:
//   - Returns early (with a requeue) until Status.FinishedAt is set so upstream status patches
//     finish before TTL math runs (lines 381-396).
//   - Phase 1 waits for FinishedAt+childTTL, repeatedly calling cleanupStoryRunDependents and
//     marking ChildrenCleanedAt when everything is gone (lines 401-449).
//   - Phase 2 interprets retentionSeconds (<0 immediate delete, 0 keep forever, >0 wait) and issues
//     a background delete after the retention window (lines 432-479).
//   - Records StoryRun TTL/retention gauges and includes finishedViaSDK in structured logs so
//     operators can see whether the SDK triggered terminal cleanup (lines 386-415).
//   - Emits a structured log with the number of dependents removed and the ChildrenCleanedAt
//     timestamp as soon as cleanup completes so retention enforcement is auditable (lines 452-462).
//
// Inputs:
//   - ctx context.Context propagated into cleanup, status patches, and deletes.
//   - srun/story/log describe the StoryRun, the parent Story policy, and the logger.
//
// Returns:
//   - ctrl.Result indicating the next requeue or stay steady; error bubbles cleanup/delete failures.
//
// Side Effects:
//   - Deletes child StepRuns/StoryRuns and the StoryRun CR itself; patches Status.ChildrenCleanedAt.
//
// Notes:
//   - Honors SDK-issued FinishedAt timestamps (`../bubu-sdk-go/k8s/client.go:703-727`), so keep
//     retention math aligned with client-side StopStoryRun.
func (r *StoryRunReconciler) handleTerminalStoryRun(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	story *bubushv1alpha1.Story,
	log *logging.ControllerLogger,
) (ctrl.Result, error) {
	if srun == nil {
		return ctrl.Result{}, nil
	}

	childTTL, retentionSeconds := r.resolveRetentionSettings(story)
	finishedViaSDK := srun.Status.Message == storyRunSDKStopMessage
	log = log.WithValues(
		"childTTL", childTTL,
		"storyRunRetention", retentionSeconds,
		"storyrun", srun.Name,
		"finishedViaSDK", finishedViaSDK,
	)
	r.recordStoryRunWindows(srun, childTTL, retentionSeconds)

	finishedAt := srun.Status.FinishedAt
	if finishedAt == nil {
		log.Info("StoryRun finished but FinishedAt not recorded yet; waiting before enforcing TTL")
		return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
	}
	childrenCleanedAt := srun.Status.ChildrenCleanedAt
	if finishedViaSDK && childrenCleanedAt == nil {
		log.Info("StoryRun finished via SDK; enforcing TTL before retention cleanup")
	}

	if res, waiting := r.waitForChildTTLPhase(finishedAt, childTTL, log); waiting {
		return res, nil
	}

	cleanedAt, requeueResult, err := r.ensureChildCleanup(ctx, srun, log)
	if err != nil {
		return ctrl.Result{}, err
	}
	if requeueResult != nil {
		return *requeueResult, nil
	}
	if cleanedAt == nil {
		cleanedNow := metav1.Now()
		cleanedAt = &cleanedNow
	}

	return r.enforceRetentionPhase(ctx, srun, log, finishedAt, cleanedAt, retentionSeconds)
}

func (r *StoryRunReconciler) recordStoryRunWindows(srun *runsv1alpha1.StoryRun, childTTL, retentionSeconds int32) {
	storyName := srun.Spec.StoryRef.Name
	metrics.RecordStoryRunWindowSeconds(srun.Namespace, storyName, metrics.WindowChildTTL, float64(childTTL))
	metrics.RecordStoryRunWindowSeconds(srun.Namespace, storyName, metrics.WindowRetention, float64(retentionSeconds))
}

func (r *StoryRunReconciler) waitForChildTTLPhase(finishedAt *metav1.Time, childTTL int32, log *logging.ControllerLogger) (ctrl.Result, bool) {
	if childTTL <= 0 {
		return ctrl.Result{}, false
	}
	childExpiresAt := finishedAt.Add(time.Duration(childTTL) * time.Second)
	now := time.Now()
	if now.Before(childExpiresAt) {
		log.Info("StoryRun within child TTL window; keeping children",
			"finishedAt", finishedAt.Time,
			"childExpiresAt", childExpiresAt,
			"remaining", childExpiresAt.Sub(now))
		return ctrl.Result{RequeueAfter: childExpiresAt.Sub(now)}, true
	}
	return ctrl.Result{}, false
}

func (r *StoryRunReconciler) ensureChildCleanup(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	log *logging.ControllerLogger,
) (*metav1.Time, *ctrl.Result, error) {
	dependentRemaining, deletedStepRuns, deletedChildStoryRuns, err := r.cleanupStoryRunDependents(ctx, srun, log)
	if err != nil {
		return nil, nil, err
	}
	if dependentRemaining {
		res := ctrl.Result{RequeueAfter: r.nextRequeueDelay()}
		return nil, &res, nil
	}

	if srun.Status.ChildrenCleanedAt != nil {
		return srun.Status.ChildrenCleanedAt, nil, nil
	}

	if err := r.markChildrenCleaned(ctx, srun); err != nil {
		log.Error(err, "Failed to mark children cleaned timestamp; requeuing before retention")
		res := ctrl.Result{RequeueAfter: r.nextRequeueDelay()}
		return nil, &res, nil
	}
	cleanedAt := srun.Status.ChildrenCleanedAt
	if cleanedAt == nil {
		now := metav1.Now()
		cleanedAt = &now
	}
	log.Info("Children cleaned up successfully; retention window starting",
		"childrenCleanedAt", cleanedAt.Time,
		"deletedStepRuns", deletedStepRuns,
		"deletedChildStoryRuns", deletedChildStoryRuns,
	)
	return cleanedAt, nil, nil
}

func (r *StoryRunReconciler) enforceRetentionPhase(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	log *logging.ControllerLogger,
	finishedAt *metav1.Time,
	childrenCleanedAt *metav1.Time,
	retentionSeconds int32,
) (ctrl.Result, error) {
	switch {
	case retentionSeconds == 0:
		log.Info("StoryRun retention set to forever; StoryRun will remain in cluster")
		return ctrl.Result{}, nil
	case retentionSeconds < 0:
		log.Info("StoryRun retention set to immediate deletion")
		return ctrl.Result{}, r.deleteStoryRun(ctx, srun)
	default:
		return r.waitForRetentionExpiry(ctx, srun, log, finishedAt, childrenCleanedAt, retentionSeconds)
	}
}

func (r *StoryRunReconciler) deleteStoryRun(ctx context.Context, srun *runsv1alpha1.StoryRun) error {
	if srun.DeletionTimestamp.IsZero() {
		propagation := metav1.DeletePropagationBackground
		if err := r.Delete(ctx, srun, &client.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (r *StoryRunReconciler) waitForRetentionExpiry(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	log *logging.ControllerLogger,
	finishedAt *metav1.Time,
	childrenCleanedAt *metav1.Time,
	retentionSeconds int32,
) (ctrl.Result, error) {
	retentionExpiresAt := finishedAt.Add(time.Duration(retentionSeconds) * time.Second)
	now := time.Now()
	if now.Before(retentionExpiresAt) {
		remaining := retentionExpiresAt.Sub(now)
		log.Info("StoryRun within retention window; will be preserved",
			"finishedAt", finishedAt.Time,
			"childrenCleanedAt", childrenCleanedAt.Time,
			"retentionExpiresAt", retentionExpiresAt,
			"remaining", remaining)
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	log.Info("StoryRun retention period expired; deleting to free etcd space",
		"finishedAt", finishedAt.Time,
		"childrenCleanedAt", childrenCleanedAt.Time,
		"retentionExpiredAt", retentionExpiresAt,
		"age", now.Sub(finishedAt.Time))
	return ctrl.Result{}, r.deleteStoryRun(ctx, srun)
}

// resolveRetentionSettings resolves child TTL and StoryRun retention.
//
// Behavior:
//   - Derives settings from Story execution policy or operator defaults.
//   - Streaming: keeps children forever unless TTL override exists.
//   - Batch: defaults to 1 hour TTL and 24 hours retention.
//
// Arguments:
//   - story *bubushv1alpha1.Story: the Story for policy lookup.
//
// Returns:
//   - int32: child StepRun TTL seconds.
//   - int32: StoryRun retention seconds.
func (r *StoryRunReconciler) resolveRetentionSettings(story *bubushv1alpha1.Story) (int32, int32) {
	cfg := r.ConfigResolver.GetOperatorConfig()
	isStreaming := story != nil && story.Spec.Pattern == enums.StreamingPattern

	childTTL := resolveStoryChildTTL(story, isStreaming)
	childTTL = applyChildTTLDefaults(childTTL, isStreaming, cfg)

	retentionSeconds := resolveStoryRetention(story)
	retentionSeconds = applyRetentionDefaults(retentionSeconds, cfg)

	return childTTL, retentionSeconds
}

func resolveStoryChildTTL(story *bubushv1alpha1.Story, isStreaming bool) int32 {
	if story == nil || story.Spec.Policy == nil || story.Spec.Policy.Execution == nil {
		return 0
	}
	exec := story.Spec.Policy.Execution
	switch {
	case isStreaming && exec.Realtime != nil && exec.Realtime.TTLSecondsAfterFinished != nil:
		return *exec.Realtime.TTLSecondsAfterFinished
	case exec.Job != nil && exec.Job.TTLSecondsAfterFinished != nil:
		return *exec.Job.TTLSecondsAfterFinished
	default:
		return 0
	}
}

func resolveStoryRetention(story *bubushv1alpha1.Story) int32 {
	if story == nil || story.Spec.Policy == nil || story.Spec.Policy.Execution == nil {
		return 0
	}
	exec := story.Spec.Policy.Execution
	if exec.Job != nil && exec.Job.StoryRunRetentionSeconds != nil {
		return *exec.Job.StoryRunRetentionSeconds
	}
	return 0
}

func applyChildTTLDefaults(childTTL int32, isStreaming bool, cfg *config.OperatorConfig) int32 {
	if childTTL != 0 {
		return childTTL
	}
	if cfg != nil {
		if isStreaming && cfg.Controller.StreamingTTLSecondsAfterFinished != 0 {
			return cfg.Controller.StreamingTTLSecondsAfterFinished
		}
		if !isStreaming && cfg.Controller.TTLSecondsAfterFinished != 0 {
			return cfg.Controller.TTLSecondsAfterFinished
		}
	}
	if !isStreaming {
		return 3600 // Default: 1 hour for batch stories
	}
	return 0
}

func applyRetentionDefaults(retentionSeconds int32, cfg *config.OperatorConfig) int32 {
	if retentionSeconds != 0 {
		return retentionSeconds
	}
	if cfg != nil && cfg.Controller.StoryRunRetentionSeconds > 0 {
		return cfg.Controller.StoryRunRetentionSeconds
	}
	return 86400 // Default: 24 hours
}

// markChildrenCleaned records when children were cleaned up in status.
//
// Behavior:
//   - Updates Status.ChildrenCleanedAt to current time.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StoryRun: the StoryRun.
//
// Returns:
//   - error: non-nil on status patch failure.
func (r *StoryRunReconciler) markChildrenCleaned(ctx context.Context, srun *runsv1alpha1.StoryRun) error {
	return kubeutil.RetryableStatusUpdate(ctx, r.Client, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		now := metav1.Now()
		sr.Status.ChildrenCleanedAt = &now
	})
}

// SetupWithManager configures the controller with watches and predicates.
//
// Behavior:
//   - Initializes RBAC manager, step executor, and DAG reconciler.
//   - Watches StoryRun and owned StepRun resources.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//   - opts controller.Options: controller options.
//
// Returns:
//   - error: non-nil on setup failure.
func (r *StoryRunReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	r.Recorder = mgr.GetEventRecorderFor("storyrun-controller")
	r.rbacManager = NewRBACManager(mgr.GetClient(), mgr.GetScheme())
	r.rbacManager.Recorder = r.Recorder
	stepExecutor := NewStepExecutor(mgr.GetClient(), mgr.GetScheme(), &r.CELEvaluator, r.ConfigResolver, r.Recorder)
	r.dagReconciler = NewDAGReconciler(DAGReconcilerDeps{
		Client:         mgr.GetClient(),
		CELEvaluator:   &r.CELEvaluator,
		StepExecutor:   stepExecutor,
		ConfigResolver: r.ConfigResolver,
	})

	generationPredicate := predicate.GenerationChangedPredicate{}
	stepRunPredicate := predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetResourceVersion() != e.ObjectNew.GetResourceVersion()
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StoryRun{}, builder.WithPredicates(generationPredicate)).
		WithOptions(opts).
		Owns(&runsv1alpha1.StepRun{}, builder.WithPredicates(stepRunPredicate)).
		Complete(r)
}
