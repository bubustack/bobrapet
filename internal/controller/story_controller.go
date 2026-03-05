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
	"encoding/json"
	goerrors "errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"

	rec "github.com/bubustack/bobrapet/pkg/reconcile"
	"github.com/bubustack/bobrapet/pkg/validation"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// StoryFinalizer is the name of the finalizer used by the Story controller.
	StoryFinalizer = "story.bubustack.io/finalizer"
)

// Index field constants for Story controller lookups.
// These reference the centralized contracts package for consistency with SetupIndexers.
const (
	storyStepEngramIndexField = contracts.IndexStoryStepEngramRefs
	storyStepStoryIndexField  = contracts.IndexStoryStepStoryRefs
	storyRunStoryIndexField   = contracts.IndexStoryRunStoryRefKey
	impulseStoryNameIndex     = contracts.IndexImpulseStoryRef
)

// Safety valve: never try to mark unlimited runs in a single Story reconcile.
// This prevents reconcile storms when a Story has many StoryRuns.
const (
	maxStoryRunsToMarkPerReconcile = 50
	requeueAfterBackfill           = 750 * time.Millisecond
)

type executeStoryWith struct {
	StoryRef refs.StoryReference `json:"storyRef"`
}

// StoryReconciler reconciles a Story object.
type StoryReconciler struct {
	config.ControllerDependencies
	Recorder record.EventRecorder

	// Dirty flags must be "peek + clear after success" to prevent losing recount requests.
	usageDirty   rec.Set
	triggerDirty rec.Set
}

type storyPipelineState struct {
	validationErr error
}

// markUsageDirty flags the provided Story key so the next Story reconcile
// recomputes usage counts even if ObservedGeneration has not changed
// (internal/controller/story_controller.go:78-114,665-690).
//
// Behavior:
//   - Stores the namespaced key inside StoryReconciler.usageDirty after guarding
//     against a nil receiver (internal/controller/story_controller.go:91-105).
//   - Records a dirty-mark metric so operators can correlate Impulse events with recounts.
//   - Dirty flags are cleared only after count/status persistence succeeds,
//     preventing data loss when recounts fail (internal/controller/story_controller.go:309-332).
//
// Arguments:
//   - key types.NamespacedName: Story identifier derived from Impulse watch handlers
//     (internal/controller/story_controller.go:665-690).
//
// Notes / Gotchas:
//   - Dirty flags live in-memory; controller restarts drop pending recounts.
func (r *StoryReconciler) markUsageDirty(key types.NamespacedName) {
	r.markStoryDirty(&r.usageDirty, key, "usage", "Marked Story as dirty for usage recount")
}

// markTriggerDirty records that the given Story needs its trigger counters
// recomputed because a referencing StoryRun changed
// (internal/controller/story_controller.go:78-121,665-690).
//
// Behavior:
//   - Stores the namespaced key in triggerDirty, guarded by a nil-reconciler check.
//   - Emits a dirty-mark metric and debug log for observability.
//   - Flags remain set until trigger recount + status persistence completes without pending work.
//
// Arguments:
//   - key types.NamespacedName: Story identifier derived from StoryRun events.
//
// Notes / Gotchas:
//   - Dirty flags reset on controller restart, so trigger recounts triggered before a restart may be lost.
func (r *StoryReconciler) markTriggerDirty(key types.NamespacedName) {
	r.markStoryDirty(&r.triggerDirty, key, "trigger", "Marked Story as dirty for trigger recount")
}

// markStoryDirty centralizes dirty-flag bookkeeping so usage/trigger helpers
// share identical metrics and logging semantics.
//
// Behavior:
//   - Guards against nil receiver or set to prevent panics.
//   - Stores the namespaced key in the provided dirty set.
//   - Records a dirty-mark metric and logs at V(1) for observability.
//
// Arguments:
//   - set *rec.Set: the dirty set to mark (usageDirty or triggerDirty).
//   - key types.NamespacedName: the Story identifier.
//   - metricReason string: reason label for the metric (e.g., "usage", "trigger").
//   - logMsg string: message to log at V(1) level.
func (r *StoryReconciler) markStoryDirty(set *rec.Set, key types.NamespacedName, metricReason, logMsg string) {
	if r == nil || set == nil {
		return
	}
	namespacedKey := refs.NamespacedKey(key.Namespace, key.Name)
	set.Mark(namespacedKey)
	metrics.RecordStoryDirtyMark(key.Namespace, key.Name, metricReason)
	logging.NewControllerLogger(context.Background(), "story-dirty").V(1).Info(logMsg, "story", key.String())
}

// shouldSyncUsageCounts returns true when a Story's usage count must be
// recomputed because its spec generation changed or the dirty flag remains set
// due to Impulse events (internal/controller/story_controller.go:91-118,288-335,665-690).
//
// Arguments:
//   - storyKey types.NamespacedName: forwarded to the dirty set to inspect its state.
//   - story *bubuv1alpha1.Story: live object whose generation and status are compared.
//
// Returns:
//   - bool: true when countStoryUsage must run, false when cached usage counts remain valid.
//
// Notes:
//   - Callers must provide a non-nil story pointer; this helper dereferences Status/Generation directly.
func (r *StoryReconciler) shouldSyncUsageCounts(storyKey types.NamespacedName, story *bubuv1alpha1.Story) bool {
	return r.shouldSyncCounts(&r.usageDirty, storyKey, story)
}

// shouldSyncTriggerCounts returns true when Story trigger counters must be
// recomputed because the spec generation changed or StoryRun events marked the
// Story dirty (internal/controller/story_controller.go:91-121,288-335,665-690).
//
// Arguments:
//   - storyKey types.NamespacedName: Story identifier passed to the dirty set.
//   - story *bubuv1alpha1.Story: live Story whose generation and cached trigger counts are evaluated.
//
// Returns:
//   - bool: true when countStoryTriggers must run, false when cached trigger counts are still valid.
//
// Notes:
//   - Callers must enforce non-nil story inputs; this helper dereferences Status/Generation directly.
func (r *StoryReconciler) shouldSyncTriggerCounts(storyKey types.NamespacedName, story *bubuv1alpha1.Story) bool {
	return r.shouldSyncCounts(&r.triggerDirty, storyKey, story)
}

// shouldSyncCounts centralizes the generation-vs-dirty check so usage and trigger
// recount gates stay aligned on the same dirty-set semantics.
//
// Behavior:
//   - Returns true if ObservedGeneration differs from Generation (spec changed).
//   - Returns true if the Story is marked dirty in the provided set.
//   - Falls back to generation check alone when set is nil.
//
// Arguments:
//   - set *rec.Set: the dirty set to check (usageDirty or triggerDirty).
//   - storyKey types.NamespacedName: the Story identifier for dirty lookup.
//   - story *bubuv1alpha1.Story: provides generation and status for comparison.
//
// Returns:
//   - bool: true when counts must be recomputed.
func (r *StoryReconciler) shouldSyncCounts(set *rec.Set, storyKey types.NamespacedName, story *bubuv1alpha1.Story) bool {
	if set == nil {
		return story.Status.ObservedGeneration != story.Generation
	}
	return story.Status.ObservedGeneration != story.Generation ||
		set.IsDirty(refs.NamespacedKey(storyKey.Namespace, storyKey.Name))
}

// +kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=bubustack.io,resources=stories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bubustack.io,resources=stories/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=bubustack.io,resources=impulses,verbs=get;list;watch

// Reconcile converges Story validation status, usage counts, and trigger statistics.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.0/pkg/reconcile
//
// Behavior:
//   - Applies optional reconcile timeout from operator config.
//   - Loads Story, handles deletion, and ensures finalizer.
//   - Validates Engram/Transport/Story references and updates status.
//   - Recomputes usage counts (Impulses) and trigger counts (StoryRuns).
//   - Returns errors for transient failures; validation failures are recorded in status.
//
// Arguments:
//   - ctx context.Context: propagated to all sub-operations.
//   - req ctrl.Request: contains the Story namespace and name.
//
// Returns:
//   - ctrl.Result: may include RequeueAfter for bounded trigger backfill.
//   - error: on transient failures for controller-runtime backoff.
func (r *StoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "story").WithValues("story", req.Name, "namespace", req.Namespace)

	timeout := time.Duration(0)
	if r.ConfigResolver != nil {
		timeout = r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	}
	if timeout > 0 {
		log.V(1).Info("Applying reconcile timeout", "timeout", timeout.String())
	}

	return rec.Run(ctx, req, rec.RunOptions{
		Controller: "story",
		Timeout:    timeout,
	}, rec.RunHooks[*bubuv1alpha1.Story]{
		Get: func(ctx context.Context, req ctrl.Request) (*bubuv1alpha1.Story, error) {
			var story bubuv1alpha1.Story
			if err := r.Get(ctx, req.NamespacedName, &story); err != nil {
				return nil, err
			}
			return &story, nil
		},
		HandleDeletion: func(ctx context.Context, story *bubuv1alpha1.Story) (bool, ctrl.Result, error) {
			return r.handleStoryDeletion(ctx, story)
		},
		EnsureFinalizer: func(ctx context.Context, story *bubuv1alpha1.Story) error {
			return r.ensureStoryFinalizer(ctx, story, log.Logr())
		},
		HandleNormal: func(ctx context.Context, story *bubuv1alpha1.Story) (ctrl.Result, error) {
			return rec.RunPipeline(ctx, story, rec.PipelineHooks[*bubuv1alpha1.Story, *storyPipelineState]{
				Prepare: func(ctx context.Context, story *bubuv1alpha1.Story) (*storyPipelineState, *ctrl.Result, error) {
					return &storyPipelineState{
						validationErr: r.validateEngramReferences(ctx, story),
					}, nil, nil
				},
				Ensure: func(ctx context.Context, story *bubuv1alpha1.Story, state *storyPipelineState) (ctrl.Result, error) {
					return r.updateStoryStatus(ctx, story, state.validationErr)
				},
				Finalize: func(ctx context.Context, story *bubuv1alpha1.Story, state *storyPipelineState, ensureResult ctrl.Result, ensureErr error) (ctrl.Result, error) {
					if ensureErr != nil {
						return ctrl.Result{}, ensureErr
					}
					if state.validationErr != nil {
						if !isReferenceValidationFailure(state.validationErr) {
							log.Error(state.validationErr, "Transient error validating Story references")
							return ctrl.Result{}, state.validationErr
						}
						return ctrl.Result{}, nil
					}
					return ensureResult, nil
				},
			})
		},
	})
}

func (r *StoryReconciler) controllerConfig() *config.ControllerConfig {
	if r == nil || r.ConfigResolver == nil {
		return nil
	}
	if resolved := r.ConfigResolver.GetOperatorConfig(); resolved != nil {
		return &resolved.Controller
	}
	return nil
}

// handleStoryDeletion checks if the Story is being deleted and handles cleanup.
//
// Behavior:
//   - Delegates to rec.ShortCircuitDeletion to detect deletion.
//   - Calls reconcileDelete to remove the finalizer when deletion is in progress.
//   - Returns nil result when deletion is not being handled.
//
// Arguments:
//   - ctx context.Context: propagated to deletion handling.
//   - story *bubuv1alpha1.Story: the Story to check for deletion.
//
// Returns:
//   - handled bool: true when deletion logic ran and reconciliation should stop.
//   - ctrl.Result: result to return when handled is true.
//   - error: on finalizer removal failures.
func (r *StoryReconciler) handleStoryDeletion(ctx context.Context, story *bubuv1alpha1.Story) (bool, ctrl.Result, error) {
	return rec.ShortCircuitDeletion(ctx, story, func(ctx context.Context) (ctrl.Result, error) {
		return r.reconcileDelete(ctx, story)
	})
}

// ensureStoryFinalizer adds the Story finalizer if not already present.
//
// Behavior:
//   - Delegates to kubeutil.EnsureFinalizer for idempotent addition.
//   - Emits events on success and failure.
//   - Logs at V(1) when the finalizer is already present.
//
// Arguments:
//   - ctx context.Context: propagated to the finalizer operation.
//   - story *bubuv1alpha1.Story: the Story to add the finalizer to.
//   - logger logr.Logger: for logging operations.
//
// Returns:
//   - error: on finalizer addition failures.
func (r *StoryReconciler) ensureStoryFinalizer(ctx context.Context, story *bubuv1alpha1.Story, logger logr.Logger) error {
	logger = logger.WithValues("story", story.Name, "namespace", story.Namespace)
	added, err := kubeutil.EnsureFinalizer(ctx, r.Client, story, StoryFinalizer)
	if err != nil {
		logger.Error(err, "Failed to add finalizer to Story")
		if r.Recorder != nil {
			r.Recorder.Event(story, "Warning", "FinalizerAddFailed", err.Error())
		}
		return err
	}
	if !added {
		logger.V(1).Info("Story finalizer already present")
		return nil
	}

	logger.Info("Added Story finalizer")
	if r.Recorder != nil {
		r.Recorder.Event(story, "Normal", "FinalizerAdded", "Story finalizer attached")
	}
	return nil
}

// updateStoryStatus recomputes derived Story status, ensuring users see fresh validation results,
// transport summaries, usage counts, and trigger totals after each reconcile.
//
// Behavior:
//   - Recomputes usage counts when spec changed or usageDirty is set.
//   - Recomputes trigger counts when spec changed or triggerDirty is set.
//   - Builds desired status via buildDesiredStoryStatus and patches if changed.
//   - Emits validation events only on meaningful transitions (no spam).
//   - Clears dirty flags only after successful count + status persistence.
//   - Returns bounded requeue when trigger backfill is pending.
//
// Arguments:
//   - ctx context.Context: propagated to count and patch operations.
//   - story *bubuv1alpha1.Story: the Story to update.
//   - validationErr error: nil when validation passed, otherwise drives status.
//
// Returns:
//   - ctrl.Result: may include RequeueAfter for bounded backfill.
//   - error: on count or status patch failures.
func (r *StoryReconciler) updateStoryStatus(
	ctx context.Context,
	story *bubuv1alpha1.Story,
	validationErr error,
) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "story-status").WithStory(story)
	storyKey := types.NamespacedName{Name: story.Name, Namespace: story.Namespace}

	needUsage := r.shouldSyncUsageCounts(storyKey, story)
	needTriggers := r.shouldSyncTriggerCounts(storyKey, story)

	usageCount := story.Status.UsageCount
	triggerCount := story.Status.Triggers

	var (
		usageErr        error
		triggerErr      error
		triggersPending bool
	)

	if needUsage {
		usageCount, usageErr = r.countStoryUsage(ctx, story)
		if usageErr != nil {
			log.Error(usageErr, "Failed to compute story usage count")
			return ctrl.Result{}, fmt.Errorf("count story usage: %w", usageErr)
		}
	}

	if needTriggers {
		triggerCount, triggersPending, triggerErr = r.countStoryTriggersBounded(ctx, story, maxStoryRunsToMarkPerReconcile)
		if triggerErr != nil {
			log.Error(triggerErr, "Failed to compute story trigger count")
			return ctrl.Result{}, fmt.Errorf("count story triggers: %w", triggerErr)
		}
	}

	previousStatus := story.Status.DeepCopy()
	desiredStatus := r.buildDesiredStoryStatus(story, validationErr, usageCount, triggerCount)

	// Transition-based validation event (no spam).
	// We only emit when:
	// - desired is invalid AND
	// - previous was valid OR the error message changed.
	if validationErr != nil {
		if r.validationEventShouldFire(previousStatus, desiredStatus) {
			reason := validation.ExtractReason(validationErr)
			rec.RecordEvent(r.Recorder, log.Logr(), story, corev1.EventTypeWarning, reason, validationErr.Error())
		}
	}

	// Idempotent status patch (shared helper).
	changed, err := rec.PatchStatusIfChanged(ctx, r.Client, story, func(obj *bubuv1alpha1.Story) {
		obj.Status = *desiredStatus
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	if changed {
		story.Status = *desiredStatus
	} else {
		// Ensure in-memory status matches what we computed for the rest of reconcile.
		story.Status = *desiredStatus
	}

	// IMPORTANT: clear dirty flags only after successful count + status persistence.
	if needUsage && usageErr == nil {
		r.usageDirty.Clear(refs.NamespacedKey(storyKey.Namespace, storyKey.Name))
	}

	// If triggers are pending (bounded backfill), keep trigger dirty to ensure progress.
	// If no pending and succeeded, clear dirty.
	if needTriggers && triggerErr == nil && !triggersPending {
		r.triggerDirty.Clear(refs.NamespacedKey(storyKey.Namespace, storyKey.Name))
	}

	// Bounded backfill requeue.
	if triggersPending {
		return ctrl.Result{RequeueAfter: requeueAfterBackfill}, nil
	}

	return ctrl.Result{}, nil
}

// validationEventShouldFire determines if a validation event should be emitted.
//
// Behavior:
//   - Returns true when prev or next is nil (initial state).
//   - Returns true when ValidationStatus changed (valid<->invalid transition).
//   - Returns true when still invalid but ValidationErrors changed.
//   - Used to prevent event spam on repeated reconciles with same state.
//
// Arguments:
//   - prev *bubuv1alpha1.StoryStatus: previous status snapshot.
//   - next *bubuv1alpha1.StoryStatus: desired status snapshot.
//
// Returns:
//   - bool: true when a validation event should be emitted.
func (r *StoryReconciler) validationEventShouldFire(prev, next *bubuv1alpha1.StoryStatus) bool {
	if prev == nil || next == nil {
		return true
	}
	// Emit only on meaningful transitions.
	if prev.ValidationStatus != next.ValidationStatus {
		return true
	}
	// If still invalid, emit only if message changed.
	if next.ValidationStatus == enums.ValidationStatusInvalid {
		return !apiequality.Semantic.DeepEqual(prev.ValidationErrors, next.ValidationErrors)
	}
	return false
}

// buildDesiredStoryStatus snapshots the Story status after validation/count
// recomputations so callers can patch a consistent view of steps, transports,
// usage, and trigger metrics.
//
// Behavior:
//   - Deep-copies the current status, stamps StepsTotal/ObservedGeneration, and
//     propagates freshly computed usage/trigger counters.
//   - Refreshes the transport summary via buildTransportStatuses so spec
//     changes immediately appear in status.
//   - Rebuilds validation fields and the Ready condition based on validationErr,
//     toggling between ValidationPassed/ValidationFailed reasons.
//
// Arguments:
//   - story *bubuv1alpha1.Story: provides spec metadata plus the existing status template.
//   - validationErr error: nil when validation passed; otherwise drives validation status/messages.
//   - usageCount int32 / triggerCount int64: reconciled counters copied into the snapshot.
//
// Returns:
//   - *bubuv1alpha1.StoryStatus containing the desired status fields; callers decide when to persist it.
func (r *StoryReconciler) buildDesiredStoryStatus(
	story *bubuv1alpha1.Story,
	validationErr error,
	usageCount int32,
	triggerCount int64,
) *bubuv1alpha1.StoryStatus {
	snapshot := story.Status.DeepCopy()
	snapshot.StepsTotal = int32(len(story.Spec.Steps) + len(story.Spec.Compensations) + len(story.Spec.Finally))
	snapshot.ObservedGeneration = story.Generation
	snapshot.Transports = buildTransportStatuses(story)
	snapshot.UsageCount = usageCount
	snapshot.Triggers = triggerCount

	conditionsCopy := append([]metav1.Condition(nil), snapshot.Conditions...)
	cm := conditions.NewConditionManager(story.Generation)

	specErrors := []string(nil)
	if validationErr != nil {
		specErrors = []string{validationErr.Error()}
	}

	outcome := kubeutil.ComputeValidationOutcome(kubeutil.ValidationParams{
		ValidationErrors:       specErrors,
		SuccessReason:          conditions.ReasonValidationPassed,
		SuccessMessage:         "All Engram references are valid.",
		ValidationFailedReason: conditions.ReasonValidationFailed,
	})

	snapshot.ValidationStatus = outcome.ValidationStatus
	snapshot.ValidationErrors = outcome.AggregatedErrors()
	cm.SetCondition(&conditionsCopy, conditions.ConditionReady, outcome.ReadyStatus, outcome.ReadyReason, outcome.ReadyMessage)
	snapshot.Conditions = conditionsCopy
	return snapshot
}

// buildTransportStatuses normalizes story.Spec.Transports into the status slice
// reported to users, deriving per-transport execution mode metadata.
//
// Behavior:
//   - Returns nil when the Story declares no transports.
//   - Preallocates the status slice to spec length, trimming TransportRefs and
//     copying names verbatim.
//   - Calls determineTransportMode for each transport so Mode/ModeReason reflect
//     the resolved execution pattern.
//
// Arguments:
//   - story *bubuv1alpha1.Story whose transport spec is being mirrored.
//
// Returns:
//   - []bubuv1alpha1.StoryTransportStatus mirroring spec order with normalized fields.
func buildTransportStatuses(story *bubuv1alpha1.Story) []bubuv1alpha1.StoryTransportStatus {
	if len(story.Spec.Transports) == 0 {
		return nil
	}
	statuses := make([]bubuv1alpha1.StoryTransportStatus, len(story.Spec.Transports))
	for i, transport := range story.Spec.Transports {
		mode, reason := determineTransportMode(story, &transport)
		statuses[i] = bubuv1alpha1.StoryTransportStatus{
			Name:         transport.Name,
			TransportRef: strings.TrimSpace(transport.TransportRef),
			Mode:         mode,
			ModeReason:   reason,
		}
	}
	return statuses
}

// determineTransportMode derives the transport execution mode based on Story pattern.
//
// Behavior:
//   - Returns TransportModeFallback with "story-missing" when story is nil.
//   - Returns TransportModeFallback for BatchPattern stories.
//   - Returns TransportModeHot for other patterns (streaming default).
//
// Arguments:
//   - story *bubuv1alpha1.Story: the parent Story.
//   - transport *bubuv1alpha1.StoryTransport: the transport being evaluated.
//
// Returns:
//   - enums.TransportMode: the resolved transport mode.
//   - string: reason explaining the mode selection.
func determineTransportMode(story *bubuv1alpha1.Story, transport *bubuv1alpha1.StoryTransport) (enums.TransportMode, string) {
	_ = transport
	if story == nil {
		return enums.TransportModeFallback, "story-missing"
	}
	switch story.Spec.Pattern {
	case enums.BatchPattern:
		return enums.TransportModeFallback, "batch-pattern"
	default:
		return enums.TransportModeHot, "streaming-default"
	}
}

// NOTE: validationReasonError, wrapValidationReasonError, and deriveValidationReason
// have been moved to pkg/validation for reuse by webhooks and other controllers.
// Use validation.WrapWithReason() and validation.ExtractReason() instead.

// reconcileDelete removes the Story finalizer during deletion.
//
// Behavior:
//   - Returns immediately if the finalizer is not present.
//   - Removes the finalizer via merge patch.
//   - Emits events on success and failure.
//
// Arguments:
//   - ctx context.Context: propagated to patch operations.
//   - story *bubuv1alpha1.Story: the Story being deleted.
//
// Returns:
//   - ctrl.Result: empty on success.
//   - error: on patch failures.
func (r *StoryReconciler) reconcileDelete(ctx context.Context, story *bubuv1alpha1.Story) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "story-delete").WithStory(story)
	log.Info("Reconciling deletion for Story")

	if !controllerutil.ContainsFinalizer(story, StoryFinalizer) {
		return ctrl.Result{}, nil
	}

	mergePatch := client.MergeFrom(story.DeepCopy())
	controllerutil.RemoveFinalizer(story, StoryFinalizer)
	if err := r.Patch(ctx, story, mergePatch); err != nil {
		log.Error(err, "Failed to remove finalizer from Story")
		if r.Recorder != nil {
			r.Recorder.Event(story, "Warning", "FinalizerRemovalFailed", err.Error())
		}
		return ctrl.Result{}, err
	}

	if r.Recorder != nil {
		r.Recorder.Event(story, "Normal", "FinalizerRemoved", "Story finalizer removed")
	}
	return ctrl.Result{}, nil
}

// validateStoryTransports validates all transports declared in the Story spec.
//
// Behavior:
//   - Checks that each transport has a non-empty name and transportRef.
//   - Enforces unique transport names within the Story.
//   - Fetches referenced Transport resources to verify existence.
//   - Caches Transport lookups to avoid redundant API calls.
//
// Arguments:
//   - ctx context.Context: propagated to Transport Get operations.
//   - story *bubuv1alpha1.Story: the Story containing transport declarations.
//
// Returns:
//   - map[string]struct{}: set of valid transport names for step validation.
//   - error: wrapped validation error if any transport is invalid.
func (r *StoryReconciler) validateStoryTransports(ctx context.Context, story *bubuv1alpha1.Story) (map[string]struct{}, error) {
	transportByName := make(map[string]struct{}, len(story.Spec.Transports))
	transportCache := make(map[string]struct{})
	var validationErrors []string

	for _, transportDecl := range story.Spec.Transports {
		name := strings.TrimSpace(transportDecl.Name)
		if name == "" {
			validationErrors = append(validationErrors, "story transport entries must define a name")
			continue
		}
		if _, exists := transportByName[name]; exists {
			validationErrors = append(validationErrors, fmt.Sprintf("transport name %q is duplicated; names must be unique per story", name))
			continue
		}
		transportByName[name] = struct{}{}

		ref := strings.TrimSpace(transportDecl.TransportRef)
		if ref == "" {
			validationErrors = append(validationErrors, fmt.Sprintf("transport '%s' must set transportRef", transportDecl.Name))
			continue
		}
		if _, checked := transportCache[ref]; checked {
			continue
		}
		var transport transportv1alpha1.Transport
		if err := r.Get(ctx, types.NamespacedName{Name: ref}, &transport); err != nil {
			if errors.IsNotFound(err) {
				validationErrors = append(validationErrors, fmt.Sprintf("transport '%s' references Transport %q which does not exist", transportDecl.Name, ref))
				continue
			}
			return nil, fmt.Errorf("failed to get Transport %q for story: %w", ref, err)
		}
		transportCache[ref] = struct{}{}
	}

	if len(validationErrors) > 0 {
		errMsg := strings.Join(validationErrors, "; ")
		return nil, validation.WrapWithReason(conditions.ReasonTransportReferenceInvalid, fmt.Errorf("%s", errMsg))
	}
	return transportByName, nil
}

// validateStoryStep validates a single step within a Story.
//
// Behavior:
//   - For steps with Ref, ensures the referenced Engram exists and mode is compatible.
//   - For steps without type or ref, returns an error.
//   - For ExecuteStory steps, validates the nested Story reference.
//   - Validates that step transport references exist in transportByName.
//
// Arguments:
//   - ctx context.Context: propagated to reference lookups.
//   - story *bubuv1alpha1.Story: the parent Story.
//   - step *bubuv1alpha1.Step: the step to validate.
//   - transportByName map[string]struct{}: valid transport names from validateStoryTransports.
//   - engramCache map[types.NamespacedName]*bubuv1alpha1.Engram: memoized Engram lookups.
//   - index int: step index for error messages.
//
// Returns:
//   - error: wrapped validation error if the step is invalid.
func (r *StoryReconciler) validateStoryStep(
	ctx context.Context,
	story *bubuv1alpha1.Story,
	step *bubuv1alpha1.Step,
	transportByName map[string]struct{},
	engramCache map[types.NamespacedName]*bubuv1alpha1.Engram,
	index int,
) error {
	switch {
	case step.Ref != nil:
		if err := r.ensureEngramExists(ctx, story, step, engramCache); err != nil {
			return err
		}
	case strings.TrimSpace(string(step.Type)) == "":
		return validation.WrapWithReason(conditions.ReasonStoryReferenceInvalid, fmt.Errorf("step %d ('%s') must have a 'type' or a 'ref'", index, step.Name))
	case step.Type == enums.StepTypeExecuteStory:
		if err := r.validateExecuteStoryStep(ctx, story, step, index); err != nil {
			return err
		}
	}

	if transportName := strings.TrimSpace(step.Transport); transportName != "" {
		if _, ok := transportByName[transportName]; !ok {
			return validation.WrapWithReason(conditions.ReasonTransportReferenceInvalid, fmt.Errorf("step %d ('%s') references unknown transport %q (story pattern %s)", index, step.Name, transportName, story.Spec.Pattern))
		}
	}
	return nil
}

// ensureEngramExists fetches (or reuses) the Engram referenced by a Story step,
// memoizing DeepCopies and enforcing streaming-mode compatibility before the
// caller proceeds with validation.
//
// Behavior:
//   - Resolves the Engram namespaced name from the step reference, defaulting
//     to the parent Story namespace.
//   - Returns cached Engrams immediately when present, keeping validation
//     idempotent across steps.
//   - Issues client.Get on cache miss, logging non-NotFound errors with Story
//     and step context before bubbling them up.
//   - Deep-copies successful results into the cache and calls validateEngramMode
//     so streaming Stories enforce workload constraints consistently.
func (r *StoryReconciler) ensureEngramExists(
	ctx context.Context,
	story *bubuv1alpha1.Story,
	step *bubuv1alpha1.Step,
	cache map[types.NamespacedName]*bubuv1alpha1.Engram,
) error {
	log := logging.NewControllerLogger(ctx, "story-engram").WithStory(story)
	targetNamespace := refs.ResolveNamespace(story, &step.Ref.ObjectReference)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		r.Client,
		r.controllerConfig(),
		story,
		"bubustack.io",
		"Story",
		"bubustack.io",
		"Engram",
		targetNamespace,
		step.Ref.Name,
		"EngramRef",
	); err != nil {
		return validation.WrapWithReason(conditions.ReasonEngramReferenceInvalid, fmt.Errorf("step '%s' references engram in namespace '%s': %w", step.Name, targetNamespace, err))
	}

	engram, key, err := refs.LoadNamespacedReference(
		ctx,
		r.Client,
		story,
		step.Ref,
		func() *bubuv1alpha1.Engram { return &bubuv1alpha1.Engram{} },
		cache,
	)
	log = log.WithValues(
		"step", step.Name,
		"engram", key.Name,
		"engramNamespace", key.Namespace,
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return validation.WrapWithReason(conditions.ReasonEngramReferenceInvalid,
				fmt.Errorf("step '%s' references engram '%s' which does not exist in namespace '%s'",
					step.Name, key.Name, key.Namespace))
		}
		log.Error(err, "Failed to fetch Engram for Story step")
		return fmt.Errorf("failed to get engram for step '%s': %w", step.Name, err)
	}

	return validateEngramMode(story, engram, step.Name)
}

// validateEngramMode ensures streaming Stories reference Engrams running a
// Deployment or StatefulSet workload mode; batch Stories accept any mode.
//
// Behavior:
//   - Short-circuits for non-streaming patterns to avoid unnecessary errors.
//   - Wraps incompatible Engram modes with EngramReferenceInvalid so callers
//     record structured validation events.
func validateEngramMode(story *bubuv1alpha1.Story, engram *bubuv1alpha1.Engram, stepName string) error {
	if story.Spec.Pattern == enums.StreamingPattern {
		if engram.Spec.Mode != enums.WorkloadModeDeployment && engram.Spec.Mode != enums.WorkloadModeStatefulSet {
			return validation.WrapWithReason(conditions.ReasonEngramReferenceInvalid, fmt.Errorf("step '%s' references engram '%s' with mode '%s', but story pattern %s requires 'deployment' or 'statefulset' mode", stepName, engram.Name, engram.Spec.Mode, story.Spec.Pattern))
		}
	}
	return nil
}

// validateExecuteStoryStep validates a step of type ExecuteStory.
//
// Behavior:
//   - Requires a 'with' block containing a storyRef.
//   - Parses the 'with' block as executeStoryWith JSON.
//   - Fetches the referenced Story to verify existence.
//
// Arguments:
//   - ctx context.Context: propagated to Story lookup.
//   - story *bubuv1alpha1.Story: the parent Story.
//   - step *bubuv1alpha1.Step: the ExecuteStory step to validate.
//   - index int: step index for error messages.
//
// Returns:
//   - error: wrapped validation error if the step is invalid.
func (r *StoryReconciler) validateExecuteStoryStep(ctx context.Context, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, index int) error {
	if step.With == nil {
		return validation.WrapWithReason(conditions.ReasonStoryReferenceInvalid, fmt.Errorf("step %d ('%s') is of type 'executeStory' but has no 'with' block", index, step.Name))
	}
	var with executeStoryWith
	if err := json.Unmarshal(step.With.Raw, &with); err != nil {
		return validation.WrapWithReason(conditions.ReasonStoryReferenceInvalid, fmt.Errorf("step %d ('%s') has an invalid 'with' block for 'executeStory': %w", index, step.Name, err))
	}
	targetNamespace := refs.ResolveNamespace(story, &with.StoryRef.ObjectReference)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		r.Client,
		r.controllerConfig(),
		story,
		"bubustack.io",
		"Story",
		"bubustack.io",
		"Story",
		targetNamespace,
		with.StoryRef.Name,
		"StoryRef",
	); err != nil {
		return validation.WrapWithReason(conditions.ReasonStoryReferenceInvalid, fmt.Errorf("step '%s' references Story in namespace '%s': %w", step.Name, targetNamespace, err))
	}
	_, key, err := refs.LoadNamespacedReference(
		ctx,
		r.Client,
		story,
		&with.StoryRef,
		func() *bubuv1alpha1.Story { return &bubuv1alpha1.Story{} },
		nil,
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return validation.WrapWithReason(conditions.ReasonStoryReferenceInvalid,
				fmt.Errorf("step '%s' references story '%s' which does not exist in namespace '%s'",
					step.Name, key.Name, key.Namespace))
		}
		return fmt.Errorf("failed to get story for step '%s': %w", step.Name, err)
	}
	return nil
}

// validateEngramReferences validates all step references in the Story.
//
// Behavior:
//   - First validates all transports via validateStoryTransports.
//   - Iterates through all steps and validates each via validateStoryStep.
//   - Uses an Engram cache to avoid redundant API calls across steps.
//
// Arguments:
//   - ctx context.Context: propagated to validation operations.
//   - story *bubuv1alpha1.Story: the Story to validate.
//
// Returns:
//   - error: the first validation error encountered, or nil if all valid.
func (r *StoryReconciler) validateEngramReferences(ctx context.Context, story *bubuv1alpha1.Story) error {
	transportByName, err := r.validateStoryTransports(ctx, story)
	if err != nil {
		return err
	}

	engramCache := make(map[types.NamespacedName]*bubuv1alpha1.Engram)
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if err := r.validateStoryStep(ctx, story, step, transportByName, engramCache, i); err != nil {
			return err
		}
	}
	return nil
}

// isReferenceValidationFailure distinguishes permanent validation errors from transient failures.
//
// Behavior:
//   - Returns false for nil errors.
//   - Returns false for context cancellation/deadline errors.
//   - Returns false for transient API errors (timeout, rate limit, server errors).
//   - Returns false for permission errors (forbidden, unauthorized).
//   - Returns true for NotFound and other permanent failures.
//
// Arguments:
//   - err error: the error to classify.
//
// Returns:
//   - bool: true if the error represents a permanent reference validation failure.
func isReferenceValidationFailure(err error) bool {
	if err == nil {
		return false
	}

	if goerrors.Is(err, context.Canceled) || goerrors.Is(err, context.DeadlineExceeded) {
		return false
	}

	switch {
	case errors.IsTimeout(err),
		errors.IsTooManyRequests(err),
		errors.IsServerTimeout(err),
		errors.IsServiceUnavailable(err),
		errors.IsInternalError(err):
		return false
	case errors.IsForbidden(err),
		errors.IsUnauthorized(err):
		return false
	case errors.IsNotFound(err):
		return true
	default:
		return true
	}
}

// SetupWithManager sets up the controller with the Manager.
//
// Behavior:
//   - Registers the controller for Story resources with GenerationChangedPredicate.
//   - Watches Engrams and EngramTemplates to revalidate dependent Stories.
//   - Watches nested Stories for executeStory step validation.
//   - Watches StoryRuns with trigger-relevant predicate for trigger counting.
//   - Watches Impulses with usage-relevant predicate for usage counting.
//   - Avoids global WithEventFilter to allow watch-specific predicates.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//   - opts controller.Options: controller configuration (concurrency, etc.).
//
// Returns:
//   - error: nil on success, or controller setup error.
//
// Side Effects:
//   - Registers the event recorder for story-controller.
func (r *StoryReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	r.Recorder = mgr.GetEventRecorderFor("story-controller")

	mapEngramToStories := func(ctx context.Context, obj client.Object) []reconcile.Request {
		engram, ok := obj.(*bubuv1alpha1.Engram)
		if !ok {
			return nil
		}
		return r.enqueueStoriesReferencingEngram(ctx, engram.GetNamespace(), engram.GetName())
	}

	mapStoryToStories := func(ctx context.Context, obj client.Object) []reconcile.Request {
		story, ok := obj.(*bubuv1alpha1.Story)
		if !ok {
			return nil
		}
		return r.enqueueStoriesReferencingStory(ctx, story.GetNamespace(), story.GetName())
	}

	mapEngramTemplateToStories := func(ctx context.Context, obj client.Object) []reconcile.Request {
		template, ok := obj.(*catalogv1alpha1.EngramTemplate)
		if !ok {
			return nil
		}

		var engramList bubuv1alpha1.EngramList
		mapLog := logging.NewControllerLogger(ctx, "story-mapper").WithValues("template", template.GetName())
		if err := r.List(ctx, &engramList, client.MatchingFields{contracts.IndexEngramTemplateRef: template.GetName()}); err != nil {
			mapLog.Error(err, "Failed to list Engrams for template")
			return nil
		}

		dedup := make(map[types.NamespacedName]struct{})
		var reqs []reconcile.Request
		for i := range engramList.Items {
			engram := &engramList.Items[i]
			for _, req := range r.enqueueStoriesReferencingEngram(ctx, engram.Namespace, engram.Name) {
				dedup[req.NamespacedName] = struct{}{}
			}
		}
		for nn := range dedup {
			reqs = append(reqs, reconcile.Request{NamespacedName: nn})
		}
		return reqs
	}

	mapStoryRunToStory := func(ctx context.Context, obj client.Object) []reconcile.Request {
		storyRun, ok := obj.(*runsv1alpha1.StoryRun)
		if !ok || storyRun.Spec.StoryRef.Name == "" {
			return nil
		}
		namespace := refs.ResolveNamespace(storyRun, &storyRun.Spec.StoryRef.ObjectReference)
		key := types.NamespacedName{Name: storyRun.Spec.StoryRef.Name, Namespace: namespace}
		r.markTriggerDirty(key)
		return []reconcile.Request{{NamespacedName: key}}
	}

	mapImpulseToStories := func(ctx context.Context, obj client.Object) []reconcile.Request {
		impulse, ok := obj.(*bubuv1alpha1.Impulse)
		if !ok || impulse.Spec.StoryRef.Name == "" {
			return nil
		}
		namespace := refs.ResolveNamespace(impulse, &impulse.Spec.StoryRef.ObjectReference)
		key := types.NamespacedName{Name: impulse.Spec.StoryRef.Name, Namespace: namespace}
		r.markUsageDirty(key)
		return []reconcile.Request{{NamespacedName: key}}
	}

	return ctrl.NewControllerManagedBy(mgr).
		// Primary: Story spec changes only (generation changes).
		For(&bubuv1alpha1.Story{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// Engram changes that can affect validation (spec/gen changes).
		Watches(&bubuv1alpha1.Engram{}, handler.EnqueueRequestsFromMapFunc(mapEngramToStories),
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// Template changes -> affects Engrams -> affects Stories (spec/gen changes).
		Watches(&catalogv1alpha1.EngramTemplate{}, handler.EnqueueRequestsFromMapFunc(mapEngramTemplateToStories),
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// Nested story graphs: only enqueue when the referenced Story's spec changes.
		Watches(&bubuv1alpha1.Story{}, handler.EnqueueRequestsFromMapFunc(mapStoryToStories),
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// StoryRun: ONLY enqueue on “trigger relevant” transitions (phase/start/finish), not annotation spam.
		Watches(&runsv1alpha1.StoryRun{}, handler.EnqueueRequestsFromMapFunc(mapStoryRunToStory),
			builder.WithPredicates(rec.StoryRunTriggerRelevantPredicate())).
		// Impulse: enqueue on create/delete or storyRef changes.
		Watches(&bubuv1alpha1.Impulse{}, handler.EnqueueRequestsFromMapFunc(mapImpulseToStories),
			builder.WithPredicates(rec.ImpulseUsageRelevantPredicate())).
		WithOptions(opts).
		Complete(r)
}

// enqueueStoriesReferencingEngram returns reconcile requests for Stories that reference the Engram.
//
// Behavior:
//   - Builds a namespaced key and delegates to enqueueStoriesByIndex.
//   - Uses the storyStepEngramIndexField for indexed lookup.
//
// Arguments:
//   - ctx context.Context: propagated to the index lookup.
//   - namespace, name string: the Engram's namespace and name.
//
// Returns:
//   - []reconcile.Request: requests for Stories referencing the Engram.
func (r *StoryReconciler) enqueueStoriesReferencingEngram(ctx context.Context, namespace, name string) []reconcile.Request {
	indexKey := refs.NamespacedKey(namespace, name)
	return r.enqueueStoriesByIndex(ctx, storyStepEngramIndexField, indexKey,
		"Failed to list Stories referencing Engram",
		"namespace", namespace, "name", name)
}

// enqueueStoriesReferencingStory returns reconcile requests for Stories that reference another Story.
//
// Behavior:
//   - Builds a namespaced key and delegates to enqueueStoriesByIndex.
//   - Uses the storyStepStoryIndexField for indexed lookup (executeStory steps).
//
// Arguments:
//   - ctx context.Context: propagated to the index lookup.
//   - namespace, name string: the referenced Story's namespace and name.
//
// Returns:
//   - []reconcile.Request: requests for Stories referencing the Story.
func (r *StoryReconciler) enqueueStoriesReferencingStory(ctx context.Context, namespace, name string) []reconcile.Request {
	indexKey := refs.NamespacedKey(namespace, name)
	return r.enqueueStoriesByIndex(ctx, storyStepStoryIndexField, indexKey,
		"Failed to list Stories referencing Story",
		"namespace", namespace, "name", name)
}

// enqueueStoriesByIndex lists Stories using the provided field index value and returns
// reconcile requests for each match, logging failures with the supplied context fields.
//
// Behavior:
//   - Delegates to refs.EnqueueByField for indexed lookup.
//   - Logs errors with provided context fields.
//   - Returns nil on lookup failure (watch event is effectively dropped).
//
// Arguments:
//   - ctx context.Context: propagated to the index lookup.
//   - field string: the index field to query.
//   - indexKey string: the value to match in the index.
//   - logMsg string: message to log on failure.
//   - logFields ...any: additional fields for the log message.
//
// Returns:
//   - []reconcile.Request: requests for matching Stories, or nil on error.
func (r *StoryReconciler) enqueueStoriesByIndex(
	ctx context.Context,
	field string,
	indexKey string,
	logMsg string,
	logFields ...any,
) []reconcile.Request {
	var stories bubuv1alpha1.StoryList
	reqs, err := refs.EnqueueByField(ctx, r.Client, &stories, field, indexKey)
	if err != nil {
		logging.NewControllerLogger(ctx, "story-enqueue").Error(err, logMsg, logFields...)
		return nil
	}
	return reqs
}

// countStoryUsage counts Impulses that reference the Story.
//
// Behavior:
//   - Lists Impulses using the impulseStoryNameIndex field index.
//   - Filters by namespace to ensure cross-namespace references are handled.
//   - Returns the count as int32 for Status.UsageCount.
//
// Arguments:
//   - ctx context.Context: propagated to the index lookup.
//   - story *bubuv1alpha1.Story: the Story to count usage for.
//
// Returns:
//   - int32: the number of Impulses referencing the Story.
//   - error: on list failures.
func (r *StoryReconciler) countStoryUsage(ctx context.Context, story *bubuv1alpha1.Story) (int32, error) {
	var impulses bubuv1alpha1.ImpulseList
	filter := func(obj client.Object) bool {
		impulse, ok := obj.(*bubuv1alpha1.Impulse)
		if !ok || impulse.Spec.StoryRef.Name == "" {
			return false
		}
		targetNamespace := refs.ResolveNamespace(impulse, &impulse.Spec.StoryRef.ObjectReference)
		return targetNamespace == story.Namespace && impulse.Spec.StoryRef.Name == story.Name
	}
	storyKey := refs.NamespacedKey(story.Namespace, story.Name)
	count, err := refs.CountByFieldFiltered(ctx, r.Client, &impulses, impulseStoryNameIndex, storyKey, filter)
	if err != nil {
		return 0, fmt.Errorf("count impulses referencing story: %w", err)
	}
	return int32(count), nil
}

// countStoryTriggers counts StoryRuns referencing the story that have the
// "story" trigger token in their status.
//
// This function reads from Status.TriggerTokens which is set by the StoryRun
// controller during phase transitions, eliminating the need for annotation
// patches that caused churn and conflicts with multiple writers.
//
// Returns:
//   - total: count of StoryRuns with the story trigger token
//   - pending: always false (no backfill needed since StoryRun controller marks tokens)
//   - err: list failure or nil
func (r *StoryReconciler) countStoryTriggersBounded(ctx context.Context, story *bubuv1alpha1.Story, maxMarks int) (total int64, pending bool, err error) {
	var storyRuns runsv1alpha1.StoryRunList
	indexKey := refs.NamespacedKey(story.Namespace, story.Name)
	if err := r.List(ctx, &storyRuns, client.MatchingFields{storyRunStoryIndexField: indexKey}); err != nil {
		return 0, false, err
	}

	// Count StoryRuns that have the story trigger token in their status.
	// The StoryRun controller marks this token when the run starts (PhaseRunning).
	for i := range storyRuns.Items {
		run := &storyRuns.Items[i]
		ns := refs.ResolveNamespace(run, &run.Spec.StoryRef.ObjectReference)
		if run.Spec.StoryRef.Name != story.Name || ns != story.Namespace {
			continue
		}

		if run.Status.HasTriggerToken(contracts.StoryRunTriggerTokenStory) {
			total++
		}
	}

	pending = maxMarks > 0 && len(storyRuns.Items) > maxMarks
	return total, pending, nil
}
