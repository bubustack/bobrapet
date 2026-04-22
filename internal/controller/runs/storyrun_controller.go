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
	stderrors "errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"go.opentelemetry.io/otel/attribute"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/observability"
	rec "github.com/bubustack/bobrapet/pkg/reconcile"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	runslist "github.com/bubustack/bobrapet/pkg/runs/list"
	runsstatus "github.com/bubustack/bobrapet/pkg/runs/status"
	transportutil "github.com/bubustack/bobrapet/pkg/transport"
	"github.com/bubustack/core/contracts"
	bootstrapruntime "github.com/bubustack/core/runtime/bootstrap"
	stagemeta "github.com/bubustack/core/runtime/stage"
	"github.com/bubustack/core/templating"
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
	// storyRunGracefulCancelObservedAnnotation records when the controller first observed
	// a graceful cancellation request so timeout enforcement survives requeues.
	storyRunGracefulCancelObservedAnnotation = "storyrun.bubustack.io/graceful-cancel-observed-at"
	// stepRunCancelRequestedAnnotationKey marks StepRuns that should drain gracefully.
	stepRunCancelRequestedAnnotationKey = "bubustack.io/cancel-requested"
	// stepRunCancelRequestedAnnotationValue is the marker value used for graceful cancel.
	stepRunCancelRequestedAnnotationValue = "true"
	// eventReasonStoryRunRedriveFromStep is emitted when a rerun-from-step is requested.
	eventReasonStoryRunRedriveFromStep = "StoryRunRedriveFromStep"

	defaultGracefulCancelTimeout = 30 * time.Second
)

func storyRunRedriveToken(srun *runsv1alpha1.StoryRun) string {
	if srun == nil {
		return ""
	}
	ann := srun.GetAnnotations()
	if ann == nil {
		return ""
	}
	return strings.TrimSpace(ann[runsidentity.StoryRunRedriveTokenAnnotation])
}

func storyRunRedriveObserved(srun *runsv1alpha1.StoryRun) string {
	if srun == nil {
		return ""
	}
	ann := srun.GetAnnotations()
	if ann == nil {
		return ""
	}
	return strings.TrimSpace(ann[runsidentity.StoryRunRedriveObservedAnnotation])
}

func storyRunRedriveFromStepValue(srun *runsv1alpha1.StoryRun) string {
	if srun == nil {
		return ""
	}
	ann := srun.GetAnnotations()
	if ann == nil {
		return ""
	}
	return strings.TrimSpace(ann[runsidentity.StoryRunRedriveFromStepAnnotation])
}

func storyRunRedriveFromStepObserved(srun *runsv1alpha1.StoryRun) string {
	if srun == nil {
		return ""
	}
	ann := srun.GetAnnotations()
	if ann == nil {
		return ""
	}
	return strings.TrimSpace(ann[runsidentity.StoryRunRedriveFromStepObservedAnnotation])
}

func parseRedriveFromStep(value string) (string, string, bool) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return "", "", false
	}
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	step := strings.TrimSpace(parts[0])
	token := strings.TrimSpace(parts[1])
	if step == "" || token == "" {
		return "", "", false
	}
	return step, token, true
}

func redriveAnnotationChanged(oldObj, newObj client.Object) bool {
	if oldObj == nil || newObj == nil {
		return false
	}
	oldToken := strings.TrimSpace(oldObj.GetAnnotations()[runsidentity.StoryRunRedriveTokenAnnotation])
	newToken := strings.TrimSpace(newObj.GetAnnotations()[runsidentity.StoryRunRedriveTokenAnnotation])
	if newToken == "" {
		return false
	}
	return oldToken != newToken
}

func redriveFromStepAnnotationChanged(oldObj, newObj client.Object) bool {
	if oldObj == nil || newObj == nil {
		return false
	}
	oldVal := strings.TrimSpace(oldObj.GetAnnotations()[runsidentity.StoryRunRedriveFromStepAnnotation])
	newVal := strings.TrimSpace(newObj.GetAnnotations()[runsidentity.StoryRunRedriveFromStepAnnotation])
	if newVal == "" {
		return false
	}
	return oldVal != newVal
}

// StoryRunReconciler reconciles a StoryRun object
type StoryRunReconciler struct {
	config.ControllerDependencies
	rbacManager   *RBACManager
	dagReconciler *DAGReconciler
	Recorder      events.EventRecorder
}

// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;patch;escalate;bind
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams,verbs=get;list;watch;create

// Reconcile is the main entry point for StoryRun reconciliation.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.0/pkg/reconcile
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
//
//nolint:gocyclo // complex by design
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
	if handled, err := r.guardStoryReference(ctx, &srun, log); handled || err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureFinalizer(ctx, &srun, log); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.rbacManager.Reconcile(ctx, &srun); err != nil {
		log.Error(err, "Failed to reconcile RBAC")
		return ctrl.Result{}, err
	}
	if handled, result, err := r.ensureStoryRunCorrelationLabel(ctx, &srun); handled {
		return result, err
	}
	if handled, result, err := r.handleRedriveFromStepIfRequested(ctx, &srun, log); handled {
		return result, err
	}
	if handled, result, err := r.handleRedriveIfRequested(ctx, &srun, log); handled {
		return result, err
	}
	// Check for graceful cancel request
	if srun.Spec.CancelRequested != nil && *srun.Spec.CancelRequested {
		return r.handleGracefulCancel(ctx, &srun)
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

// handleRedriveFromStepIfRequested processes a StoryRun redrive-from-step request by
// deleting affected child StepRuns/StoryRuns and resetting partial status once dependents are gone.
func (r *StoryRunReconciler) handleRedriveFromStepIfRequested(ctx context.Context, srun *runsv1alpha1.StoryRun, log *logging.ControllerLogger) (bool, ctrl.Result, error) {
	if srun == nil {
		return false, ctrl.Result{}, nil
	}
	raw := storyRunRedriveFromStepValue(srun)
	if raw == "" {
		return false, ctrl.Result{}, nil
	}
	if raw == storyRunRedriveFromStepObserved(srun) {
		return false, ctrl.Result{}, nil
	}
	stepName, token, ok := parseRedriveFromStep(raw)
	if !ok {
		return true, ctrl.Result{}, fmt.Errorf("invalid redrive-from-step annotation %q (expected <step-name>:<token>)", raw)
	}
	if log != nil {
		log.Info("StoryRun redrive-from-step requested", "step", stepName, "token", token)
	}

	handled, result, story, err := r.getStoryOrWait(ctx, srun, log)
	if handled || err != nil {
		return true, result, err
	}
	stepSet, err := resolveRedriveFromStepSet(story, stepName)
	if err != nil {
		return true, ctrl.Result{}, err
	}

	remaining, deletedSteps, deletedChildren, err := r.cleanupStoryRunSteps(ctx, srun, stepSet, log)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	if remaining {
		if log != nil {
			log.Info("StoryRun redrive-from-step cleanup in progress",
				"step", stepName,
				"deletedStepRuns", deletedSteps,
				"deletedChildStoryRuns", deletedChildren,
			)
		}
		return true, ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
	}

	if err := r.resetStoryRunForStepRedrive(ctx, srun, stepSet); err != nil {
		return true, ctrl.Result{}, err
	}
	if err := r.patchRedriveFromStepAnnotations(ctx, srun, raw); err != nil {
		return true, ctrl.Result{}, err
	}
	labels := map[string]string{contracts.StepLabelKey: stepName}
	if srun.Spec.StoryRef.Name != "" {
		labels[contracts.StoryLabelKey] = srun.Spec.StoryRef.Name
	}
	r.emitStoryRunLabeledEvent(ctx, srun, eventReasonStoryRunRedriveFromStep, fmt.Sprintf("Requested rerun from step %s", stepName), labels)
	return true, ctrl.Result{Requeue: true}, nil
}

func (r *StoryRunReconciler) ensureStoryRunCorrelationLabel(ctx context.Context, srun *runsv1alpha1.StoryRun) (bool, ctrl.Result, error) {
	if srun == nil {
		return false, ctrl.Result{}, nil
	}
	correlation := correlationIDFromAnnotations(srun)
	if correlation == "" {
		return false, ctrl.Result{}, nil
	}
	labels := srun.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	if !ensureCorrelationLabel(labels, correlation) {
		return false, ctrl.Result{}, nil
	}
	patch := client.MergeFrom(srun.DeepCopy())
	srun.SetLabels(labels)
	if err := r.Patch(ctx, srun, patch); err != nil {
		return true, ctrl.Result{}, err
	}
	return true, ctrl.Result{Requeue: true}, nil
}

// handleRedriveIfRequested processes a StoryRun redrive token by cleaning children
// and resetting status once dependents are gone.
func (r *StoryRunReconciler) handleRedriveIfRequested(ctx context.Context, srun *runsv1alpha1.StoryRun, log *logging.ControllerLogger) (bool, ctrl.Result, error) {
	if srun == nil {
		return false, ctrl.Result{}, nil
	}
	token := storyRunRedriveToken(srun)
	if token == "" {
		return false, ctrl.Result{}, nil
	}
	if token == storyRunRedriveObserved(srun) {
		return false, ctrl.Result{}, nil
	}
	if log != nil {
		log.Info("StoryRun redrive requested", "token", token)
	}

	dependentRemaining, deletedStepRuns, deletedChildStoryRuns, err := r.cleanupStoryRunDependents(ctx, srun, log)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	if dependentRemaining {
		if log != nil {
			log.Info("StoryRun redrive cleanup in progress",
				"deletedStepRuns", deletedStepRuns,
				"deletedChildStoryRuns", deletedChildStoryRuns,
			)
		}
		return true, ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
	}

	if err := r.resetStoryRunForRedrive(ctx, srun); err != nil {
		return true, ctrl.Result{}, err
	}
	if err := r.patchRedriveAnnotations(ctx, srun, token); err != nil {
		return true, ctrl.Result{}, err
	}
	return true, ctrl.Result{Requeue: true}, nil
}

func (r *StoryRunReconciler) resetStoryRunForRedrive(ctx context.Context, srun *runsv1alpha1.StoryRun) error {
	return kubeutil.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		sr.Status.Phase = ""
		sr.Status.Message = ""
		sr.Status.Active = nil
		sr.Status.Completed = nil
		sr.Status.PrimitiveChildren = nil
		sr.Status.StepStates = nil
		sr.Status.Gates = nil
		sr.Status.Error = nil
		sr.Status.Output = nil
		sr.Status.ChildrenCleanedAt = nil
		sr.Status.Duration = ""
		sr.Status.StartedAt = nil
		sr.Status.FinishedAt = nil
		sr.Status.StepsComplete = 0
		sr.Status.StepsFailed = 0
		sr.Status.StepsSkipped = 0
		sr.Status.AllowedFailures = nil
		sr.Status.Conditions = nil
		sr.Status.ObservedGeneration = 0
		// Preserve Attempts and TriggerTokens so redrives remain observable.
	})
}

func (r *StoryRunReconciler) patchRedriveAnnotations(ctx context.Context, srun *runsv1alpha1.StoryRun, token string) error {
	if srun == nil {
		return nil
	}
	if r.dagReconciler != nil {
		return r.dagReconciler.patchStoryRunAnnotations(ctx, srun, func(ann map[string]string) (bool, error) {
			changed := false
			if ann[runsidentity.StoryRunRedriveObservedAnnotation] != token {
				ann[runsidentity.StoryRunRedriveObservedAnnotation] = token
				changed = true
			}
			if _, ok := ann[storyRunGracefulCancelObservedAnnotation]; ok {
				delete(ann, storyRunGracefulCancelObservedAnnotation)
				changed = true
			}
			if _, ok := ann[stepTimersAnnotationKey]; ok {
				delete(ann, stepTimersAnnotationKey)
				changed = true
			}
			return changed, nil
		})
	}
	patch := client.MergeFrom(srun.DeepCopy())
	ann := srun.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	changed := false
	if ann[runsidentity.StoryRunRedriveObservedAnnotation] != token {
		ann[runsidentity.StoryRunRedriveObservedAnnotation] = token
		changed = true
	}
	if _, ok := ann[storyRunGracefulCancelObservedAnnotation]; ok {
		delete(ann, storyRunGracefulCancelObservedAnnotation)
		changed = true
	}
	if _, ok := ann[stepTimersAnnotationKey]; ok {
		delete(ann, stepTimersAnnotationKey)
		changed = true
	}
	if !changed {
		return nil
	}
	srun.SetAnnotations(ann)
	return r.Patch(ctx, srun, patch)
}

func (r *StoryRunReconciler) patchRedriveFromStepAnnotations(ctx context.Context, srun *runsv1alpha1.StoryRun, value string) error {
	if srun == nil {
		return nil
	}
	if r.dagReconciler != nil {
		return r.dagReconciler.patchStoryRunAnnotations(ctx, srun, func(ann map[string]string) (bool, error) {
			changed := false
			if ann[runsidentity.StoryRunRedriveFromStepObservedAnnotation] != value {
				ann[runsidentity.StoryRunRedriveFromStepObservedAnnotation] = value
				changed = true
			}
			if _, ok := ann[storyRunGracefulCancelObservedAnnotation]; ok {
				delete(ann, storyRunGracefulCancelObservedAnnotation)
				changed = true
			}
			if _, ok := ann[stepTimersAnnotationKey]; ok {
				delete(ann, stepTimersAnnotationKey)
				changed = true
			}
			return changed, nil
		})
	}
	patch := client.MergeFrom(srun.DeepCopy())
	ann := srun.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	changed := false
	if ann[runsidentity.StoryRunRedriveFromStepObservedAnnotation] != value {
		ann[runsidentity.StoryRunRedriveFromStepObservedAnnotation] = value
		changed = true
	}
	if _, ok := ann[storyRunGracefulCancelObservedAnnotation]; ok {
		delete(ann, storyRunGracefulCancelObservedAnnotation)
		changed = true
	}
	if _, ok := ann[stepTimersAnnotationKey]; ok {
		delete(ann, stepTimersAnnotationKey)
		changed = true
	}
	if !changed {
		return nil
	}
	srun.SetAnnotations(ann)
	return r.Patch(ctx, srun, patch)
}

func resolveRedriveFromStepSet(story *bubushv1alpha1.Story, stepName string) (map[string]bool, error) {
	if story == nil {
		return nil, fmt.Errorf("story is nil")
	}
	steps, ok := findStepGroup(story, stepName)
	if !ok {
		return nil, fmt.Errorf("step %q not found in story %s", stepName, story.Name)
	}
	_, dependents := buildDependencyGraphs(steps)
	selected := map[string]bool{stepName: true}
	queue := []string{stepName}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for dep := range dependents[current] {
			if selected[dep] {
				continue
			}
			selected[dep] = true
			queue = append(queue, dep)
		}
	}
	return selected, nil
}

func findStepGroup(story *bubushv1alpha1.Story, stepName string) ([]bubushv1alpha1.Step, bool) {
	for i := range story.Spec.Steps {
		if story.Spec.Steps[i].Name == stepName {
			return story.Spec.Steps, true
		}
	}
	for i := range story.Spec.Compensations {
		if story.Spec.Compensations[i].Name == stepName {
			return story.Spec.Compensations, true
		}
	}
	for i := range story.Spec.Finally {
		if story.Spec.Finally[i].Name == stepName {
			return story.Spec.Finally, true
		}
	}
	return nil, false
}

func (r *StoryRunReconciler) cleanupStoryRunSteps(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	steps map[string]bool,
	log *logging.ControllerLogger,
) (bool, int, int, error) {
	if srun == nil || len(steps) == 0 {
		return false, 0, 0, nil
	}
	meta := stagemeta.StoryRunMetadata(srun.Name, srun.Namespace)
	contract := bootstrapruntime.NewContractLogger(log.Logr(), "storyrun-step-cleanup").WithStage(meta)
	contract.Start("cleanup")

	stepSummary, err := r.deleteStepRunsForSteps(ctx, srun, steps, meta, log, contract)
	if err != nil {
		return true, stepSummary.deleted, 0, err
	}
	primitiveSummary, err := r.deletePrimitiveChildStepRunsForSteps(ctx, srun, steps, meta, log, contract)
	if err != nil {
		return true, stepSummary.deleted + primitiveSummary.deleted, 0, err
	}
	storySummary, err := r.deleteChildStoryRunsForSteps(ctx, srun, steps, meta, log, contract)
	if err != nil {
		return true, stepSummary.deleted + primitiveSummary.deleted, storySummary.deleted, err
	}

	deletedStepRuns := stepSummary.deleted + primitiveSummary.deleted
	r.recordDependentMetrics(srun, meta, log, deletedStepRuns, storySummary.deleted)
	remaining := stepSummary.remaining || primitiveSummary.remaining || storySummary.remaining
	contract.Success("cleanup",
		"stepRuns", deletedStepRuns,
		"childStoryRuns", storySummary.deleted,
		"dependentsRemaining", remaining,
	)
	return remaining, deletedStepRuns, storySummary.deleted, nil
}

func (r *StoryRunReconciler) deleteStepRunsForSteps(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	steps map[string]bool,
	meta stagemeta.Metadata,
	log *logging.ControllerLogger,
	contract bootstrapruntime.ContractLogger,
) (cleanupSummary, error) {
	var stepRunList runsv1alpha1.StepRunList
	if err := runslist.StepRunsByStoryRun(ctx, r.Client, srun.Namespace, srun.Name, nil, &stepRunList); err != nil {
		meta.Error(log.Logr(), err, "Failed to list StepRuns for redrive-from-step")
		contract.Failure("cleanup", err)
		return cleanupSummary{remaining: true}, err
	}
	summary := cleanupSummary{}
	for i := range stepRunList.Items {
		sr := &stepRunList.Items[i]
		if !steps[sr.Spec.StepID] {
			continue
		}
		summary.remaining = true
		if sr.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, sr, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
				meta.Error(log.Logr(), err, "Failed to delete StepRun during redrive-from-step", "stepRun", sr.Name)
				contract.Failure("cleanup", err, "phase", "delete-step", "stepRun", sr.Name)
				return summary, err
			}
			meta.Info(log.Logr(), "Deleted StepRun for redrive-from-step", "stepRun", sr.Name)
			summary.deleted++
		}
	}
	return summary, nil
}

func (r *StoryRunReconciler) deleteChildStoryRunsForSteps(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	steps map[string]bool,
	meta stagemeta.Metadata,
	log *logging.ControllerLogger,
	contract bootstrapruntime.ContractLogger,
) (cleanupSummary, error) {
	if srun == nil || len(steps) == 0 {
		return cleanupSummary{}, nil
	}

	var childStoryRuns runsv1alpha1.StoryRunList
	if err := kubeutil.ListByLabels(ctx, r.Client, srun.Namespace, map[string]string{contracts.ParentStoryRunLabel: srun.Name}, &childStoryRuns); err != nil {
		meta.Error(log.Logr(), err, "Failed to list child StoryRuns for redrive-from-step")
		contract.Failure("cleanup", err, "phase", "list-storyruns")
		return cleanupSummary{remaining: true}, err
	}

	summary := cleanupSummary{}
	for i := range childStoryRuns.Items {
		child := &childStoryRuns.Items[i]
		if child.UID == srun.UID {
			continue
		}
		stepName := strings.TrimSpace(child.Labels[contracts.ParentStepLabel])
		if !steps[stepName] {
			continue
		}
		summary.remaining = true
		if !child.DeletionTimestamp.IsZero() {
			continue
		}
		if err := r.Delete(ctx, child, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
			meta.Error(log.Logr(), err, "Failed to delete child StoryRun during redrive-from-step", "childStoryRun", child.Name)
			contract.Failure("cleanup", err, "phase", "delete-storyrun", "childStoryRun", child.Name)
			return summary, err
		}
		meta.Info(log.Logr(), "Deleted child StoryRun for redrive-from-step", "childStoryRun", child.Name)
		summary.deleted++
	}
	return summary, nil
}

func (r *StoryRunReconciler) deletePrimitiveChildStepRunsForSteps(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	steps map[string]bool,
	meta stagemeta.Metadata,
	log *logging.ControllerLogger,
	contract bootstrapruntime.ContractLogger,
) (cleanupSummary, error) {
	summary := cleanupSummary{}
	if srun == nil || len(steps) == 0 || len(srun.Status.PrimitiveChildren) == 0 {
		return summary, nil
	}
	for stepName, childNames := range srun.Status.PrimitiveChildren {
		if !steps[stepName] {
			continue
		}
		for _, childName := range childNames {
			if strings.TrimSpace(childName) == "" {
				continue
			}
			var child runsv1alpha1.StepRun
			key := types.NamespacedName{Namespace: srun.Namespace, Name: childName}
			if err := r.Get(ctx, key, &child); err != nil {
				if errors.IsNotFound(err) {
					continue
				}
				meta.Error(log.Logr(), err, "Failed to fetch primitive child StepRun during redrive-from-step", "stepRun", childName)
				contract.Failure("cleanup", err, "phase", "get-step", "stepRun", childName)
				return cleanupSummary{remaining: true}, err
			}
			summary.remaining = true
			if !child.DeletionTimestamp.IsZero() {
				continue
			}
			if err := r.Delete(ctx, &child, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
				meta.Error(log.Logr(), err, "Failed to delete primitive child StepRun during redrive-from-step", "stepRun", child.Name)
				contract.Failure("cleanup", err, "phase", "delete-step", "stepRun", child.Name)
				return summary, err
			}
			meta.Info(log.Logr(), "Deleted primitive child StepRun for redrive-from-step", "stepRun", child.Name)
			summary.deleted++
		}
	}
	return summary, nil
}

func (r *StoryRunReconciler) resetStoryRunForStepRedrive(ctx context.Context, srun *runsv1alpha1.StoryRun, steps map[string]bool) error {
	return kubeutil.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		if len(steps) == 0 {
			return
		}
		if sr.Status.StepStates != nil {
			for step := range steps {
				delete(sr.Status.StepStates, step)
			}
			if len(sr.Status.StepStates) == 0 {
				sr.Status.StepStates = nil
			}
		}
		if sr.Status.Gates != nil {
			for step := range steps {
				delete(sr.Status.Gates, step)
			}
			if len(sr.Status.Gates) == 0 {
				sr.Status.Gates = nil
			}
		}
		if sr.Status.PrimitiveChildren != nil {
			for step := range steps {
				delete(sr.Status.PrimitiveChildren, step)
			}
			if len(sr.Status.PrimitiveChildren) == 0 {
				sr.Status.PrimitiveChildren = nil
			}
		}
		sr.Status.Active = filterStepNames(sr.Status.Active, steps)
		sr.Status.Completed = filterStepNames(sr.Status.Completed, steps)
		sr.Status.AllowedFailures = filterStepNames(sr.Status.AllowedFailures, steps)
		sr.Status.Error = nil
		sr.Status.Output = nil
		sr.Status.Message = ""
		sr.Status.Conditions = nil
		sr.Status.ChildrenCleanedAt = nil
		sr.Status.FinishedAt = nil
		sr.Status.Duration = ""
		if sr.Status.Phase.IsTerminal() {
			sr.Status.Phase = ""
			sr.Status.Active = nil
			sr.Status.Completed = nil
		}
		if r.dagReconciler != nil {
			r.dagReconciler.updateStepProgress(sr)
		}
	})
}

func filterStepNames(values []string, drop map[string]bool) []string {
	if len(values) == 0 || len(drop) == 0 {
		return values
	}
	out := make([]string, 0, len(values))
	for _, v := range values {
		if drop[v] {
			continue
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (r *StoryRunReconciler) emitStoryRunLabeledEvent(ctx context.Context, srun *runsv1alpha1.StoryRun, reason, message string, labels map[string]string) {
	if srun == nil {
		return
	}
	eventLabels := runsidentity.SelectorLabels(srun.Name)
	if correlation := correlationIDFromAnnotations(srun); correlation != "" {
		ensureCorrelationLabel(eventLabels, correlation)
	}
	for k, v := range labels {
		if strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
			continue
		}
		eventLabels[k] = v
	}
	now := metav1.Now()
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-", srun.Name),
			Namespace:    srun.Namespace,
			Labels:       eventLabels,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:       "StoryRun",
			Namespace:  srun.Namespace,
			Name:       srun.Name,
			UID:        srun.UID,
			APIVersion: runsv1alpha1.GroupVersion.String(),
		},
		Reason:         reason,
		Message:        message,
		Type:           corev1.EventTypeNormal,
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
		Source: corev1.EventSource{
			Component: "storyrun-controller",
		},
	}
	if err := r.Create(ctx, ev); err != nil && !errors.IsAlreadyExists(err) {
		if log := logging.NewReconcileLogger(ctx, "storyrun-event"); log != nil {
			log.Error(err, "Failed to emit labeled StoryRun event", "reason", reason)
		}
	}
}

func (r *StoryRunReconciler) guardStoryReference(ctx context.Context, srun *runsv1alpha1.StoryRun, log *logging.ControllerLogger) (bool, error) {
	if srun == nil {
		return false, nil
	}

	var cfg *config.ControllerConfig
	if r != nil && r.ConfigResolver != nil {
		if resolved := r.ConfigResolver.GetOperatorConfig(); resolved != nil {
			cfg = &resolved.Controller
		}
	}

	storyKey := srun.Spec.StoryRef.ToNamespacedName(srun)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		r.Client,
		cfg,
		srun,
		"runs.bubustack.io",
		"StoryRun",
		"bubustack.io",
		"Story",
		storyKey.Namespace,
		storyKey.Name,
		"StoryRef",
	); err != nil {
		message := fmt.Sprintf("StoryRef rejected by cross-namespace policy: %v", err)
		if log != nil {
			log.Info("Blocking StoryRun due to StoryRef policy", "error", err.Error())
		}
		if r.Recorder != nil {
			r.Recorder.Eventf(srun, nil, "Warning", conditions.ReasonStoryReferenceInvalid, "Reconcile", message)
		}
		if err := r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, conditions.ReasonStoryReferenceInvalid, message); err != nil {
			return true, err
		}
		return true, nil
	}

	return false, nil
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
//
//nolint:gocyclo // complex by design
func (r *StoryRunReconciler) reconcileAfterSetup(ctx context.Context, srun *runsv1alpha1.StoryRun, log *logging.ControllerLogger) (ctrl.Result, error) {
	var story *bubushv1alpha1.Story
	handled, result, s, err := r.getStoryOrWait(ctx, srun, log)
	if handled || err != nil {
		return result, err
	}
	story = s

	if err := r.ensureStoryRunSchemaRefs(ctx, srun, story); err != nil {
		log.Error(err, "Failed to update StoryRun schema references")
		return ctrl.Result{}, err
	}

	if _, err := r.ensureStoryRunSchedulingLabels(ctx, srun, story, log); err != nil {
		return ctrl.Result{}, err
	}

	if handled, err := r.validateStoryRunInputs(ctx, srun, story, log); handled || err != nil {
		return ctrl.Result{}, err
	}
	if handled, err := r.initializePhaseIfEmpty(ctx, srun); handled || err != nil {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	result, err = r.dagReconciler.Reconcile(ctx, srun, story)
	if err != nil {
		var blocked *templating.ErrEvaluationBlocked
		if stderrors.As(err, &blocked) {
			if log != nil {
				log.V(1).Info("Template evaluation blocked; requeueing", "reason", blocked.Reason)
			}
			if result.RequeueAfter == 0 {
				result.RequeueAfter = r.nextRequeueDelay()
			}
			return result, nil
		}
		var offloaded *templating.ErrOffloadedDataUsage
		if stderrors.As(err, &offloaded) {
			policy := resolveOffloadedPolicy(r.ConfigResolver)
			if shouldBlockOffloaded(policy) || shouldResolveAllOffloaded(policy) {
				if log != nil {
					log.Info("Template evaluation requires offloaded data; requeueing", "reason", offloaded.Reason)
				}
				if result.RequeueAfter == 0 {
					result.RequeueAfter = r.nextRequeueDelay()
				}
				return result, nil
			}
		}
	}
	return result, err
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
	maxBytes := DefaultMaxInlineInputsSize
	if r != nil && r.ConfigResolver != nil {
		if operatorCfg := r.ConfigResolver.GetOperatorConfig(); operatorCfg != nil && operatorCfg.Controller.StoryRun.MaxInlineInputsSize > 0 {
			maxBytes = operatorCfg.Controller.StoryRun.MaxInlineInputsSize
		}
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxInlineInputsSize
	}
	if srun.Spec.Inputs != nil && len(srun.Spec.Inputs.Raw) > maxBytes {
		if srun.Status.Phase != enums.PhaseFailed {
			log.Info("StoryRun spec.inputs is too large, failing run", "size", len(srun.Spec.Inputs.Raw), "maxSize", maxBytes)
			failMsg := fmt.Sprintf("spec.inputs size %d bytes exceeds maximum of %d", len(srun.Spec.Inputs.Raw), maxBytes)
			if err := r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, conditions.ReasonInputTooLarge, failMsg); err != nil {
				log.Error(err, "Failed to set StoryRun status to Failed due to oversized inputs")
				return true, err
			}
		}
		return true, nil
	}
	return false, nil
}

// validateStoryRunInputs validates resolved inputs against the Story's inputsSchema.
//
// Behavior:
//   - Resolves inputs with schema defaults.
//   - Validates against inputsSchema when present.
//   - Fails the StoryRun on validation errors.
//
// Returns:
//   - bool: true if handled (failed or errored).
//   - error: non-nil on patch failures.
func (r *StoryRunReconciler) validateStoryRunInputs(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubushv1alpha1.Story, log *logging.ControllerLogger) (bool, error) {
	if srun == nil || story == nil || story.Spec.InputsSchema == nil || len(story.Spec.InputsSchema.Raw) == 0 {
		return false, nil
	}
	ctx = withStoryMaxRecursionDepth(ctx, r.ConfigResolver, story)
	resolved, err := resolveHydratedStoryRunInputs(ctx, srun, story)
	if err != nil {
		log.Error(err, "Failed to resolve StoryRun inputs")
		if phaseErr := r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, conditions.ReasonValidationFailed, fmt.Sprintf("Failed to resolve StoryRun inputs: %v", err)); phaseErr != nil {
			return true, phaseErr
		}
		return true, nil
	}
	raw, err := json.Marshal(resolved)
	if err != nil {
		log.Error(err, "Failed to marshal resolved StoryRun inputs")
		if phaseErr := r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, conditions.ReasonValidationFailed, fmt.Sprintf("Failed to marshal StoryRun inputs: %v", err)); phaseErr != nil {
			return true, phaseErr
		}
		return true, nil
	}
	if err := validateJSONInputsBytes(raw, story.Spec.InputsSchema, "Story inputs"); err != nil {
		log.Error(err, "StoryRun inputs failed schema validation")
		if phaseErr := r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, conditions.ReasonInputSchemaFailed, fmt.Sprintf("StoryRun inputs failed schema validation: %v", err)); phaseErr != nil {
			return true, phaseErr
		}
		return true, nil
	}
	return false, nil
}

func (r *StoryRunReconciler) ensureStoryRunSchemaRefs(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubushv1alpha1.Story) error {
	if srun == nil || story == nil {
		return nil
	}

	version := resolveSchemaVersion(srun.Spec.StoryRef.Version, story.Spec.Version)
	var inputRef *runsv1alpha1.SchemaReference
	if story.Spec.InputsSchema != nil && len(story.Spec.InputsSchema.Raw) > 0 {
		inputRef = storySchemaRef(story.Namespace, story.Name, version, "inputs")
	}
	var outputRef *runsv1alpha1.SchemaReference
	if story.Spec.OutputsSchema != nil && len(story.Spec.OutputsSchema.Raw) > 0 {
		outputRef = storySchemaRef(story.Namespace, story.Name, version, "output")
	}

	if schemaRefEqual(srun.Status.InputSchemaRef, inputRef) && schemaRefEqual(srun.Status.OutputSchemaRef, outputRef) {
		return nil
	}

	return kubeutil.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		sr.Status.InputSchemaRef = inputRef
		sr.Status.OutputSchemaRef = outputRef
	})
}

func (r *StoryRunReconciler) ensureStoryRunSchedulingLabels(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubushv1alpha1.Story, log *logging.ControllerLogger) (bool, error) {
	if srun == nil {
		return false, nil
	}
	before := srun.DeepCopy()
	if srun.Labels == nil {
		srun.Labels = map[string]string{}
	}
	scheduling := resolveSchedulingDecision(story, r.ConfigResolver, nil)
	changed := ensureSchedulingLabels(srun.Labels, scheduling)
	if !changed {
		return false, nil
	}
	if err := r.Patch(ctx, srun, client.MergeFrom(before)); err != nil {
		if log != nil {
			log.Error(err, "Failed to patch StoryRun scheduling labels")
		}
		return true, err
	}
	if log != nil {
		log.V(1).Info("Patched StoryRun scheduling labels",
			"queue", srun.Labels[contracts.QueueLabelKey],
			"priority", srun.Labels[contracts.QueuePriorityLabelKey],
		)
	}
	return true, nil
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
			r.Recorder.Eventf(srun, nil, "Warning", conditions.ReasonStoryNotFound, "Reconcile", "Waiting for Story '%s'", srun.Spec.StoryRef.ToNamespacedName(srun).String())
		}
		statusErr := kubeutil.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
			sr := obj.(*runsv1alpha1.StoryRun)
			if sr.Status.Phase == "" {
				sr.Status.Phase = enums.PhasePending
			}
			cm := conditions.NewConditionManager(sr.Generation)
			message := fmt.Sprintf("Waiting for Story '%s' to be created", srun.Spec.StoryRef.ToNamespacedName(srun).String())
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonStoryNotFound, message)
			cm.SetProgressingCondition(&sr.Status.Conditions, true, conditions.ReasonStoryNotFound, message)
			cm.SetDegradedCondition(&sr.Status.Conditions, false, conditions.ReasonStoryNotFound, message)
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
	if err := r.setStoryRunPhase(ctx, srun, enums.PhaseRunning, conditions.ReasonStartingExecution, "Starting StoryRun execution"); err != nil {
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
func (r *StoryRunReconciler) setStoryRunPhase(ctx context.Context, srun *runsv1alpha1.StoryRun, phase enums.Phase, reason, message string) error {
	return runsstatus.PatchStoryRunPhase(ctx, r.Client, srun, phase, reason, message)
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
// handleGracefulCancel processes a graceful cancel request on a StoryRun.
// It annotates all non-terminal StepRuns with "bubustack.io/cancel-requested"
// so SDKs can drain in-flight work. Once the configured drain window expires,
// it force-deletes any remaining non-terminal StepRuns so the existing StepRun
// deletion cleanup path can tear down realtime runtime resources.
func (r *StoryRunReconciler) handleGracefulCancel(ctx context.Context, srun *runsv1alpha1.StoryRun) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun").WithValues("storyrun", srun.Name)

	story, err := r.getStoryOptional(ctx, srun)
	if err != nil {
		return ctrl.Result{}, err
	}
	startedAt, err := r.ensureGracefulCancelObservedAt(ctx, srun)
	if err != nil {
		return ctrl.Result{}, err
	}
	timeout := r.resolveGracefulCancelTimeout(ctx, story, log)
	now := time.Now()
	forceDelete := timeout <= 0 || !now.Before(startedAt.Add(timeout))

	// 1. List all StepRuns belonging to this StoryRun
	stepRunList := &runsv1alpha1.StepRunList{}
	if err := runslist.StepRunsByStoryRun(ctx, r.Client, srun.Namespace, srun.Name, nil, stepRunList); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Annotate all non-terminal StepRuns with cancel signal
	allTerminal := true
	deletedCount := 0
	for i := range stepRunList.Items {
		sr := &stepRunList.Items[i]
		terminal, deleted, err := r.processGracefulCancelStepRun(ctx, sr, forceDelete, log)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !terminal {
			allTerminal = false
		}
		if deleted {
			deletedCount++
		}
	}

	// 3. If all steps are terminal, set StoryRun to Finished
	if allTerminal {
		if srun.Status.Phase != enums.PhaseFinished {
			if err := r.setStoryRunPhase(ctx, srun, enums.PhaseFinished, conditions.ReasonCanceled, "StoryRun gracefully canceled"); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// 4. Otherwise, keep draining until the timeout expires, then wait for deletions to finish.
	if forceDelete {
		if deletedCount > 0 {
			log.Info("Graceful cancel timeout expired; deleting remaining StepRuns",
				"remainingStepRuns", deletedCount,
				"timeout", timeout.String(),
				"startedAt", startedAt.Format(time.RFC3339Nano),
			)
		}
		return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
	}
	remaining := startedAt.Add(timeout).Sub(now)
	return ctrl.Result{RequeueAfter: min(r.nextRequeueDelay(), remaining)}, nil
}

func (r *StoryRunReconciler) processGracefulCancelStepRun(
	ctx context.Context,
	sr *runsv1alpha1.StepRun,
	forceDelete bool,
	log *logging.ControllerLogger,
) (bool, bool, error) {
	if sr == nil || sr.Status.Phase.IsTerminal() {
		return true, false, nil
	}
	r.annotateStepRunForGracefulCancel(ctx, sr, log)
	if !forceDelete || !sr.DeletionTimestamp.IsZero() {
		return false, false, nil
	}
	if err := r.deleteStepRunForGracefulCancel(ctx, sr, log); err != nil {
		return false, false, err
	}
	return false, true, nil
}

func (r *StoryRunReconciler) annotateStepRunForGracefulCancel(
	ctx context.Context,
	sr *runsv1alpha1.StepRun,
	log *logging.ControllerLogger,
) {
	if sr == nil {
		return
	}
	if sr.Annotations != nil && sr.Annotations[stepRunCancelRequestedAnnotationKey] == stepRunCancelRequestedAnnotationValue {
		return
	}
	patch := client.MergeFrom(sr.DeepCopy())
	if sr.Annotations == nil {
		sr.Annotations = map[string]string{}
	}
	sr.Annotations[stepRunCancelRequestedAnnotationKey] = stepRunCancelRequestedAnnotationValue
	if err := r.Patch(ctx, sr, patch); err != nil {
		if log != nil {
			log.Error(err, "Failed to annotate StepRun with cancel", "steprun", sr.Name)
		}
	}
}

func (r *StoryRunReconciler) deleteStepRunForGracefulCancel(
	ctx context.Context,
	sr *runsv1alpha1.StepRun,
	log *logging.ControllerLogger,
) error {
	if sr == nil {
		return nil
	}
	if err := r.Delete(ctx, sr, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
		if log != nil {
			log.Error(err, "Failed to delete StepRun after graceful cancel timeout", "steprun", sr.Name)
		}
		return err
	}
	return nil
}

func gracefulCancelObservedAt(srun *runsv1alpha1.StoryRun) (time.Time, bool) {
	if srun == nil {
		return time.Time{}, false
	}
	raw := strings.TrimSpace(srun.GetAnnotations()[storyRunGracefulCancelObservedAnnotation])
	if raw == "" {
		return time.Time{}, false
	}
	timestamp, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || timestamp.IsZero() {
		return time.Time{}, false
	}
	return timestamp, true
}

func (r *StoryRunReconciler) ensureGracefulCancelObservedAt(ctx context.Context, srun *runsv1alpha1.StoryRun) (time.Time, error) {
	if timestamp, ok := gracefulCancelObservedAt(srun); ok {
		return timestamp, nil
	}
	now := time.Now().UTC()
	patch := client.MergeFrom(srun.DeepCopy())
	ann := srun.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[storyRunGracefulCancelObservedAnnotation] = now.Format(time.RFC3339Nano)
	srun.SetAnnotations(ann)
	if err := r.Patch(ctx, srun, patch); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

func (r *StoryRunReconciler) resolveGracefulCancelTimeout(
	ctx context.Context,
	story *bubushv1alpha1.Story,
	log *logging.ControllerLogger,
) time.Duration {
	timeout := defaultGracefulCancelTimeout
	if r != nil && r.ConfigResolver != nil {
		if cfg := r.ConfigResolver.GetOperatorConfig(); cfg != nil && cfg.Controller.Engram.EngramControllerConfig.DefaultGracefulShutdownTimeoutSeconds > 0 {
			timeout = time.Duration(cfg.Controller.Engram.EngramControllerConfig.DefaultGracefulShutdownTimeoutSeconds) * time.Second
		}
	}
	if story == nil {
		return timeout
	}
	if story.Spec.Policy != nil && story.Spec.Policy.Timeouts != nil && story.Spec.Policy.Timeouts.GracefulShutdownTimeout != nil {
		if configured := story.Spec.Policy.Timeouts.GracefulShutdownTimeout.Duration; configured > 0 {
			timeout = configured
		}
	}
	if story.Spec.Pattern != enums.RealtimePattern {
		return timeout
	}
	if drain := r.resolveStoryTransportDrainTimeout(ctx, story, log); drain > 0 {
		timeout = drain
	}
	return timeout
}

func (r *StoryRunReconciler) resolveStoryTransportDrainTimeout(
	ctx context.Context,
	story *bubushv1alpha1.Story,
	log *logging.ControllerLogger,
) time.Duration {
	if story == nil {
		return 0
	}
	maxDrain := time.Duration(0)
	for i := range story.Spec.Transports {
		drain, ok := r.resolveTransportDrainTimeout(ctx, &story.Spec.Transports[i], log)
		if ok && drain > maxDrain {
			maxDrain = drain
		}
	}
	return maxDrain
}

func (r *StoryRunReconciler) resolveTransportDrainTimeout(
	ctx context.Context,
	decl *bubushv1alpha1.StoryTransport,
	log *logging.ControllerLogger,
) (time.Duration, bool) {
	if decl == nil {
		return 0, false
	}
	storySettings, err := transportutil.MergeSettingsWithStreaming(decl.Settings, decl.Streaming)
	if err != nil {
		if log != nil {
			log.Error(err, "Failed to encode story transport settings while resolving graceful cancel timeout", "transport", decl.Name)
		}
		return 0, false
	}

	baseSettings := r.loadTransportDefaultSettings(ctx, decl, log)

	mergedSettings, err := transportutil.MergeSettings(baseSettings, storySettings)
	if err != nil {
		if log != nil {
			log.Error(err, "Failed to merge transport settings while resolving graceful cancel timeout", "transport", decl.Name)
		}
		return 0, false
	}
	if len(mergedSettings) == 0 {
		return 0, false
	}
	return decodeTransportDrainTimeout(mergedSettings, decl.Name, log)
}

func (r *StoryRunReconciler) loadTransportDefaultSettings(
	ctx context.Context,
	decl *bubushv1alpha1.StoryTransport,
	log *logging.ControllerLogger,
) *runtime.RawExtension {
	if decl == nil {
		return nil
	}
	ref := strings.TrimSpace(decl.TransportRef)
	if ref == "" {
		return nil
	}

	var transport transportv1alpha1.Transport
	if err := r.Get(ctx, types.NamespacedName{Name: ref}, &transport); err != nil {
		if log != nil && !errors.IsNotFound(err) {
			log.Error(err, "Failed to load transport defaults while resolving graceful cancel timeout", "transport", decl.Name, "transportRef", ref)
		}
		return nil
	}

	baseSettings, err := transportutil.MergeSettingsWithStreaming(transport.Spec.DefaultSettings, transport.Spec.Streaming)
	if err != nil {
		if log != nil {
			log.Error(err, "Failed to encode transport defaults while resolving graceful cancel timeout", "transport", decl.Name, "transportRef", ref)
		}
		return nil
	}
	return baseSettings
}

func decodeTransportDrainTimeout(
	settingsBytes []byte,
	transportName string,
	log *logging.ControllerLogger,
) (time.Duration, bool) {
	var settings transportv1alpha1.TransportStreamingSettings
	if err := json.Unmarshal(settingsBytes, &settings); err != nil {
		if log != nil {
			log.Error(err, "Failed to decode transport settings while resolving graceful cancel timeout", "transport", transportName)
		}
		return 0, false
	}
	if settings.Lifecycle == nil || settings.Lifecycle.DrainTimeoutSeconds == nil {
		return 0, false
	}
	return time.Duration(*settings.Lifecycle.DrainTimeoutSeconds) * time.Second, true
}

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
	isStreaming := story != nil && story.Spec.Pattern == enums.RealtimePattern

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
		if isStreaming && cfg.Controller.RealtimeTTLSecondsAfterFinished != 0 {
			return cfg.Controller.RealtimeTTLSecondsAfterFinished
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
	r.Recorder = mgr.GetEventRecorder("storyrun-controller")
	r.rbacManager = NewRBACManager(mgr.GetClient(), mgr.GetScheme())
	r.rbacManager.Recorder = r.Recorder
	stepExecutor := NewStepExecutor(mgr.GetClient(), mgr.GetScheme(), r.TemplateEvaluator, r.ConfigResolver, r.Recorder)
	r.dagReconciler = NewDAGReconciler(DAGReconcilerDeps{
		Client:            mgr.GetClient(),
		TemplateEvaluator: r.TemplateEvaluator,
		StepExecutor:      stepExecutor,
		ConfigResolver:    r.ConfigResolver,
	})

	storyRunPredicate := predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}
			if e.ObjectOld.GetDeletionTimestamp() == nil && e.ObjectNew.GetDeletionTimestamp() != nil {
				return true
			}
			return redriveAnnotationChanged(e.ObjectOld, e.ObjectNew) || redriveFromStepAnnotationChanged(e.ObjectOld, e.ObjectNew)
		},
	}
	stepRunPredicate := predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetResourceVersion() != e.ObjectNew.GetResourceVersion()
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StoryRun{}, builder.WithPredicates(storyRunPredicate)).
		WithOptions(opts).
		Owns(&runsv1alpha1.StepRun{}, builder.WithPredicates(stepRunPredicate)).
		Complete(r)
}
