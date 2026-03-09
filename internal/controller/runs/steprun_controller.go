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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/attribute"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/observability"
	"github.com/bubustack/bobrapet/pkg/podspec"
	rec "github.com/bubustack/bobrapet/pkg/reconcile"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	runsinputs "github.com/bubustack/bobrapet/pkg/runs/inputs"
	runstatus "github.com/bubustack/bobrapet/pkg/runs/status"
	runsvalidation "github.com/bubustack/bobrapet/pkg/runs/validation"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bobrapet/pkg/templatesafety"
	transportutil "github.com/bubustack/bobrapet/pkg/transport"
	transportbinding "github.com/bubustack/bobrapet/pkg/transport/binding"
	"github.com/bubustack/bobrapet/pkg/workload"
	"github.com/bubustack/core/contracts"
	coreenv "github.com/bubustack/core/runtime/env"
	coretransport "github.com/bubustack/core/runtime/transport"
	"github.com/bubustack/core/templating"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
)

const (
	// StepRunFinalizer is the finalizer for StepRun resources
	StepRunFinalizer = "steprun.bubustack.io/finalizer"

	eventReasonJobPatched                            = "JobStatusPatched"
	eventReasonJobFallback                           = "JobStatusFallback"
	eventReasonJobSucceeded                          = "JobCompleted"
	eventReasonJobRetryScheduled                     = "JobRetryScheduled"
	eventReasonJobRestarted                          = "JobRestarted"
	eventReasonTransportBindingAnnotated             = "TransportBindingAnnotated"
	eventReasonTransportBindingMergeFailed           = "TransportBindingMergeFailed"
	eventReasonTransportBindingCodecFailed           = "TransportBindingCodecRejected"
	eventReasonDownstreamTargetsRejected             = "DownstreamTargetsRejected"
	eventReasonDownstreamTargetsUpdated              = "DownstreamTargetsUpdated"
	eventReasonDownstreamTargetsCleared              = "DownstreamTargetsCleared"
	eventReasonTransportBindingAnnotationInvalid     = "TransportBindingAnnotationInvalid"
	eventReasonTransportBindingPending               = "TransportBindingPending"
	eventReasonTransportBindingDecodeFailed          = "TransportBindingDecodeFailed"
	eventReasonTransportBindingAnnotationPatchFailed = "TransportBindingAnnotationPatchFailed"
	eventReasonJobTemplateBlocked                    = "JobTemplateBlocked"
	eventReasonTransportConditionUpdated             = "TransportConditionUpdated"
	eventReasonJobCreateTooLarge                     = "JobCreateTooLarge"
	eventReasonOutputKeyMismatch                     = "OutputKeyMismatch"
)

var errTransportBindingPending = fmt.Errorf("transport binding not yet observed in informer cache")
var errStepRunInputSchemaInvalid = errors.New("step input schema validation failed")
var errStepRunRequiresContextNil = errors.New("required context path is nil")
var errStepRunCacheHit = errors.New("step output cache hit")

const (
	cacheAnnotationKey            = "cache.bubustack.io/key"
	cacheAnnotationMode           = "cache.bubustack.io/mode"
	cacheAnnotationTTL            = "cache.bubustack.io/ttl-seconds"
	cacheAnnotationStatus         = "cache.bubustack.io/status"
	stepRunJobUIDAnnotation       = "runs.bubustack.io/job-uid"
	stepRunRestartCountAnnotation = "runs.bubustack.io/restart-count"
	stepRunRestartedAtAnnotation  = "runs.bubustack.io/restarted-at"

	cacheStatusHit    = "hit"
	cacheStatusStored = "stored"
)

const (
	executionModeHybrid    = "hybrid"
	executionModeStreaming = "streaming"
)

type jobResources struct {
	envVars      []corev1.EnvVar
	envFrom      []corev1.EnvFromSource
	volumes      []corev1.Volume
	volumeMounts []corev1.VolumeMount
}

// StepRunReconciler reconciles a StepRun object
type StepRunReconciler struct {
	config.ControllerDependencies
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=pods/log,verbs=get
// +kubebuilder:rbac:groups=transport.bubustack.io,resources=transportbindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=transport.bubustack.io,resources=transportbindings/status,verbs=get;update;patch

// Reconcile is the main entry point for StepRun reconciliation.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.0/pkg/reconcile
//
// Behavior:
//   - Applies optional reconcile timeout from operator config.
//   - Fetches StepRun and ignores NotFound.
//   - Handles deletion if DeletionTimestamp set.
//   - Ensures finalizer is present.
//   - Routes to reconcileNormal for non-terminal phases.
//
// Arguments:
//   - ctx context.Context: for API calls and cancellation.
//   - req ctrl.Request: the reconcile request with StepRun name.
//
// Returns:
//   - ctrl.Result: may include requeue or delay.
//   - error: non-nil on failures.
func (r *StepRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	ctx, span := observability.StartSpan(ctx, "StepRunReconciler.Reconcile",
		attribute.String("namespace", req.Namespace),
		attribute.String("steprun", req.Name),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
		}
		requeue := res.Requeue || res.RequeueAfter > 0
		span.SetAttributes(
			attribute.Bool("requeue", requeue),
			attribute.String("requeue_after", res.RequeueAfter.String()),
		)
		span.End()
	}()

	baseLogger := logging.NewReconcileLogger(ctx, "steprun").WithValues("steprun", req.NamespacedName)

	timeout := time.Duration(0)
	if r.ConfigResolver != nil && r.ConfigResolver.GetOperatorConfig() != nil {
		timeout = r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	}
	if timeout > 0 {
		baseLogger.V(1).Info("Applying reconcile timeout", "timeout", timeout.String())
	}

	res, err = rec.Run(ctx, req, rec.RunOptions{
		Controller: "steprun",
		Timeout:    timeout,
	}, rec.RunHooks[*runsv1alpha1.StepRun]{
		Get: func(ctx context.Context, req ctrl.Request) (*runsv1alpha1.StepRun, error) {
			var stepRun runsv1alpha1.StepRun
			if err := r.Get(ctx, req.NamespacedName, &stepRun); err != nil {
				return nil, err
			}
			return &stepRun, nil
		},
		HandleDeletion: func(ctx context.Context, step *runsv1alpha1.StepRun) (bool, ctrl.Result, error) {
			return rec.ShortCircuitDeletion(ctx, step, func(ctx context.Context) (ctrl.Result, error) {
				return r.reconcileDelete(ctx, step)
			})
		},
		EnsureFinalizer: func(ctx context.Context, step *runsv1alpha1.StepRun) error {
			stepLogger := baseLogger.WithStepRun(step)
			added, err := kubeutil.EnsureFinalizer(ctx, r.Client, step, StepRunFinalizer)
			if err != nil {
				stepLogger.Error(err, "Failed to add StepRun finalizer")
				return err
			}
			if added {
				stepLogger.Info("Added StepRun finalizer")
			}
			return nil
		},
		HandleNormal: func(ctx context.Context, step *runsv1alpha1.StepRun) (ctrl.Result, error) {
			stepLogger := baseLogger.WithStepRun(step)
			return rec.RunPipeline(ctx, step, rec.PipelineHooks[*runsv1alpha1.StepRun, struct{}]{
				Prepare: func(ctx context.Context, step *runsv1alpha1.StepRun) (struct{}, *ctrl.Result, error) {
					if handled, res, err := r.ensureStepRunCorrelationLabel(ctx, step); err != nil {
						return struct{}{}, nil, err
					} else if handled {
						return struct{}{}, &res, nil
					}
					if step.Status.Phase.IsTerminal() {
						if _, err := r.cleanupRunScopedRuntime(ctx, step); err != nil {
							stepLogger.Error(err, "Failed to cleanup realtime runtime for terminal StepRun")
							return struct{}{}, nil, err
						}
						return struct{}{}, &ctrl.Result{}, nil
					}
					if step.Status.Phase == "" {
						if err := r.setStepRunPhase(ctx, step, enums.PhasePending, conditions.ReasonPending, "StepRun created and awaiting processing"); err != nil {
							stepLogger.Error(err, "Failed to update StepRun status to Pending")
							return struct{}{}, nil, err
						}
						return struct{}{}, &ctrl.Result{Requeue: true}, nil
					}
					return struct{}{}, nil, nil
				},
				Ensure: func(ctx context.Context, step *runsv1alpha1.StepRun, _ struct{}) (ctrl.Result, error) {
					return r.reconcileNormal(ctx, step)
				},
			})
		},
	})
	return res, err
}

// reconcileNormal handles normal (non-deletion) StepRun reconciliation.
//
// Behavior:
//   - Fetches referenced Engram and EngramTemplate.
//   - Determines execution mode (job, deployment, statefulset).
//   - Routes to appropriate execution handler.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to reconcile.
//
// Returns:
//   - ctrl.Result: reconcile result with possible requeue.
//   - error: non-nil on failures.
func (r *StepRunReconciler) reconcileNormal(ctx context.Context, step *runsv1alpha1.StepRun) (ctrl.Result, error) {
	stepLogger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step)

	if handled, err := r.guardStepRunReferences(ctx, step, stepLogger); handled || err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureStoryNameLabel(ctx, step, stepLogger); err != nil {
		return ctrl.Result{}, err
	}

	if isMaterializeStepRun(step) && step.Spec.EngramRef != nil {
		if err := ensureMaterializeEngram(ctx, r.Client, step.Namespace, step.Spec.EngramRef.Name); err != nil {
			stepLogger.Error(err, "Failed to ensure materialize engram")
			return ctrl.Result{}, err
		}
	}

	engram, err := r.getEngramForStep(ctx, step)
	if err != nil {
		if apierrors.IsNotFound(err) {
			stepLogger.Info("Engram referenced by StepRun not found, setting phase to Blocked")
			// Emit event for user visibility (guard recorder for tests)
			if r.Recorder != nil {
				r.Recorder.Event(step, "Warning", conditions.ReasonReferenceNotFound, fmt.Sprintf("Engram '%s' not found", step.Spec.EngramRef.Name))
			}
			err := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, conditions.ReasonReferenceNotFound, fmt.Sprintf("Engram '%s' not found", step.Spec.EngramRef.Name))
			return ctrl.Result{}, err // Stop reconciliation, wait for watch to trigger
		}
		stepLogger.Error(err, "Failed to get Engram for StepRun")
		return ctrl.Result{}, err
	}

	// Default to "job" mode if not specified
	mode := enums.WorkloadModeJob
	if engram != nil && engram.Spec.Mode != "" {
		mode = engram.Spec.Mode
	}
	if override := materializeModeOverride(step); override != "" {
		mode = enums.WorkloadMode(override)
	}

	stepLogger.Info("Determined execution mode", "mode", mode)

	switch mode {
	case enums.WorkloadModeDeployment, enums.WorkloadModeStatefulSet:
		story, err := r.getStoryForStep(ctx, step)
		if err != nil {
			stepLogger.Error(err, "Failed to load Story for realtime StepRun")
			return ctrl.Result{}, err
		}
		stepSpec := findStoryStep(story, step.Spec.StepID)
		if stepSpec == nil {
			if isMaterializeStepRun(step) {
				stepSpec = &v1alpha1.Step{Name: step.Spec.StepID}
			} else {
				err := fmt.Errorf("story %s does not define step %s", story.Name, step.Spec.StepID)
				stepLogger.Error(err, "Streaming StepRun references missing step")
				return ctrl.Result{}, err
			}
		}
		engramTemplate, err := r.getEngramTemplateForEngram(ctx, engram)
		if err != nil {
			if apierrors.IsNotFound(err) {
				stepLogger.Info("EngramTemplate referenced by Engram not found, setting phase to Blocked")
				err := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, conditions.ReasonTemplateNotFound, fmt.Sprintf("EngramTemplate '%s' not found", engram.Spec.TemplateRef.Name))
				return ctrl.Result{}, err
			}
			stepLogger.Error(err, "Failed to get EngramTemplate for Engram")
			return ctrl.Result{}, err
		}
		if err := r.ensureStepRunSchemaRefs(ctx, step, engram, engramTemplate); err != nil {
			stepLogger.Error(err, "Failed to update StepRun schema references")
			return ctrl.Result{}, err
		}
		return r.reconcileRunScopedRealtimeStep(ctx, step, story, stepSpec, engram, engramTemplate)
	default:
		engramTemplate, err := r.getEngramTemplateForEngram(ctx, engram)
		if err != nil {
			if apierrors.IsNotFound(err) {
				stepLogger.Info("EngramTemplate referenced by Engram not found, setting phase to Blocked")
				err := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, conditions.ReasonTemplateNotFound, fmt.Sprintf("EngramTemplate '%s' not found", engram.Spec.TemplateRef.Name))
				return ctrl.Result{}, err // Stop reconciliation, wait for watch to trigger
			}
			stepLogger.Error(err, "Failed to get EngramTemplate for Engram")
			return ctrl.Result{}, err
		}
		if err := r.ensureStepRunSchemaRefs(ctx, step, engram, engramTemplate); err != nil {
			stepLogger.Error(err, "Failed to update StepRun schema references")
			return ctrl.Result{}, err
		}
		return r.reconcileJobExecution(ctx, step, engram, engramTemplate)
	}
}

func (r *StepRunReconciler) guardStepRunReferences(ctx context.Context, step *runsv1alpha1.StepRun, log *logging.ControllerLogger) (bool, error) {
	if step == nil {
		return false, nil
	}

	var cfg *config.ControllerConfig
	if r != nil && r.ConfigResolver != nil {
		if resolved := r.ConfigResolver.GetOperatorConfig(); resolved != nil {
			cfg = &resolved.Controller
		}
	}

	storyRunKey := step.Spec.StoryRunRef.ToNamespacedName(step)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		r.Client,
		cfg,
		step,
		"runs.bubustack.io",
		"StepRun",
		"runs.bubustack.io",
		"StoryRun",
		storyRunKey.Namespace,
		storyRunKey.Name,
		"StoryRunRef",
	); err != nil {
		message := fmt.Sprintf("StoryRunRef rejected by cross-namespace policy: %v", err)
		if log != nil {
			log.Info("Blocking StepRun due to StoryRunRef policy", "error", err.Error())
		}
		if r.Recorder != nil {
			r.Recorder.Event(step, corev1.EventTypeWarning, conditions.ReasonStoryReferenceInvalid, message)
		}
		if err := r.setStepRunPhase(ctx, step, enums.PhaseFailed, conditions.ReasonStoryReferenceInvalid, message); err != nil {
			return true, err
		}
		return true, nil
	}

	if step.Spec.EngramRef == nil {
		return false, nil
	}

	engramKey := step.Spec.EngramRef.ToNamespacedName(step)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		r.Client,
		cfg,
		step,
		"runs.bubustack.io",
		"StepRun",
		"bubustack.io",
		"Engram",
		engramKey.Namespace,
		engramKey.Name,
		"EngramRef",
	); err != nil {
		message := fmt.Sprintf("EngramRef rejected by cross-namespace policy: %v", err)
		if log != nil {
			log.Info("Blocking StepRun due to EngramRef policy", "error", err.Error())
		}
		if r.Recorder != nil {
			r.Recorder.Event(step, corev1.EventTypeWarning, conditions.ReasonEngramReferenceInvalid, message)
		}
		if err := r.setStepRunPhase(ctx, step, enums.PhaseFailed, conditions.ReasonEngramReferenceInvalid, message); err != nil {
			return true, err
		}
		return true, nil
	}

	return false, nil
}

func (r *StepRunReconciler) ensureStoryNameLabel(ctx context.Context, step *runsv1alpha1.StepRun, log *logging.ControllerLogger) error {
	if step == nil {
		return nil
	}
	labels := step.GetLabels()
	if strings.TrimSpace(labels[contracts.StoryNameLabelKey]) != "" {
		return nil
	}

	storyRun, err := r.getParentStoryRun(ctx, step)
	if err != nil {
		return err
	}
	storyName := strings.TrimSpace(storyRun.Spec.StoryRef.Name)
	if storyName == "" {
		return nil
	}

	before := step.DeepCopy()
	changed, err := ensureStoryNameLabel(step, storyName)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if err := r.Patch(ctx, step, client.MergeFrom(before)); err != nil {
		if log != nil {
			log.Error(err, "Failed to patch StepRun story label")
		}
		return err
	}
	if log != nil {
		log.V(1).Info("Patched StepRun story label", "story", storyName)
	}
	return nil
}

// reconcileJobExecution ensures a Job exists for the StepRun and monitors it.
//
// Behavior:
//   - Creates Job if not found, handling template evaluation blocks.
//   - Marks StepRun as Running after successful Job creation.
//   - Defers steady-state monitoring to handleJobStatus.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to reconcile.
//   - engram *v1alpha1.Engram: the referenced Engram.
//   - engramTemplate *catalogv1alpha1.EngramTemplate: the template.
//
// Returns:
//   - ctrl.Result: reconcile result with possible requeue.
//   - error: non-nil on failures.
func (r *StepRunReconciler) reconcileJobExecution(ctx context.Context, step *runsv1alpha1.StepRun, engram *v1alpha1.Engram, engramTemplate *catalogv1alpha1.EngramTemplate) (ctrl.Result, error) {
	stepLogger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step)

	retryDue := false
	if step.Status.NextRetryAt != nil {
		now := time.Now()
		if step.Status.NextRetryAt.After(now) {
			delay := max(step.Status.NextRetryAt.Sub(now), config.MinRequeueDelay)
			stepLogger.V(1).Info("Waiting to retry StepRun", "retryAt", step.Status.NextRetryAt.Time, "delay", delay.String())
			return ctrl.Result{RequeueAfter: delay}, nil
		}
		retryDue = true
	}

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, job)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if retryDue {
				if clearErr := r.clearNextRetryAt(ctx, step); clearErr != nil {
					stepLogger.Error(clearErr, "Failed to clear retry schedule before creating new Job")
					return ctrl.Result{}, clearErr
				}
			}
			activePods, podCount, podErr := r.hasActiveJobPods(ctx, step)
			if podErr != nil {
				stepLogger.Error(podErr, "Failed to list Job pods before recreate")
				return ctrl.Result{}, podErr
			}
			if activePods {
				stepLogger.Info("Job not found but pods still active; waiting before recreating", "activePods", podCount)
				return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
			}
			if err := r.markStepRunRestart(ctx, step, "job missing before create", stepLogger); err != nil {
				stepLogger.Error(err, "Failed to record StepRun restart before Job creation")
				return ctrl.Result{}, err
			}
			stepLogger.Info("Job not found, creating a new one")
			newJob, createErr := r.createJobForStep(ctx, step, engram, engramTemplate)
			if createErr != nil {
				if errors.Is(createErr, errStepRunCacheHit) {
					return ctrl.Result{}, nil
				}
				if errors.Is(createErr, errStepRunInputSchemaInvalid) {
					return ctrl.Result{}, nil
				}
				if errors.Is(createErr, errStepRunRequiresContextNil) {
					return ctrl.Result{}, nil
				}
				var evalBlocked *templating.ErrEvaluationBlocked
				if errors.As(createErr, &evalBlocked) {
					if shouldFail, message := r.shouldFailBlockedStep(ctx, step, evalBlocked.Reason); shouldFail {
						stepLogger.Info("Template evaluation blocked after dependencies completed; failing StepRun", "reason", evalBlocked.Reason)
						if err := r.setStepRunPhase(ctx, step, enums.PhaseFailed, conditions.ReasonExpressionFailed, message); err != nil {
							stepLogger.Error(err, "Failed to update StepRun status to Failed after blocked template evaluation")
							return ctrl.Result{}, err
						}
						return ctrl.Result{}, nil
					}
					stepLogger.Info("Template evaluation blocked, requeueing", "reason", evalBlocked.Reason)
					if r.Recorder != nil {
						r.Recorder.Eventf(
							step,
							corev1.EventTypeNormal,
							eventReasonJobTemplateBlocked,
							"Template evaluation blocked while creating Job: %s",
							evalBlocked.Reason,
						)
					}
					return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
				}
				var offloaded *templating.ErrOffloadedDataUsage
				if errors.As(createErr, &offloaded) {
					policy := resolveOffloadedPolicy(r.ConfigResolver)
					if shouldBlockOffloaded(policy) {
						stepLogger.Info("Template evaluation requires offloaded data; requeueing", "reason", offloaded.Reason)
						if r.Recorder != nil {
							r.Recorder.Eventf(
								step,
								corev1.EventTypeNormal,
								eventReasonJobTemplateBlocked,
								"Template evaluation requires offloaded data: %s",
								offloaded.Reason,
							)
						}
						return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
					}
					failMsg := offloaded.Error()
					if err := r.setStepRunPhase(ctx, step, enums.PhaseFailed, conditions.ReasonExpressionFailed, failMsg); err != nil {
						stepLogger.Error(err, "Failed to update StepRun status to Failed after offloaded template access")
						return ctrl.Result{}, err
					}
					return ctrl.Result{}, nil
				}
				stepLogger.Error(createErr, "Failed to create Job for StepRun")
				return ctrl.Result{}, createErr
			}
			if err := r.Create(ctx, newJob); err != nil {
				if apierrors.IsAlreadyExists(err) {
					if retryDue {
						stepLogger.Info("Retry due but job still exists; deleting and requeueing", "job", newJob.Name)
						deleteErr := r.Delete(ctx, newJob)
						if deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
							stepLogger.Error(deleteErr, "Failed to delete stale Job before retry")
							return ctrl.Result{}, deleteErr
						}
						return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
					}
					stepLogger.Info("Job already exists after create attempt; reconciling existing Job status")
					existing := &batchv1.Job{}
					if getErr := r.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, existing); getErr != nil {
						stepLogger.Error(getErr, "Failed to fetch Job after AlreadyExists error")
						return ctrl.Result{}, getErr
					}
					r.ensureStepRunJobUID(ctx, step, existing, stepLogger)
					return r.handleJobStatus(ctx, step, existing)
				}
				if apierrors.IsRequestEntityTooLargeError(err) || strings.Contains(strings.ToLower(err.Error()), "request entity too large") {
					contextSize := envVarSizeBytes(newJob, contracts.TemplateContextEnv)
					stepLogger.Error(err, "Job create rejected: request entity too large", "templateContextBytes", contextSize)
					if r.Recorder != nil {
						if contextSize > 0 {
							r.Recorder.Eventf(
								step,
								corev1.EventTypeWarning,
								eventReasonJobCreateTooLarge,
								"Job create rejected (request entity too large). Template context size=%d bytes. Set %s=true to log/annotate a summary.",
								contextSize,
								templateContextDebugAnnotation,
							)
						} else {
							r.Recorder.Eventf(
								step,
								corev1.EventTypeWarning,
								eventReasonJobCreateTooLarge,
								"Job create rejected (request entity too large). Set %s=true to log/annotate a summary.",
								templateContextDebugAnnotation,
							)
						}
					}
					failureMsg := "Job create rejected: request entity too large"
					if contextSize > 0 {
						failureMsg = fmt.Sprintf("%s (template context size=%d bytes)", failureMsg, contextSize)
					}
					if phaseErr := r.setStepRunPhase(ctx, step, enums.PhaseFailed, conditions.ReasonInputTooLarge, failureMsg); phaseErr != nil {
						stepLogger.Error(phaseErr, "Failed to update StepRun status after job create rejection")
						return ctrl.Result{}, phaseErr
					}
					return ctrl.Result{}, nil
				}
				stepLogger.Error(err, "Failed to create Job in cluster")
				return ctrl.Result{}, err
			}
			stepLogger.Info("Created Job for StepRun", "jobName", newJob.Name, "jobUID", string(newJob.GetUID()))
			r.ensureStepRunJobUID(ctx, step, newJob, stepLogger)
			if err := r.setStepRunPhase(ctx, step, enums.PhaseRunning, conditions.ReasonScheduled, "Job created for StepRun"); err != nil {
				stepLogger.Error(err, "Failed to update StepRun status to Running")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		stepLogger.Error(err, "Failed to get Job for StepRun")
		return ctrl.Result{}, err
	}

	if retryDue {
		stepLogger.Info("Retry due but previous Job still exists; deleting and requeueing", "job", job.Name)
		deleteErr := r.Delete(ctx, job)
		if deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
			stepLogger.Error(deleteErr, "Failed to delete stale Job before retry")
			return ctrl.Result{}, deleteErr
		}
		return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
	}

	r.ensureStepRunJobUID(ctx, step, job, stepLogger)
	if step.Status.Phase == enums.PhasePending {
		if err := r.setStepRunPhase(ctx, step, enums.PhaseRunning, conditions.ReasonRunning, "Job exists; resuming StepRun monitoring"); err != nil {
			stepLogger.Error(err, "Failed to update StepRun status to Running on resume")
			return ctrl.Result{}, err
		}
	}
	return r.handleJobStatus(ctx, step, job)
}

// shouldFailBlockedStep decides whether a blocked template evaluation should be treated as terminal.
//
// Behavior:
//   - Resolves the Story and parent StoryRun for the StepRun.
//   - Finds the step dependencies inferred from "needs"/"with"/"if".
//   - Returns true when all dependencies are terminal, meaning missing data won't appear.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun being evaluated.
//   - reason string: the blocked template reason.
//
// Returns:
//   - bool: true if the StepRun should be failed.
//   - string: failure message to store on the StepRun.
func (r *StepRunReconciler) shouldFailBlockedStep(ctx context.Context, step *runsv1alpha1.StepRun, reason string) (bool, string) {
	if isMaterializeRunningReason(reason) {
		return false, ""
	}
	story, err := r.getStoryForStep(ctx, step)
	if err != nil {
		return false, ""
	}
	storyRun, err := r.getParentStoryRun(ctx, step)
	if err != nil {
		return false, ""
	}
	stepSpec := findStoryStep(story, step.Spec.StepID)
	if stepSpec == nil {
		return false, ""
	}

	dependencies, _ := buildDependencyGraphs(allStorySteps(story))
	deps := dependencies[stepSpec.Name]
	if len(deps) == 0 {
		return false, ""
	}
	for dep := range deps {
		state, ok := storyRun.Status.StepStates[dep]
		if !ok || !state.Phase.IsTerminal() {
			return false, ""
		}
	}

	message := fmt.Sprintf("Input resolution failed after dependencies completed: %s", reason)
	return true, message
}

func isMaterializeRunningReason(reason string) bool {
	const prefix = "materialize step "
	const suffix = " is still running"
	trimmed := strings.TrimSpace(reason)
	if !strings.HasPrefix(trimmed, prefix) || !strings.HasSuffix(trimmed, suffix) {
		return false
	}
	name := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, prefix), suffix))
	return name != ""
}

// setStepRunPhase updates the StepRun phase and conditions via conflict-retrying kubeutil.
//
// Behavior:
//   - Updates parent Engram status for terminal phases (best-effort).
//   - Delegates to runstatus.PatchStepRunPhase for actual kubeutil.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to update.
//   - phase enums.Phase: the new phase.
//   - message string: message for the phase transition.
//
// Returns:
//   - error: non-nil on patch failure.
func (r *StepRunReconciler) setStepRunPhase(ctx context.Context, step *runsv1alpha1.StepRun, phase enums.Phase, reason, message string) error {
	// If the phase is terminal, also update the parent Engram's status.
	if phase.IsTerminal() {
		if err := r.updateEngramStatus(ctx, step, phase); err != nil {
			// Log the error but don't fail the StepRun phase transition.
			// The Engram status update is best-effort.
			logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step).Error(err, "Failed to update parent engram status")
		}
	}

	return runstatus.PatchStepRunPhase(ctx, r.Client, step, phase, reason, message)
}

func (r *StepRunReconciler) setStepRunHandoff(ctx context.Context, step *runsv1alpha1.StepRun, handoff *runsv1alpha1.HandoffStatus) error {
	if step == nil {
		return nil
	}
	return runstatus.PatchStepRunHandoff(ctx, r.Client, step, handoff)
}

func (r *StepRunReconciler) ensureStepRunCorrelationLabel(ctx context.Context, step *runsv1alpha1.StepRun) (bool, ctrl.Result, error) {
	if step == nil {
		return false, ctrl.Result{}, nil
	}
	correlation := correlationIDFromAnnotations(step)
	if correlation == "" {
		return false, ctrl.Result{}, nil
	}
	labels := step.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	if !ensureCorrelationLabel(labels, correlation) {
		return false, ctrl.Result{}, nil
	}
	patch := client.MergeFrom(step.DeepCopy())
	step.SetLabels(labels)
	if err := r.Patch(ctx, step, patch); err != nil {
		return true, ctrl.Result{}, err
	}
	return true, ctrl.Result{Requeue: true}, nil
}

// updateEngramStatus increments parent Engram execution counters on terminal phases.
//
// Behavior:
//   - Skips StepRuns without an EngramRef.
//   - Fetches the Engram and applies retryable status kubeutil.
//   - Updates LastExecutionTime, TotalExecutions, and FailedExecutions.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the completed StepRun.
//   - phase enums.Phase: the terminal phase.
//
// Returns:
//   - error: non-nil on fetch or patch failure.
func (r *StepRunReconciler) updateEngramStatus(ctx context.Context, step *runsv1alpha1.StepRun, phase enums.Phase) error {
	if step == nil || step.Spec.EngramRef == nil || step.Spec.EngramRef.Name == "" {
		return nil // No engram to update
	}

	engram := &v1alpha1.Engram{}
	engramKey := client.ObjectKey{Namespace: step.Namespace, Name: step.Spec.EngramRef.Name}
	if err := r.Get(ctx, engramKey, engram); err != nil {
		return fmt.Errorf("failed to get parent engram %s for status update: %w", step.Spec.EngramRef.Name, err)
	}

	return kubeutil.RetryableStatusPatch(ctx, r.Client, engram, func(obj client.Object) {
		e := obj.(*v1alpha1.Engram)
		now := metav1.Now()
		e.Status.LastExecutionTime = &now
		e.Status.TotalExecutions++
		if phase == enums.PhaseFailed {
			e.Status.FailedExecutions++
		}
	})
}

// getEngramForStep fetches the Engram referenced by the StepRun.
//
// Behavior:
//   - Returns error if EngramRef is missing or empty.
//   - Uses refs.LoadNamespacedReference for fetching.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun with the reference.
//
// Returns:
//   - *v1alpha1.Engram: the fetched Engram.
//   - error: non-nil on missing ref or fetch failure.
func (r *StepRunReconciler) getEngramForStep(ctx context.Context, step *runsv1alpha1.StepRun) (*v1alpha1.Engram, error) {
	if step.Spec.EngramRef == nil || strings.TrimSpace(step.Spec.EngramRef.Name) == "" {
		return nil, fmt.Errorf("steprun %s has no engram reference (.spec.engramRef.name)", step.Name)
	}
	engram, key, err := refs.LoadNamespacedReference(
		ctx,
		r.Client,
		step,
		step.Spec.EngramRef,
		func() *v1alpha1.Engram { return &v1alpha1.Engram{} },
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("get engram %s: %w", key.String(), err)
	}
	return engram, nil
}

// getEngramTemplateForEngram fetches the cluster-scoped EngramTemplate.
//
// Behavior:
//   - Returns error if engram is nil or templateRef.name is empty.
//   - Templates are cluster-scoped (no namespace).
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - engram *v1alpha1.Engram: supplies Spec.TemplateRef.Name.
//
// Returns:
//   - *catalogv1alpha1.EngramTemplate: the fetched template.
//   - error: non-nil on fetch failure.
func (r *StepRunReconciler) getEngramTemplateForEngram(ctx context.Context, engram *v1alpha1.Engram) (*catalogv1alpha1.EngramTemplate, error) {
	if engram == nil {
		return nil, fmt.Errorf("engram is nil: cannot fetch template")
	}
	name := strings.TrimSpace(engram.Spec.TemplateRef.Name)
	if name == "" {
		return nil, fmt.Errorf("engram %s missing spec.templateRef.name", engram.Name)
	}
	template, err := refs.LoadClusterObject(
		ctx,
		r.Client,
		name,
		func() *catalogv1alpha1.EngramTemplate { return &catalogv1alpha1.EngramTemplate{} },
	)
	if err != nil {
		return nil, fmt.Errorf("get EngramTemplate %s: %w", name, err)
	}
	return template, nil
}

// checkDeclaredOutputKeys emits a warning event when the actual output keys
// of a completed StepRun don't match the DeclaredOutputKeys on its
// EngramTemplate.  This is informational only — it never fails the step.
func (r *StepRunReconciler) checkDeclaredOutputKeys(ctx context.Context, step *runsv1alpha1.StepRun, logger *logging.ControllerLogger) {
	if step.Status.Output == nil || len(step.Status.Output.Raw) == 0 {
		return
	}

	engram, err := r.getEngramForStep(ctx, step)
	if err != nil || engram == nil {
		return
	}
	template, err := r.getEngramTemplateForEngram(ctx, engram)
	if err != nil || template == nil || len(template.Spec.DeclaredOutputKeys) == 0 {
		return
	}

	var output map[string]json.RawMessage
	if err := json.Unmarshal(step.Status.Output.Raw, &output); err != nil {
		return
	}

	declared := make(map[string]struct{}, len(template.Spec.DeclaredOutputKeys))
	for _, k := range template.Spec.DeclaredOutputKeys {
		declared[k] = struct{}{}
	}

	var missing, extra []string
	for _, k := range template.Spec.DeclaredOutputKeys {
		if _, ok := output[k]; !ok {
			missing = append(missing, k)
		}
	}
	for k := range output {
		if _, ok := declared[k]; !ok {
			extra = append(extra, k)
		}
	}

	if len(missing) > 0 || len(extra) > 0 {
		sort.Strings(extra) // deterministic ordering for output map iteration
		msg := fmt.Sprintf("Output key mismatch: missing=%v extra=%v", missing, extra)
		logger.Info(msg)
		r.emitStepRunLabeledEvent(ctx, step, corev1.EventTypeWarning, eventReasonOutputKeyMismatch, msg)
	}
}

// reconcileDelete handles StepRun deletion and resource cleanup.
//
// Behavior:
//   - Deletes owned Job if it exists.
//   - Cleans up realtime runtime resources.
//   - Removes finalizer when cleanup is complete.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun being deleted.
//
// Returns:
//   - ctrl.Result: with requeue if cleanup pending.
//   - error: non-nil on failures.
func (r *StepRunReconciler) reconcileDelete(ctx context.Context, step *runsv1alpha1.StepRun) (ctrl.Result, error) {
	stepLogger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step)
	stepLogger.Info("Reconciling deletion for StepRun")

	if !controllerutil.ContainsFinalizer(step, StepRunFinalizer) {
		return ctrl.Result{}, nil
	}

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, job)

	if err != nil && !apierrors.IsNotFound(err) {
		stepLogger.Error(err, "Failed to get Job for cleanup check")
		return ctrl.Result{}, err
	}

	// If the job exists and is not being deleted, delete it.
	if err == nil && job.DeletionTimestamp.IsZero() {
		stepLogger.Info("Deleting owned Job")
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
			stepLogger.Error(err, "Failed to delete Job during cleanup")
			return ctrl.Result{}, err
		}
		// Requeue to wait for the job to be fully deleted.
		return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
	}

	// If the job is not found, it's safe to remove the finalizer.
	if apierrors.IsNotFound(err) {
		cleaned, cleanupErr := r.cleanupRunScopedRuntime(ctx, step)
		if cleanupErr != nil {
			stepLogger.Error(cleanupErr, "Failed to cleanup realtime runtime during deletion")
			return ctrl.Result{}, cleanupErr
		}
		if !cleaned {
			stepLogger.Info("Waiting for realtime runtime resources to be deleted before removing finalizer")
			return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
		}

		stepLogger.Info("Owned resources are deleted, removing finalizer")
		beforePatch := step.DeepCopy()
		controllerutil.RemoveFinalizer(step, StepRunFinalizer)
		if err := r.Patch(ctx, step, client.MergeFrom(beforePatch)); err != nil {
			stepLogger.Error(err, "Failed to remove finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// If the job still exists (is being deleted), requeue.
	stepLogger.Info("Owned Job is still terminating, waiting for it to be deleted")
	return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
}

// createJobForStep builds a Job resource for executing the StepRun.
//
// Behavior:
//   - Prepares execution context with inputs and config.
//   - Builds job spec with proper pod template.
//   - Sets owner reference to StepRun.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StepRun: the StepRun owner.
//   - engram *v1alpha1.Engram: the Engram configuration.
//   - engramTemplate *catalogv1alpha1.EngramTemplate: the template.
//
// Returns:
//   - *batchv1.Job: the constructed Job.
//   - error: non-nil on failures.
func (r *StepRunReconciler) createJobForStep(ctx context.Context, srun *runsv1alpha1.StepRun, engram *v1alpha1.Engram, engramTemplate *catalogv1alpha1.EngramTemplate) (*batchv1.Job, error) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(srun)
	operatorCfg := r.ConfigResolver.GetOperatorConfig()
	runtimeEnvCfg := toRuntimeEnvConfig(operatorCfg.Controller.Engram.EngramControllerConfig)

	story, storyRun, resolvedConfig, inputBytes, stepTimeout, downstreamTargets, dependentSteps, err := r.prepareExecutionContext(ctx, srun, engram, engramTemplate, stepLogger)
	if err != nil {
		return nil, err
	}

	cacheMeta, cacheErr := r.buildCacheMetadata(ctx, srun, engram, resolvedConfig.Cache, inputBytes)
	if cacheErr != nil {
		stepLogger.Error(cacheErr, "Failed to resolve cache metadata; skipping cache")
		cacheMeta = nil
	}
	if cacheMeta != nil && cacheMeta.ReadEnabled {
		hit, err := r.tryCacheHit(ctx, srun, cacheMeta, engramTemplate, stepLogger)
		if err != nil {
			stepLogger.Error(err, "Cache lookup failed; continuing without cache")
		} else {
			engramName := ""
			if engram != nil {
				engramName = engram.Name
			}
			result := "miss"
			if hit {
				result = "hit"
			}
			metrics.RecordStepRunCacheLookup(srun.Namespace, engramName, result)
			if hit {
				return nil, errStepRunCacheHit
			}
		}
	}
	if cacheMeta != nil && (cacheMeta.ReadEnabled || cacheMeta.WriteEnabled) {
		r.ensureCacheAnnotations(ctx, srun, cacheMeta, stepLogger)
	}

	if err := r.ensureStorageSecret(ctx, srun.Namespace, resolvedConfig, stepLogger); err != nil {
		return nil, err
	}

	if err := r.ensureDownstreamTargets(ctx, srun, downstreamTargets, dependentSteps, stepLogger); err != nil {
		return nil, err
	}

	executionMode := "batch"
	if len(downstreamTargets) > 0 {
		executionMode = "hybrid"
	}

	storageTimeout := podspec.FormatStorageTimeout(resolvedConfig, runtimeEnvCfg.DefaultStorageTimeoutSeconds)
	resources, err := r.assembleJobResources(ctx, jobResourceInputs{
		srun:           srun,
		story:          story,
		storyRun:       storyRun,
		engram:         engram,
		engramTemplate: engramTemplate,
		resolvedConfig: resolvedConfig,
		runtimeEnvCfg:  runtimeEnvCfg,
		inputBytes:     inputBytes,
		stepTimeout:    stepTimeout,
		executionMode:  executionMode,
		stepLogger:     stepLogger,
	})
	if err != nil {
		return nil, err
	}

	activeDeadlineSeconds := int64((stepTimeout + 2*time.Minute).Seconds())
	envVars := resources.envVars

	job := r.buildJobSpec(srun, resolvedConfig, envVars, resources.envFrom, resources.volumes, resources.volumeMounts, activeDeadlineSeconds, storageTimeout)
	if err := controllerutil.SetControllerReference(srun, job, r.Scheme); err != nil {
		return nil, err
	}
	_ = storyRun // storyRun currently unused beyond inputs; keep for future enhancements
	return job, nil
}

type jobResourceInputs struct {
	srun           *runsv1alpha1.StepRun
	story          *v1alpha1.Story
	storyRun       *runsv1alpha1.StoryRun
	engram         *v1alpha1.Engram
	engramTemplate *catalogv1alpha1.EngramTemplate
	resolvedConfig *config.ResolvedExecutionConfig
	runtimeEnvCfg  coreenv.Config
	inputBytes     []byte
	stepTimeout    time.Duration
	executionMode  string
	stepLogger     *logging.ControllerLogger
}

func (r *StepRunReconciler) assembleJobResources(ctx context.Context, in jobResourceInputs) (jobResources, error) {
	resources := jobResources{}
	secretArtifacts := podspec.BuildSecretArtifacts(in.engramTemplate.Spec.SecretSchema, in.resolvedConfig.Secrets)

	envVars := r.buildBaseEnvVars(in.srun, in.story, in.inputBytes, in.stepTimeout, in.executionMode, in.resolvedConfig, in.resolvedConfig.DebugLogs)
	envVars = append(envVars, coreenv.BuildGRPCTuningEnv(in.runtimeEnvCfg)...)
	envVars = append(envVars, secretArtifacts.EnvVars...)
	envVars = append(envVars, corev1.EnvVar{Name: contracts.EngramNameEnv, Value: in.engram.Name})
	envVars = append(envVars, corev1.EnvVar{Name: contracts.EngramVersionEnv, Value: strings.TrimSpace(in.engram.Spec.Version)})

	volumes := append([]corev1.Volume(nil), secretArtifacts.Volumes...)
	volumeMounts := append([]corev1.VolumeMount(nil), secretArtifacts.VolumeMounts...)
	envFrom := append([]corev1.EnvFromSource(nil), secretArtifacts.EnvFrom...)

	if hasS3(in.resolvedConfig) {
		in.stepLogger.Info("Configuring pod for S3 object storage access", "bucket", in.resolvedConfig.Storage.S3.Bucket)
	}

	if tlsSecret := getTLSSecretName(&in.engram.ObjectMeta); tlsSecret != "" {
		if err := r.configureTLSEnvAndMounts(ctx, in.engram.Namespace, tlsSecret, &volumes, &volumeMounts, &envVars); err != nil {
			in.stepLogger.Error(err, "Failed to configure TLS for StepRun job", "secret", tlsSecret)
		}
	}

	startedAt := metav1.Now()
	if in.srun.Status.StartedAt != nil {
		startedAt = *in.srun.Status.StartedAt
	}
	r.addStartedAtEnv(&envVars, startedAt)

	schemaName := fmt.Sprintf("EngramTemplate %s inputs", in.engramTemplate.Name)
	configBytes, err := r.evaluateStepConfig(ctx, in.srun, in.storyRun, in.story, in.engramTemplate.Spec.InputSchema, schemaName)
	if err != nil {
		return jobResources{}, fmt.Errorf("failed to evaluate step config: %w", err)
	}
	envVars = append(envVars, buildConfigEnvVars(configBytes)...)
	if !isMaterializeStepRun(in.srun) {
		if templateEnv, err := r.buildTemplateContextEnvVar(ctx, in.srun, in.storyRun, in.story); err != nil {
			logr.FromContextOrDiscard(ctx).Error(err, "Failed to build template context env", "stepRun", in.srun.Name)
		} else if templateEnv != nil {
			envVars = append(envVars, *templateEnv)
		}
	}

	resources.envVars = envVars
	resources.envFrom = envFrom
	resources.volumes = volumes
	resources.volumeMounts = volumeMounts
	return resources, nil
}

func hasS3(resolved *config.ResolvedExecutionConfig) bool {
	return resolved != nil && resolved.Storage != nil && resolved.Storage.S3 != nil
}

func (r *StepRunReconciler) ensureStorageSecret(ctx context.Context, targetNamespace string, resolved *config.ResolvedExecutionConfig, logger *logging.ControllerLogger) error {
	if !hasS3(resolved) || resolved.Storage.S3.Authentication.SecretRef == nil {
		return nil
	}
	sourceNamespace := r.ConfigResolver.ConfigNamespace()
	if sourceNamespace == "" || sourceNamespace == targetNamespace {
		return nil
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	secretName := resolved.Storage.S3.Authentication.SecretRef.Name
	if err := kubeutil.EnsureSecretCopy(ctx, reader, r.Client, sourceNamespace, targetNamespace, secretName); err != nil {
		logger.Error(err, "Failed to sync storage secret", "secret", secretName, "sourceNamespace", sourceNamespace)
		return err
	}
	logger.V(1).Info("Synced storage secret into workload namespace", "secret", secretName, "sourceNamespace", sourceNamespace)
	return nil
}

// prepareExecutionContext gathers all context needed for Job/Deployment creation.
//
// Behavior:
//   - Fetches Story, StoryRun, and resolves execution config.
//   - Resolves inputs and computes step timeout.
//   - Computes downstream targets for realtime routing.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StepRun: the StepRun.
//   - engram *v1alpha1.Engram: the Engram configuration.
//   - engramTemplate *catalogv1alpha1.EngramTemplate: the template.
//   - stepLogger *logging.ControllerLogger: for logging.
//
// Returns:
//   - All execution context values, or error on failure.
func (r *StepRunReconciler) prepareExecutionContext(ctx context.Context, srun *runsv1alpha1.StepRun, engram *v1alpha1.Engram, engramTemplate *catalogv1alpha1.EngramTemplate, stepLogger *logging.ControllerLogger) (*v1alpha1.Story, *runsv1alpha1.StoryRun, *config.ResolvedExecutionConfig, []byte, time.Duration, []runsv1alpha1.DownstreamTarget, []string, error) {
	story, err := r.getStoryForStep(ctx, srun)
	if err != nil {
		return nil, nil, nil, nil, 0, nil, nil, fmt.Errorf("failed to get story for step: %w", err)
	}
	storyRun, err := r.getParentStoryRun(ctx, srun)
	if err != nil {
		return nil, nil, nil, nil, 0, nil, nil, fmt.Errorf("failed to get parent storyrun: %w", err)
	}
	resolvedConfig, err := r.ConfigResolver.ResolveExecutionConfig(ctx, srun, story, engram, engramTemplate, nil)
	if err != nil {
		return nil, nil, nil, nil, 0, nil, nil, fmt.Errorf("failed to resolve execution config for step '%s': %w", srun.Name, err)
	}
	stepLogger.Info("Resolved ServiceAccountName", "sa", resolvedConfig.ServiceAccountName)
	var inputSchema *runtime.RawExtension
	schemaName := ""
	if engramTemplate != nil {
		inputSchema = engramTemplate.Spec.InputSchema
		schemaName = fmt.Sprintf("EngramTemplate %s inputs", engramTemplate.Name)
	}
	inputBytes, err := r.resolveRunScopedInputs(ctx, srun, storyRun, story, inputSchema, schemaName)
	if err != nil {
		return nil, nil, nil, nil, 0, nil, nil, err
	}
	stepTimeout := r.computeStepTimeout(srun, story, stepLogger)
	downstreamTargets, dependentSteps, err := r.computeDownstreamTargets(ctx, story, srun)
	if err != nil {
		return nil, nil, nil, nil, 0, nil, nil, err
	}
	return story, storyRun, resolvedConfig, inputBytes, stepTimeout, downstreamTargets, dependentSteps, nil
}

// computeStepTimeout selects the effective timeout for a StepRun.
//
// Behavior:
//   - Starts from Controller.DefaultStepTimeout (or 30m if unset).
//   - Overrides with StepRun.Spec.Timeout if valid.
//   - Finally honors Story.Spec.Policy.Timeouts.Step when present.
//   - Logs invalid duration strings before falling back.
//
// Arguments:
//   - srun *runsv1alpha1.StepRun: supplies optional per-step timeout.
//   - story *v1alpha1.Story: provides policy-level overrides.
//   - stepLogger *logging.ControllerLogger: for logging timeout selection.
//
// Returns:
//   - time.Duration: the computed timeout.
func (r *StepRunReconciler) computeStepTimeout(srun *runsv1alpha1.StepRun, story *v1alpha1.Story, stepLogger *logging.ControllerLogger) time.Duration {
	timeout := r.baseStepTimeout(story, srun)

	if srun != nil && srun.Spec.Timeout != "" {
		if parsed, err := parsePositiveDuration(srun.Spec.Timeout); err == nil {
			return parsed
		} else {
			stepLogger.Error(err, "Invalid step timeout on StepRun, using defaults", "rawTimeout", srun.Spec.Timeout)
		}
	}

	if policyTimeout, hasPolicy, err := parseStoryPolicyTimeout(story); hasPolicy {
		if err != nil {
			stepLogger.Error(err, "Invalid step timeout in Story policy, using default")
		} else {
			stepLogger.Info("Using Story-level step timeout", "timeout", policyTimeout)
			return policyTimeout
		}
	}

	return timeout
}

func (r *StepRunReconciler) baseStepTimeout(story *v1alpha1.Story, srun *runsv1alpha1.StepRun) time.Duration {
	controllerConfig := r.ConfigResolver.GetOperatorConfig().Controller
	timeout := controllerConfig.DefaultStepTimeout

	if story != nil && srun != nil {
		if storyStep := findStoryStep(story, srun.Spec.StepID); storyStep != nil {
			switch storyStep.Type {
			case enums.StepTypeGate:
				if controllerConfig.ApprovalDefaultTimeout > 0 {
					timeout = controllerConfig.ApprovalDefaultTimeout
				}
			case enums.StepTypeWait:
				if controllerConfig.ExternalDataTimeout > 0 {
					timeout = controllerConfig.ExternalDataTimeout
				}
			case enums.StepTypeCondition:
				if controllerConfig.ConditionalTimeout > 0 {
					timeout = controllerConfig.ConditionalTimeout
				}
			}
		}
	}

	if timeout == 0 {
		timeout = 30 * time.Minute
	}
	return timeout
}

func parsePositiveDuration(raw string) (time.Duration, error) {
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse duration: %w", err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	return parsed, nil
}

func parseStoryPolicyTimeout(story *v1alpha1.Story) (time.Duration, bool, error) {
	if story == nil || story.Spec.Policy == nil || story.Spec.Policy.Timeouts == nil || story.Spec.Policy.Timeouts.Step == nil {
		return 0, false, nil
	}
	duration, err := parsePositiveDuration(*story.Spec.Policy.Timeouts.Step)
	if err != nil {
		return 0, true, err
	}
	return duration, true, nil
}

// computeDownstreamTargets finds realtime Engrams that depend on this StepRun.
//
// Behavior:
//   - Uses buildDependencyGraphs to find dependent steps.
//   - Resolves each step's Engram and filters to Deployment/StatefulSet modes.
//   - Constructs `name.namespace.svc:<port>` endpoints.
//   - Deduplicates before returning DownstreamTarget entries.
//
// Arguments:
//   - ctx context.Context: for Engram GET calls.
//   - story *v1alpha1.Story: supplies the step graph.
//   - srun *runsv1alpha1.StepRun: provides step ID and namespace.
//
// Returns:
//   - []runsv1alpha1.DownstreamTarget: unique GRPC endpoints.
//   - []string: dependent step names.
//   - error: non-nil for unrecoverable API failures.
func (r *StepRunReconciler) computeDownstreamTargets(ctx context.Context, story *v1alpha1.Story, srun *runsv1alpha1.StepRun) ([]runsv1alpha1.DownstreamTarget, []string, error) {
	if story == nil || srun == nil {
		return nil, nil, nil
	}

	allSteps := allStorySteps(story)
	_, dependents := buildDependencyGraphs(allSteps)
	dependentSet := dependents[srun.Spec.StepID]
	if len(dependentSet) == 0 {
		return nil, nil, nil
	}

	stepIndex := make(map[string]*v1alpha1.Step, len(allSteps))
	for i := range allSteps {
		step := &allSteps[i]
		stepIndex[step.Name] = step
	}

	port := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort
	maxDownstreams := 0
	if story != nil {
		if sourceStep := stepIndex[srun.Spec.StepID]; sourceStep != nil {
			transportName := sourceStep.Transport
			transportObj, decl, err := transportutil.ResolveStoryTransport(ctx, r.Client, story, transportName)
			if err == nil && transportObj != nil && decl != nil {
				defaultSettings, err := transportutil.MergeSettingsWithStreaming(transportObj.Spec.DefaultSettings, transportObj.Spec.Streaming)
				if err == nil {
					storySettings, err := transportutil.MergeSettingsWithStreaming(decl.Settings, decl.Streaming)
					if err == nil {
						settingsBytes, err := transportutil.MergeSettings(defaultSettings, storySettings)
						if err == nil {
							maxDownstreams = extractRoutingMaxDownstreams(settingsBytes)
						}
					}
				}
			}
		}
	}
	dependentNames := make([]string, 0, len(dependentSet))
	for name := range dependentSet {
		dependentNames = append(dependentNames, name)
	}
	sort.Strings(dependentNames)

	log := logging.NewControllerLogger(ctx, "steprun-hybrid").WithStepRun(srun)
	seenEndpoints := make(map[string]struct{})
	targets := make([]runsv1alpha1.DownstreamTarget, 0, len(dependentNames))
	selectedNames := make([]string, 0, len(dependentNames))

	for _, depName := range dependentNames {
		depStep := stepIndex[depName]
		if depStep == nil || depStep.Ref == nil {
			continue
		}

		depEngram := &v1alpha1.Engram{}
		key := depStep.Ref.ToNamespacedName(srun)
		if err := r.Get(ctx, key, depEngram); err != nil {
			if apierrors.IsNotFound(err) {
				log.Info("Skipping downstream target; referenced engram not found", "dependentStep", depName, "engram", key.Name)
				continue
			}
			return nil, nil, fmt.Errorf("failed to resolve downstream engram '%s' for step '%s': %w", key.Name, depName, err)
		}

		mode := depEngram.Spec.Mode
		if mode == "" {
			mode = enums.WorkloadModeJob
		}
		if mode != enums.WorkloadModeDeployment && mode != enums.WorkloadModeStatefulSet {
			continue
		}

		endpoint := fmt.Sprintf("%s.%s.svc:%d", depEngram.Name, depEngram.Namespace, port)
		if _, exists := seenEndpoints[endpoint]; exists {
			continue
		}
		seenEndpoints[endpoint] = struct{}{}

		targets = append(targets, runsv1alpha1.DownstreamTarget{
			GRPCTarget: &runsv1alpha1.GRPCTarget{Endpoint: endpoint},
		})
		selectedNames = append(selectedNames, depName)
		if maxDownstreams > 0 && len(targets) >= maxDownstreams {
			log.Info("Capped downstream targets due to maxDownstreams", "step", srun.Spec.StepID, "maxDownstreams", maxDownstreams)
			break
		}
	}

	return targets, selectedNames, nil
}

func extractRoutingMaxDownstreams(settings []byte) int {
	if len(settings) == 0 {
		return 0
	}
	var parsed map[string]any
	if err := json.Unmarshal(settings, &parsed); err != nil {
		return 0
	}
	rawRouting, ok := parsed["routing"].(map[string]any)
	if !ok {
		return 0
	}
	raw := rawRouting["maxDownstreams"]
	switch v := raw.(type) {
	case float64:
		if v > 0 {
			return int(v)
		}
	case int:
		if v > 0 {
			return v
		}
	case int32:
		if v > 0 {
			return int(v)
		}
	case int64:
		if v > 0 {
			return int(v)
		}
	}
	return 0
}

// ensureDownstreamTargets validates and patches StepRun downstream targets.
//
// Behavior:
//   - Validates desired targets via pkg/runs/validation.
//   - Patches StepRun.Spec.DownstreamTargets if changed.
//   - Emits events with hashed endpoints.
//   - Records mutation metrics.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StepRun: the StepRun to update.
//   - desired []runsv1alpha1.DownstreamTarget: target list.
//   - dependentSteps []string: names of dependent steps.
//   - logger *logging.ControllerLogger: for logging.
//
// Returns:
//   - error: non-nil on validation or patch failure.
func (r *StepRunReconciler) ensureDownstreamTargets(ctx context.Context, srun *runsv1alpha1.StepRun, desired []runsv1alpha1.DownstreamTarget, dependentSteps []string, logger *logging.ControllerLogger) error {
	stepSummary := strings.Join(dependentSteps, ",")
	if stepSummary == "" {
		stepSummary = metrics.EmptyHashSummary
	}
	if err := runsvalidation.ValidateDownstreamTargets(desired); err != nil {
		logger.Error(err, "Refusing to update downstream targets due to validation failure")
		r.emitStepRunLabeledEvent(
			ctx,
			srun,
			corev1.EventTypeWarning,
			eventReasonDownstreamTargetsRejected,
			fmt.Sprintf(
				"Rejected %d downstream target(s) for dependent steps [%s]: %v",
				len(desired),
				stepSummary,
				err,
			),
		)
		return err
	}
	desiredHashes := summarizeDownstreamTargetHashes(desired)
	if reflect.DeepEqual(srun.Spec.DownstreamTargets, desired) {
		return nil
	}

	before := srun.DeepCopy()
	prevHashes := summarizeDownstreamTargetHashes(before.Spec.DownstreamTargets)
	formatHashes := func(hashes []string) string {
		if len(hashes) == 0 {
			return metrics.EmptyHashSummary
		}
		return strings.Join(hashes, "|")
	}
	if len(desired) == 0 {
		srun.Spec.DownstreamTargets = nil
	} else {
		srun.Spec.DownstreamTargets = desired
	}

	if err := r.Patch(ctx, srun, client.MergeFrom(before)); err != nil {
		logger.Error(err, "Failed to update StepRun downstream targets")
		return err
	}
	action := "updated"
	if len(desired) == 0 {
		action = "cleared"
	}
	hashLabel := formatHashes(desiredHashes)
	if len(desired) == 0 {
		hashLabel = formatHashes(prevHashes)
	}
	metrics.RecordDownstreamTargetMutation(srun.Namespace, srun.Spec.StepID, action, hashLabel)

	if len(desired) > 0 {
		logger.Info("Updated StepRun downstream targets", "count", len(desired), "endpointHashes", desiredHashes)
		hashText := strings.Join(desiredHashes, ",")
		if hashText == "" {
			hashText = metrics.EmptyHashSummary
		}
		r.emitStepRunLabeledEvent(
			ctx,
			srun,
			corev1.EventTypeNormal,
			eventReasonDownstreamTargetsUpdated,
			fmt.Sprintf(
				"Updated %d downstream target(s) for dependent steps [%s] (hashes=%s)",
				len(desired),
				stepSummary,
				hashText,
			),
		)
	} else {
		logger.Info("Cleared StepRun downstream targets", "previousEndpointHashes", prevHashes)
		hashText := strings.Join(prevHashes, ",")
		if hashText == "" {
			hashText = metrics.EmptyHashSummary
		}
		r.emitStepRunLabeledEvent(
			ctx,
			srun,
			corev1.EventTypeNormal,
			eventReasonDownstreamTargetsCleared,
			fmt.Sprintf(
				"Cleared StepRun downstream targets for dependent steps [%s] (previous hashes=%s)",
				stepSummary,
				hashText,
			),
		)
	}
	return nil
}

// summarizeDownstreamTargetHashes generates short SHA256 hashes for logging.
//
// Behavior:
//   - Returns nil for empty targets.
//   - Hashes GRPCTarget.Endpoint or Terminate.StopMode.
//   - Returns 8-char hex prefix for each target.
//
// Arguments:
//   - targets []runsv1alpha1.DownstreamTarget: targets to hash.
//
// Returns:
//   - []string: hash prefixes for each target.
func summarizeDownstreamTargetHashes(targets []runsv1alpha1.DownstreamTarget) []string {
	if len(targets) == 0 {
		return nil
	}
	hashes := make([]string, 0, len(targets))
	for _, target := range targets {
		var payload string
		switch {
		case target.GRPCTarget != nil:
			payload = strings.TrimSpace(target.GRPCTarget.Endpoint)
		case target.Terminate != nil:
			payload = string(target.Terminate.StopMode)
		}
		if payload == "" {
			continue
		}
		sum := sha256.Sum256([]byte(payload))
		hashes = append(hashes, fmt.Sprintf("%.8x", sum[:4]))
	}
	return hashes
}

// buildBaseEnvVars constructs the base environment variables for step execution.
//
// Behavior:
//   - Sets story/step/run context variables.
//   - Sets runtime config from operator config.
//   - Includes downward API variables for pod metadata.
//
// Arguments:
//   - srun *runsv1alpha1.StepRun: the StepRun.
//   - story *v1alpha1.Story: the parent Story.
//   - inputBytes []byte: serialized inputs.
//   - stepTimeout time.Duration: the timeout value.
//   - executionMode string: job/hybrid mode.
//   - debugEnabled bool: whether debug logs are enabled.
//
// Returns:
//   - []corev1.EnvVar: the environment variables.
func (r *StepRunReconciler) buildBaseEnvVars(srun *runsv1alpha1.StepRun, story *v1alpha1.Story, inputBytes []byte, stepTimeout time.Duration, executionMode string, resolvedConfig *config.ResolvedExecutionConfig, debugEnabled bool) []corev1.EnvVar {
	storyName := ""
	storyVersion := ""
	if story != nil {
		storyName = story.Name
		storyVersion = strings.TrimSpace(story.Spec.Version)
	}
	operatorCfg := r.ConfigResolver.GetOperatorConfig()
	runtimeEnvCfg := toRuntimeEnvConfig(operatorCfg.Controller.Engram.EngramControllerConfig)
	maxInlineSize := runtimeEnvCfg.DefaultMaxInlineSize
	if resolvedConfig != nil {
		maxInlineSize = resolvedConfig.MaxInlineSize
	}

	envVars := []corev1.EnvVar{
		{Name: contracts.StoryNameEnv, Value: storyName},
		{Name: contracts.StoryVersionEnv, Value: storyVersion},
		{Name: contracts.StoryRunIDEnv, Value: srun.Spec.StoryRunRef.Name},
		{Name: contracts.StepNameEnv, Value: srun.Spec.StepID},
		{Name: contracts.StepRunNameEnv, Value: srun.Name},
		{Name: contracts.StepRunNamespaceEnv, Value: srun.Namespace},
		{Name: contracts.TriggerDataEnv, Value: string(inputBytes)},
		{Name: contracts.ExecutionModeEnv, Value: executionMode},
		{Name: contracts.HybridBridgeEnv, Value: strconv.FormatBool(executionMode == executionModeHybrid)},
		{Name: contracts.GRPCPortEnv, Value: fmt.Sprintf("%d", runtimeEnvCfg.DefaultGRPCPort)},
		{Name: contracts.MaxInlineSizeEnv, Value: fmt.Sprintf("%d", maxInlineSize)},
		{Name: contracts.StorageTimeoutEnv, Value: fmt.Sprintf("%ds", runtimeEnvCfg.DefaultStorageTimeoutSeconds)},
		{Name: contracts.StepTimeoutEnv, Value: stepTimeout.String()},
		{Name: contracts.MaxRecursionDepthEnv, Value: "64"},
		{Name: contracts.DebugEnv, Value: strconv.FormatBool(debugEnabled)},
		// Downward API: expose pod metadata to containers via env
		{Name: contracts.PodNameEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: contracts.PodNamespaceEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: contracts.ServiceAccountNameEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.serviceAccountName"}}},
	}
	if isMaterializeStepRun(srun) {
		envVars = append(envVars, corev1.EnvVar{Name: contracts.SkipInputTemplatingEnv, Value: "true"})
	}
	return envVars
}

// addStartedAtEnv appends BUBU_STARTED_AT to env vars via core helper.
//
// Behavior:
//   - Delegates to coreenv.AppendStartedAtEnv for RFC3339Nano timestamp.
//
// Arguments:
//   - envVars *[]corev1.EnvVar: slice to append to.
//   - startedAt metav1.Time: the start timestamp.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Appends to envVars slice.
func (r *StepRunReconciler) addStartedAtEnv(envVars *[]corev1.EnvVar, startedAt metav1.Time) {
	coreenv.AppendStartedAtEnv(envVars, startedAt)
}

// buildJobSpec constructs a Job manifest for the StepRun.
//
// Behavior:
//   - Builds pod template with resolved config and env vars.
//   - Applies storage env to container.
//   - Sets labels for informer selectors.
//
// Arguments:
//   - srun *runsv1alpha1.StepRun: the StepRun owner.
//   - resolvedConfig *config.ResolvedExecutionConfig: resolved settings.
//   - envVars []corev1.EnvVar: environment variables.
//   - envFrom []corev1.EnvFromSource: env sources.
//   - volumes []corev1.Volume: pod volumes.
//   - volumeMounts []corev1.VolumeMount: container mounts.
//   - activeDeadlineSeconds int64: job timeout.
//   - storageTimeout string: formatted storage timeout.
//
// Returns:
//   - *batchv1.Job: the constructed Job.
func (r *StepRunReconciler) buildJobSpec(
	srun *runsv1alpha1.StepRun,
	resolvedConfig *config.ResolvedExecutionConfig,
	envVars []corev1.EnvVar,
	envFrom []corev1.EnvFromSource,
	volumes []corev1.Volume,
	volumeMounts []corev1.VolumeMount,
	activeDeadlineSeconds int64,
	storageTimeout string,
) *batchv1.Job {
	stepRunLabel := safeLabelValue(srun.Name)
	storyRunLegacyLabel := safeLabelValue(srun.Spec.StoryRunRef.Name)
	operatorCfg := r.ConfigResolver.GetOperatorConfig()
	podTemplate := r.buildRuntimePodTemplate(
		resolvedConfig,
		operatorCfg,
		map[string]string{contracts.StepRunLabelKey: stepRunLabel},
		nil,
		envVars,
		envFrom,
		volumes,
		volumeMounts,
		nil,
		resolvedConfig.RestartPolicy,
	)

	if len(podTemplate.Spec.Containers) > 0 {
		podspec.ApplyStorageEnv(resolvedConfig, &podTemplate.Spec, &podTemplate.Spec.Containers[0], storageTimeout)
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      srun.Name,
			Namespace: srun.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "bobrapet",
				"app.kubernetes.io/component": "engram",
				contracts.StepRunLabelKey:     stepRunLabel,
				contracts.StoryRunLegacyLabel: storyRunLegacyLabel,
			},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			BackoffLimit:            &resolvedConfig.BackoffLimit,
			TTLSecondsAfterFinished: &resolvedConfig.TTLSecondsAfterFinished,
			Template:                podTemplate,
		},
	}
}

// buildRuntimePodTemplate constructs PodTemplateSpec for jobs and deployments.
//
// Behavior:
//   - Delegates to podspec.Build for shared pod template construction.
//   - Sets container name to "engram" for StepRun workloads.
//   - Applies termination grace period from operator config.
//
// Arguments:
//   - resolvedConfig *config.ResolvedExecutionConfig: resolved settings.
//   - operatorCfg *config.OperatorConfig: operator config.
//   - labels map[string]string: pod labels.
//   - annotations map[string]string: pod annotations.
//   - envVars []corev1.EnvVar: environment variables.
//   - envFrom []corev1.EnvFromSource: env sources.
//   - volumes []corev1.Volume: pod volumes.
//   - volumeMounts []corev1.VolumeMount: container mounts.
//   - ports []corev1.ContainerPort: container ports.
//   - restartPolicy corev1.RestartPolicy: pod restart policy.
//
// Returns:
//   - corev1.PodTemplateSpec: the constructed template.
func (r *StepRunReconciler) buildRuntimePodTemplate(
	resolvedConfig *config.ResolvedExecutionConfig,
	operatorCfg *config.OperatorConfig,
	labels map[string]string,
	annotations map[string]string,
	envVars []corev1.EnvVar,
	envFrom []corev1.EnvFromSource,
	volumes []corev1.Volume,
	volumeMounts []corev1.VolumeMount,
	ports []corev1.ContainerPort,
	restartPolicy corev1.RestartPolicy,
) corev1.PodTemplateSpec {
	return podspec.Build(podspec.Config{
		ContainerName:             "engram",
		Labels:                    labels,
		Annotations:               annotations,
		EnvVars:                   envVars,
		EnvFrom:                   envFrom,
		Volumes:                   volumes,
		VolumeMounts:              volumeMounts,
		Ports:                     ports,
		RestartPolicy:             restartPolicy,
		TerminationGracePeriodSec: int64(operatorCfg.Controller.Engram.EngramControllerConfig.DefaultTerminationGracePeriodSeconds),
		ResolvedConfig:            resolvedConfig,
	})
}

// getStoryRunInputs fetches initial inputs from the parent StoryRun.
//
// Behavior:
//   - Delegates to runsinputs.DecodeStoryRunInputs.
//
// Arguments:
//   - _ context.Context: unused.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//
// Returns:
//   - map[string]any: decoded inputs.
//   - error: non-nil on decode failure.
func (r *StepRunReconciler) getStoryRunInputs(_ context.Context, storyRun *runsv1alpha1.StoryRun, story *v1alpha1.Story) (map[string]any, error) {
	return runsinputs.ResolveStoryRunInputs(story, storyRun)
}

// getParentStoryRun fetches the StoryRun that owns this StepRun.
//
// Behavior:
//   - Validates storyRunRef.name is present.
//   - Uses refs.LoadNamespacedReference for fetch.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StepRun: the StepRun.
//
// Returns:
//   - *runsv1alpha1.StoryRun: the parent StoryRun.
//   - error: non-nil on missing ref or fetch failure.
func (r *StepRunReconciler) getParentStoryRun(ctx context.Context, srun *runsv1alpha1.StepRun) (*runsv1alpha1.StoryRun, error) {
	if strings.TrimSpace(srun.Spec.StoryRunRef.Name) == "" {
		return nil, fmt.Errorf("steprun %s missing spec.storyRunRef.name", srun.Name)
	}
	storyRun, key, err := refs.LoadNamespacedReference(
		ctx,
		r.Client,
		srun,
		&srun.Spec.StoryRunRef,
		func() *runsv1alpha1.StoryRun { return &runsv1alpha1.StoryRun{} },
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("get parent StoryRun %s: %w", key.String(), err)
	}
	return storyRun, nil
}

// handleJobStatus reconciles StepRun status with observed Job status.
//
// Behavior:
//   - Lets SDK patches win for terminal phases.
//   - Applies fallback updates when SDK fails to write.
//   - Extracts exit codes for granular failure handling.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to update.
//   - job *batchv1.Job: the observed Job.
//
// Returns:
//   - ctrl.Result: reconcile result.
//   - error: non-nil on patch failures.
func (r *StepRunReconciler) handleJobStatus(ctx context.Context, step *runsv1alpha1.StepRun, job *batchv1.Job) (ctrl.Result, error) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(step).WithValues("job_name", job.Name)

	switch {
	case job.Status.Succeeded > 0:
		return r.handleJobSucceeded(ctx, step, stepLogger)
	case job.Status.Failed > 0:
		return r.handleJobFailed(ctx, step, job, stepLogger)
	default:
		return ctrl.Result{}, nil
	}
}

func (r *StepRunReconciler) handleJobSucceeded(ctx context.Context, step *runsv1alpha1.StepRun, logger *logging.ControllerLogger) (ctrl.Result, error) {
	logger.Info("Job succeeded. Verifying StepRun status.")
	if err := r.validateStepRunOutput(ctx, step); err != nil {
		logger.Error(err, "StepRun output failed schema validation")
		if patchErr := r.failStepRunOutputValidation(ctx, step, err); patchErr != nil {
			logger.Error(patchErr, "Failed to update StepRun status after output validation error")
			return ctrl.Result{}, patchErr
		}
		return ctrl.Result{}, nil
	}
	// Evaluate post-execution verification if defined.
	if postExecErr := r.evaluatePostExecution(ctx, step); postExecErr != nil {
		logger.Info("Post-execution check failed", "error", postExecErr)
		message := fmt.Sprintf("Post-execution verification failed: %v", postExecErr)
		if err := r.setStepRunPhase(ctx, step, enums.PhaseFailed, conditions.ReasonExpressionFailed, message); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	if step.Status.Phase != enums.PhaseSucceeded {
		logger.Info("Job pod exited 0, but Engram SDK did not patch StepRun status to Succeeded. Controller will mark as Succeeded as a fallback.")
		successMsg := "StepRun pod completed successfully; final status updated by controller."
		if err := r.setStepRunPhase(ctx, step, enums.PhaseSucceeded, conditions.ReasonCompleted, successMsg); err != nil {
			logger.Error(err, "Failed to update StepRun status to Succeeded")
			return ctrl.Result{}, err
		}
		r.emitStepRunLabeledEvent(ctx, step, corev1.EventTypeNormal, eventReasonJobPatched, "StepRun job succeeded but controller patched status because the SDK had not updated it")
	}
	// Record output timestamp for staleness detection.
	if step.Status.Output != nil && len(step.Status.Output.Raw) > 0 && step.Status.LastOutputAt == nil {
		now := metav1.Now()
		if err := kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
			sr := obj.(*runsv1alpha1.StepRun)
			sr.Status.LastOutputAt = &now
		}); err != nil {
			logger.Error(err, "Failed to set LastOutputAt")
			// Non-fatal — don't block success.
		}
	}
	r.emitStepRunLabeledEvent(ctx, step, corev1.EventTypeNormal, eventReasonJobSucceeded, "StepRun job completed successfully")
	// Warn if output keys don't match declared keys (informational only).
	r.checkDeclaredOutputKeys(ctx, step, logger)
	r.maybeWriteCache(ctx, step, logger)
	return ctrl.Result{}, nil
}

func (r *StepRunReconciler) handleJobFailed(ctx context.Context, step *runsv1alpha1.StepRun, job *batchv1.Job, logger *logging.ControllerLogger) (ctrl.Result, error) {
	exitCode := r.extractPodExitCode(ctx, job)
	exitClass := step.Status.ExitClass
	if exitClass == "" {
		exitClass = classifyExitCode(exitCode)
	}
	failureMsg := step.Status.LastFailureMsg
	if failureMsg == "" {
		failureMsg = defaultFailureMessage(exitCode)
	}
	if scheduled, result, err := r.scheduleRetryIfNeeded(ctx, step, job, exitCode, exitClass, failureMsg, logger); scheduled || err != nil {
		return result, err
	}

	if r.stepStatusPatchedBySDK(step) {
		logger.Info("Job failed but SDK already updated StepRun status; preserving SDK's error details",
			"podExitCode", exitCode,
			"currentPhase", step.Status.Phase)
		r.ensureExitCodePatched(ctx, step, exitCode, logger)
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.applyFailureFallback(ctx, step, exitCode, logger)
}

func (r *StepRunReconciler) stepStatusPatchedBySDK(step *runsv1alpha1.StepRun) bool {
	return step.Status.Phase == enums.PhaseFailed || step.Status.Phase == enums.PhaseTimeout
}

func (r *StepRunReconciler) ensureExitCodePatched(ctx context.Context, step *runsv1alpha1.StepRun, exitCode int, logger *logging.ControllerLogger) {
	if exitCode == 0 || step.Status.ExitCode != 0 {
		return
	}
	if err := kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.ExitCode = int32(exitCode)
		sr.Status.ExitClass = classifyExitCode(exitCode)
	}); err != nil {
		logger.Error(err, "Failed to patch exit code into StepRun status")
	}
}

func (r *StepRunReconciler) validateStepRunOutput(ctx context.Context, step *runsv1alpha1.StepRun) error {
	if step == nil || step.Spec.EngramRef == nil {
		return nil
	}
	engram, err := r.getEngramForStep(ctx, step)
	if err != nil {
		return fmt.Errorf("resolve engram for output validation: %w", err)
	}
	template, err := r.getEngramTemplateForEngram(ctx, engram)
	if err != nil {
		return fmt.Errorf("resolve engram template for output validation: %w", err)
	}
	if template.Spec.OutputSchema == nil || len(template.Spec.OutputSchema.Raw) == 0 {
		return nil
	}
	schemaName := fmt.Sprintf("EngramTemplate %s output", template.Name)
	if err := validateJSONOutputRaw(step.Status.Output, template.Spec.OutputSchema, schemaName); err != nil {
		return err
	}
	return nil
}

func (r *StepRunReconciler) failStepRunOutputValidation(ctx context.Context, step *runsv1alpha1.StepRun, validationErr error) error {
	message := fmt.Sprintf("StepRun output failed schema validation: %v", validationErr)
	if err := r.setStepRunPhase(ctx, step, enums.PhaseFailed, conditions.ReasonOutputSchemaFailed, message); err != nil {
		return err
	}
	return kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.LastFailureMsg = message
		sr.Status.ExitClass = enums.ExitClassTerminal
		sr.Status.NextRetryAt = nil
	})
}

// evaluatePostExecution checks the optional PostExecution condition defined on the
// Story step. If the condition evaluates to false, an error is returned so the
// caller can fail the StepRun with a verification error.
func (r *StepRunReconciler) evaluatePostExecution(ctx context.Context, step *runsv1alpha1.StepRun) error {
	story, err := r.getStoryForStep(ctx, step)
	if err != nil {
		return nil // Don't block on transient errors
	}
	storyStep := findStoryStep(story, step.Spec.StepID)
	if storyStep == nil || storyStep.PostExecution == nil {
		return nil
	}

	// Build vars with step's own output.
	vars := map[string]any{}
	if step.Status.Output != nil && len(step.Status.Output.Raw) > 0 {
		var output any
		if err := json.Unmarshal(step.Status.Output.Raw, &output); err == nil {
			vars["steps"] = map[string]any{
				step.Spec.StepID: map[string]any{
					"output": output,
				},
			}
		}
	}

	result, err := r.TemplateEvaluator.EvaluateCondition(ctx, storyStep.PostExecution.Condition, vars)
	if err != nil {
		return fmt.Errorf("post-execution condition evaluation error: %w", err)
	}
	if !result {
		msg := "post-execution condition evaluated to false"
		if storyStep.PostExecution.FailureMessage != "" {
			msg = storyStep.PostExecution.FailureMessage
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

func (r *StepRunReconciler) failStepRunInputValidation(ctx context.Context, step *runsv1alpha1.StepRun, validationErr error) error {
	message := fmt.Sprintf("StepRun input failed schema validation: %v", validationErr)
	if err := r.setStepRunPhase(ctx, step, enums.PhaseFailed, conditions.ReasonInputSchemaFailed, message); err != nil {
		return err
	}
	return kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.LastFailureMsg = message
		sr.Status.ExitClass = enums.ExitClassTerminal
		sr.Status.NextRetryAt = nil
	})
}

func (r *StepRunReconciler) ensureStepRunSchemaRefs(ctx context.Context, step *runsv1alpha1.StepRun, engram *v1alpha1.Engram, template *catalogv1alpha1.EngramTemplate) error {
	if step == nil || engram == nil || template == nil {
		return nil
	}

	version := resolveSchemaVersion(step.Spec.EngramRef.Version, engram.Spec.Version)
	var inputRef *runsv1alpha1.SchemaReference
	if template.Spec.InputSchema != nil && len(template.Spec.InputSchema.Raw) > 0 {
		inputRef = engramSchemaRef(engram.Namespace, engram.Name, version, "input")
	}
	var outputRef *runsv1alpha1.SchemaReference
	if template.Spec.OutputSchema != nil && len(template.Spec.OutputSchema.Raw) > 0 {
		outputRef = engramSchemaRef(engram.Namespace, engram.Name, version, "output")
	}

	if schemaRefEqual(step.Status.InputSchemaRef, inputRef) && schemaRefEqual(step.Status.OutputSchemaRef, outputRef) {
		return nil
	}

	return kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.InputSchemaRef = inputRef
		sr.Status.OutputSchemaRef = outputRef
	})
}

func (r *StepRunReconciler) scheduleRetryIfNeeded(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	job *batchv1.Job,
	exitCode int,
	exitClass enums.ExitClass,
	failureMsg string,
	logger *logging.ControllerLogger,
) (bool, ctrl.Result, error) {
	if !retryableExitClass(exitClass) {
		return false, ctrl.Result{}, nil
	}

	policy := r.resolveRetryPolicy(step)
	maxRetries := int32(0)
	if policy != nil && policy.MaxRetries != nil {
		maxRetries = *policy.MaxRetries
	}
	if maxRetries <= 0 {
		return false, ctrl.Result{}, nil
	}
	if step.Status.Retries >= maxRetries {
		return false, ctrl.Result{}, nil
	}

	attempt := step.Status.Retries + 1
	// Unknown exit class: retry without counting against budget (infrastructure failure)
	if exitClass == enums.ExitClassUnknown {
		attempt = step.Status.Retries // don't increment
	}
	delay := computeRetryDelay(policy, attempt, logger)
	nextRetry := metav1.NewTime(time.Now().Add(delay))
	message := fmt.Sprintf("Retrying in %s (attempt %d/%d)", delay.String(), attempt, maxRetries)

	if err := r.setStepRunPhase(ctx, step, enums.PhasePending, conditions.ReasonRetryScheduled, message); err != nil {
		return true, ctrl.Result{}, err
	}
	if err := kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.Retries = attempt
		sr.Status.NextRetryAt = &nextRetry
		sr.Status.LastFailureMsg = failureMsg
		sr.Status.FinishedAt = nil
		sr.Status.Duration = ""
		if sr.Status.ExitClass == "" {
			sr.Status.ExitClass = exitClass
		}
		if exitCode != 0 {
			sr.Status.ExitCode = int32(exitCode)
		}
	}); err != nil {
		return true, ctrl.Result{}, err
	}

	if job != nil {
		if deleteErr := r.Delete(ctx, job); deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
			logger.Error(deleteErr, "Failed to delete failed Job before retry")
			return true, ctrl.Result{}, deleteErr
		}
	}

	engramLabel := metrics.UnknownLabelValue
	if step != nil && step.Spec.EngramRef != nil && step.Spec.EngramRef.Name != "" {
		engramLabel = step.Spec.EngramRef.Name
	}
	metrics.StepRunRetries.WithLabelValues(step.Namespace, engramLabel, string(exitClass)).Inc()

	r.emitStepRunLabeledEvent(
		ctx,
		step,
		corev1.EventTypeWarning,
		eventReasonJobRetryScheduled,
		fmt.Sprintf("Scheduled retry %d/%d in %s", attempt, maxRetries, delay.String()),
	)
	return true, ctrl.Result{RequeueAfter: delay}, nil
}

func (r *StepRunReconciler) resolveRetryPolicy(step *runsv1alpha1.StepRun) *v1alpha1.RetryPolicy {
	var policy *v1alpha1.RetryPolicy
	if step != nil && step.Spec.Retry != nil {
		policy = step.Spec.Retry.DeepCopy()
	}
	var cfg *config.ControllerConfig
	if r != nil && r.ConfigResolver != nil && r.ConfigResolver.GetOperatorConfig() != nil {
		cfg = &r.ConfigResolver.GetOperatorConfig().Controller
	}
	return webhookshared.ResolveRetryPolicy(cfg, policy)
}

func computeRetryDelay(policy *v1alpha1.RetryPolicy, attempt int32, logger *logging.ControllerLogger) time.Duration {
	baseDelay := webhookshared.DefaultRetryDelay
	if policy != nil && policy.Delay != nil && *policy.Delay != "" {
		if parsed, err := parsePositiveDuration(*policy.Delay); err == nil {
			baseDelay = parsed
		} else if logger != nil {
			logger.Error(err, "Invalid retry delay; falling back to default", "delay", *policy.Delay)
		}
	}

	var maxDelay time.Duration
	if policy != nil && policy.MaxDelay != nil && *policy.MaxDelay != "" {
		if parsed, err := parsePositiveDuration(*policy.MaxDelay); err == nil {
			maxDelay = parsed
		} else if logger != nil {
			logger.Error(err, "Invalid retry maxDelay; ignoring", "maxDelay", *policy.MaxDelay)
		}
	}
	if maxDelay > 0 && maxDelay < config.MinRequeueDelay {
		if logger != nil {
			logger.V(1).Info("Retry maxDelay below minimum; clamping to minimum", "maxDelay", maxDelay.String(), "minDelay", config.MinRequeueDelay.String())
		}
		maxDelay = config.MinRequeueDelay
	}

	jitter := 0.0
	if policy != nil && policy.Jitter != nil {
		jitterPct := *policy.Jitter
		if jitterPct < 0 {
			jitterPct = 0
		} else if jitterPct > 100 {
			jitterPct = 100
		}
		jitter = float64(jitterPct) / 100.0
	}

	if attempt < 1 {
		attempt = 1
	}
	strategy := enums.BackoffStrategyExponential
	if policy != nil && policy.Backoff != nil {
		strategy = *policy.Backoff
	}

	delay := baseDelay
	switch strategy {
	case enums.BackoffStrategyLinear:
		delay = baseDelay * time.Duration(attempt)
	case enums.BackoffStrategyConstant:
		delay = baseDelay
	default:
		multiplier := int64(1) << uint(attempt-1)
		delay = baseDelay * time.Duration(multiplier)
	}

	if maxDelay > 0 && delay > maxDelay {
		delay = maxDelay
	}
	if jitter > 0 {
		delay = wait.Jitter(delay, jitter)
		if maxDelay > 0 && delay > maxDelay {
			delay = maxDelay
		}
	}

	if delay < config.MinRequeueDelay {
		delay = config.MinRequeueDelay
	}
	return delay
}

func retryableExitClass(exitClass enums.ExitClass) bool {
	switch exitClass {
	case enums.ExitClassRetry, enums.ExitClassRateLimited, enums.ExitClassUnknown:
		return true
	default:
		return false
	}
}

func defaultFailureMessage(exitCode int) string {
	if exitCode == 124 {
		return "Job execution timed out. Check step timeout configuration and pod logs."
	}
	return "Job execution failed. Check pod logs for details."
}

func (r *StepRunReconciler) clearNextRetryAt(ctx context.Context, step *runsv1alpha1.StepRun) error {
	return kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.NextRetryAt = nil
	})
}

func (r *StepRunReconciler) applyFailureFallback(ctx context.Context, step *runsv1alpha1.StepRun, exitCode int, logger *logging.ControllerLogger) error {
	logger.Info("Job failed and SDK did not update status; applying fallback status", "podExitCode", exitCode)

	phase := enums.PhaseFailed
	message := defaultFailureMessage(exitCode)
	if exitCode == 124 {
		phase = enums.PhaseTimeout
	}

	if err := r.setStepRunPhase(ctx, step, phase, "", message); err != nil {
		logger.Error(err, "Failed to update StepRun status after job failure")
		return err
	}

	if err := kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.LastFailureMsg = message
		if exitCode != 0 {
			sr.Status.ExitCode = int32(exitCode)
			sr.Status.ExitClass = classifyExitCode(exitCode)
		}
	}); err != nil {
		logger.Error(err, "Failed to patch StepRun failure metadata after job failure")
		return err
	}

	eventMsg := fmt.Sprintf("StepRun job failed (exit=%d); controller patched status because the SDK had not updated it", exitCode)
	r.emitStepRunLabeledEvent(ctx, step, corev1.EventTypeWarning, eventReasonJobFallback, eventMsg)
	return nil
}

// extractPodExitCode retrieves exit code from the Job's failed pod.
//
// Behavior:
//   - Lists pods owned by the Job.
//   - Returns exit code from most recent failed pod.
//   - Returns -1 if pod or exit code cannot be determined.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - job *batchv1.Job: the Job to inspect.
//
// Returns:
//   - int: the exit code (-1 if unknown).
func (r *StepRunReconciler) extractPodExitCode(ctx context.Context, job *batchv1.Job) int {
	// List pods owned by this Job
	var podList corev1.PodList
	if err := kubeutil.ListByLabels(ctx, r.Client, job.Namespace, map[string]string{"job-name": job.Name}, &podList); err != nil {
		return -1
	}

	// Find the most recent failed pod
	for i := len(podList.Items) - 1; i >= 0; i-- {
		pod := &podList.Items[i]
		if pod.Status.Phase == corev1.PodFailed {
			// Check container statuses for exit code
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
					return int(cs.State.Terminated.ExitCode)
				}
			}
		}
	}
	return -1
}

// hasActiveJobPods checks for running/pending pods for the StepRun's Job name.
func (r *StepRunReconciler) hasActiveJobPods(ctx context.Context, step *runsv1alpha1.StepRun) (bool, int, error) {
	var podList corev1.PodList
	if err := kubeutil.ListByLabels(ctx, r.Client, step.Namespace, map[string]string{"job-name": step.Name}, &podList); err != nil {
		return false, 0, err
	}
	active := 0
	for i := range podList.Items {
		switch podList.Items[i].Status.Phase {
		case corev1.PodPending, corev1.PodRunning, corev1.PodUnknown:
			active++
		}
	}
	return active > 0, active, nil
}

func (r *StepRunReconciler) ensureStepRunJobUID(ctx context.Context, step *runsv1alpha1.StepRun, job *batchv1.Job, log *logging.ControllerLogger) {
	if step == nil || job == nil {
		return
	}
	jobUID := string(job.UID)
	if strings.TrimSpace(jobUID) == "" {
		return
	}
	patch := client.MergeFrom(step.DeepCopy())
	if step.Annotations == nil {
		step.Annotations = map[string]string{}
	}
	prevUID := strings.TrimSpace(step.Annotations[stepRunJobUIDAnnotation])
	if prevUID == jobUID {
		return
	}
	if prevUID != "" {
		bumpStepRunRestartAnnotations(step.Annotations)
		if r.Recorder != nil {
			r.Recorder.Eventf(step, corev1.EventTypeWarning, eventReasonJobRestarted, "StepRun Job UID changed (previous=%s new=%s)", prevUID, jobUID)
		}
		if log != nil {
			log.Info("Recorded StepRun restart after Job UID change", "previousJobUID", prevUID, "newJobUID", jobUID)
		}
	}
	step.Annotations[stepRunJobUIDAnnotation] = jobUID
	if err := r.Patch(ctx, step, patch); err != nil && log != nil {
		log.Error(err, "Failed to patch StepRun Job UID annotation")
	}
}

func (r *StepRunReconciler) markStepRunRestart(ctx context.Context, step *runsv1alpha1.StepRun, reason string, log *logging.ControllerLogger) error {
	if step == nil {
		return nil
	}
	prevUID := ""
	if step.Annotations != nil {
		prevUID = strings.TrimSpace(step.Annotations[stepRunJobUIDAnnotation])
	}
	if prevUID == "" {
		return nil
	}
	patch := client.MergeFrom(step.DeepCopy())
	if step.Annotations == nil {
		step.Annotations = map[string]string{}
	}
	delete(step.Annotations, stepRunJobUIDAnnotation)
	bumpStepRunRestartAnnotations(step.Annotations)
	if err := r.Patch(ctx, step, patch); err != nil {
		return err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(step, corev1.EventTypeWarning, eventReasonJobRestarted, "Restarting StepRun after Job went missing (reason=%s previousJobUID=%s)", reason, prevUID)
	}
	if log != nil {
		log.Info("Recorded StepRun restart before Job creation", "reason", reason, "previousJobUID", prevUID)
	}
	return nil
}

func bumpStepRunRestartAnnotations(annotations map[string]string) {
	if annotations == nil {
		return
	}
	count := 0
	if raw := strings.TrimSpace(annotations[stepRunRestartCountAnnotation]); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			count = parsed
		}
	}
	count++
	annotations[stepRunRestartCountAnnotation] = strconv.Itoa(count)
	annotations[stepRunRestartedAtAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
}

// reconcileRunScopedRealtimeStep loads the parent StoryRun, resolves runtime
// config, ensures the per-step TransportBinding, and reconciles the run-scoped
// Service + Deployment before updating StepRun status/conditions based on
// binding/deployment readiness (internal/controller/runs/steprun_controller.go:1090-1197).
// Behavior:
//   - Fetches the StoryRun + execution config, skips pre-evaluating realtime inputs, and computes the step/storage
//     timeouts plus the deterministic binding name (lines 1092-1114).
//   - Calls ensureRunTransportBinding; paused vs blocked phases and the next requeue delay depend on whether the
//     binding is pending or failed (lines 1115-1131).
//   - Resolves the binding env payload, writes sanitized Engram annotations, and decodes the payload to determine
//     insecure transport allowances before reconciling the Service and Deployment (lines 1133-1159,1484-1504).
//   - Reads TransportBinding + Deployment readiness, sets the StepRun phase and transport Ready condition, and
//     requeues until both report Ready (lines 1160-1194).
//
// Args:
//   - ctx context.Context: propagate cancellation to client calls.
//   - step *runsv1alpha1.StepRun: resource receiving status updates.
//   - story *v1alpha1.Story: optional Story spec used when building env vars.
//   - stepSpec *v1alpha1.Step: defines realtime mode specifics passed into binding/workload builders.
//   - engram *v1alpha1.Engram: influences binding permissions and insecure transport checks.
//   - template *catalogv1alpha1.EngramTemplate: forwarded to workload builders.
//
// Returns:
//   - ctrl.Result: requeues with nextRequeueDelay until dependencies report ready; returns empty result once ready.
//   - error: bubbled up from binding/service/deployment reconciliation or status patches.
func (r *StepRunReconciler) reconcileRunScopedRealtimeStep(ctx context.Context, step *runsv1alpha1.StepRun, story *v1alpha1.Story, stepSpec *v1alpha1.Step, engram *v1alpha1.Engram, template *catalogv1alpha1.EngramTemplate) (ctrl.Result, error) {
	rtx, err := r.buildRealtimeRuntimeContext(ctx, step, story, stepSpec, engram, template)
	if err != nil {
		return ctrl.Result{}, err
	}

	binding, result, err := r.ensureRealtimeBinding(ctx, step, story, stepSpec, rtx)
	if result != nil || err != nil {
		if result != nil {
			return *result, err
		}
		return ctrl.Result{}, err
	}

	envValue, bindingInfo := r.prepareBindingEnv(ctx, step, binding, rtx)
	if err := r.ensureEngramTransportAnnotation(ctx, engram, envValue, step); err != nil {
		rtx.logger.Error(err, "Failed to annotate engram with transport binding")
	}

	allowInsecure := transportutil.AllowInsecureTransport(engram, bindingInfo)
	if err := r.ensureRealtimeService(ctx, step, story, rtx); err != nil {
		return ctrl.Result{}, err
	}

	return r.ensureRealtimeDeployment(ctx, step, story, engram, template, binding, envValue, bindingInfo, allowInsecure, rtx)
}

type realtimeRuntimeContext struct {
	logger         *logging.ControllerLogger
	storyRun       *runsv1alpha1.StoryRun
	resolvedCfg    *config.ResolvedExecutionConfig
	stepTimeout    time.Duration
	storageTimeout string
	bindingName    string
}

func (r *StepRunReconciler) buildRealtimeRuntimeContext(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	stepSpec *v1alpha1.Step,
	engram *v1alpha1.Engram,
	template *catalogv1alpha1.EngramTemplate,
) (*realtimeRuntimeContext, error) {
	logger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step).WithValues("mode", "run-scoped-realtime")

	storyRun, err := r.getParentStoryRun(ctx, step)
	if err != nil {
		logger.Error(err, "Failed to load parent StoryRun")
		return nil, err
	}

	resolvedCfg, err := r.ConfigResolver.ResolveExecutionConfig(ctx, step, story, engram, template, stepSpec)
	if err != nil {
		logger.Error(err, "Failed to resolve execution config for realtime StepRun")
		return nil, err
	}

	stepTimeout := r.computeStepTimeout(step, story, logger)
	runtimeEnvCfg := toRuntimeEnvConfig(r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig)
	storageTimeout := podspec.FormatStorageTimeout(resolvedCfg, runtimeEnvCfg.DefaultStorageTimeoutSeconds)
	bindingName := transportbinding.Name(step.Spec.StoryRunRef.Name, step.Spec.StepID)

	return &realtimeRuntimeContext{
		logger:         logger,
		storyRun:       storyRun,
		resolvedCfg:    resolvedCfg,
		stepTimeout:    stepTimeout,
		storageTimeout: storageTimeout,
		bindingName:    bindingName,
	}, nil
}

func (r *StepRunReconciler) ensureRealtimeBinding(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	stepSpec *v1alpha1.Step,
	rtx *realtimeRuntimeContext,
) (*transportv1alpha1.TransportBinding, *ctrl.Result, error) {
	binding, err := r.ensureRunTransportBinding(ctx, step, rtx.storyRun, story, stepSpec, rtx.resolvedCfg, rtx.bindingName)
	switch err {
	case nil:
		return binding, nil, nil
	case errTransportBindingPending:
		r.handleBindingPending(ctx, step, rtx)
		res := ctrl.Result{RequeueAfter: r.nextRequeueDelay()}
		return nil, &res, nil
	default:
		r.handleBindingFailure(ctx, step, err, rtx)
		res := ctrl.Result{RequeueAfter: r.nextRequeueDelay()}
		return nil, &res, err
	}
}

func (r *StepRunReconciler) handleBindingPending(ctx context.Context, step *runsv1alpha1.StepRun, rtx *realtimeRuntimeContext) {
	rtx.logger.V(1).Info("Transport binding not observed yet; waiting for informer cache", "binding", rtx.bindingName)
	if err := r.setStepRunPhase(ctx, step, enums.PhasePaused, conditions.ReasonAwaitingTransport, "Waiting for transport binding to become available"); err != nil {
		rtx.logger.Error(err, "Failed to update StepRun status while waiting for transport binding")
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(
			step,
			corev1.EventTypeNormal,
			eventReasonTransportBindingPending,
			"Waiting for transport binding %s to become visible",
			rtx.bindingName,
		)
	}
}

func (r *StepRunReconciler) handleBindingFailure(ctx context.Context, step *runsv1alpha1.StepRun, err error, rtx *realtimeRuntimeContext) {
	rtx.logger.Error(err, "Failed to ensure transport binding for run-scoped realtime step")
	statusErr := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, conditions.ReasonTransportFailed, fmt.Sprintf("Transport binding error: %v", err))
	if statusErr != nil {
		rtx.logger.Error(statusErr, "Failed to update StepRun status after transport binding failure")
	}
}

func (r *StepRunReconciler) prepareBindingEnv(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	binding *transportv1alpha1.TransportBinding,
	rtx *realtimeRuntimeContext,
) (string, *transportpb.BindingInfo) {
	bindingEnvValue, bindingResolved := r.resolveBindingEnvValue(ctx, step.Namespace, binding.Name)
	if !bindingResolved {
		rtx.logger.V(1).Info("Falling back to binding name for env payload", "binding", rtx.bindingName)
	}
	bindingInfo, decodeErr := transportutil.DecodeBindingInfo(bindingEnvValue)
	if decodeErr != nil {
		rtx.logger.Info("Failed to decode transport binding info; env overrides may be incomplete", "binding", rtx.bindingName, "error", decodeErr)
		if r.Recorder != nil {
			r.Recorder.Eventf(
				step,
				corev1.EventTypeWarning,
				eventReasonTransportBindingDecodeFailed,
				"Failed to decode env payload for binding %s: %v",
				rtx.bindingName,
				decodeErr,
			)
		}
	}
	return bindingEnvValue, bindingInfo
}

func (r *StepRunReconciler) ensureRealtimeService(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	rtx *realtimeRuntimeContext,
) error {
	service := r.desiredRunScopedService(step, story, rtx.resolvedCfg)
	if _, err := r.reconcileRunScopedService(ctx, step, service); err != nil {
		rtx.logger.Error(err, "Failed to reconcile Service for realtime StepRun")
		return err
	}
	return nil
}

func (r *StepRunReconciler) ensureRealtimeDeployment(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	engram *v1alpha1.Engram,
	template *catalogv1alpha1.EngramTemplate,
	binding *transportv1alpha1.TransportBinding,
	bindingEnvValue string,
	bindingInfo *transportpb.BindingInfo,
	allowInsecure bool,
	rtx *realtimeRuntimeContext,
) (ctrl.Result, error) {
	deployment, err := r.desiredRunScopedDeployment(ctx, step, story, rtx.storyRun, engram, template, rtx.resolvedCfg, rtx.storageTimeout, rtx.stepTimeout, bindingEnvValue, bindingInfo, allowInsecure)
	if err != nil {
		rtx.logger.Error(err, "Failed to construct Deployment for realtime StepRun")
		if statusErr := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, conditions.ReasonBlocked, fmt.Sprintf("Failed to construct Deployment: %v", err)); statusErr != nil {
			rtx.logger.Error(statusErr, "Failed to update StepRun phase after Deployment construction failure")
		}
		return ctrl.Result{}, err
	}
	lifecycle := resolveLifecycleSettings(binding, rtx.logger)
	applyRealtimeDeploymentStrategy(deployment, lifecycle)
	currentDeployment, err := r.reconcileRunScopedDeployment(ctx, step, deployment)
	if err != nil {
		rtx.logger.Error(err, "Failed to reconcile Deployment for realtime StepRun", "deployment", deployment.Name)
		if statusErr := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, conditions.ReasonBlocked, fmt.Sprintf("Failed to reconcile Deployment: %v", err)); statusErr != nil {
			rtx.logger.Error(statusErr, "Failed to update StepRun phase after Deployment reconciliation failure")
		}
		return ctrl.Result{}, err
	}

	bindingReady, _, bindingMsg := transportbinding.ReadyState(binding)
	deploymentReady, deploymentMsg := deploymentReadyState(currentDeployment)

	phase, message, conditionMsg := deriveRealtimePhase(bindingReady, bindingMsg, deploymentReady, deploymentMsg)
	if err := r.setStepRunPhase(ctx, step, phase, "", message); err != nil {
		rtx.logger.Error(err, "Failed to update StepRun phase for realtime execution")
		return ctrl.Result{}, err
	}
	if err := r.setTransportCondition(ctx, step, bindingReady, conditionMsg); err != nil {
		rtx.logger.Error(err, "Failed to update transport condition on StepRun")
		return ctrl.Result{}, err
	}
	if err := r.setStepRunHandoff(ctx, step, deriveHandoffStatus(lifecycle, currentDeployment)); err != nil {
		rtx.logger.Error(err, "Failed to update handoff status on StepRun")
		return ctrl.Result{}, err
	}

	if !bindingReady || !deploymentReady {
		return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
	}

	rtx.logger.Info("Run-scoped realtime runtime is ready", "deployment", currentDeployment.Name)
	return ctrl.Result{}, nil
}

func deriveRealtimePhase(bindingReady bool, bindingMsg string, deploymentReady bool, deploymentMsg string) (enums.Phase, string, string) {
	if !bindingReady {
		if bindingMsg == "" {
			bindingMsg = "transport binding is not ready"
		}
		return enums.PhasePaused, bindingMsg, bindingMsg
	}
	if !deploymentReady {
		if deploymentMsg == "" {
			deploymentMsg = "waiting for realtime deployment readiness"
		}
		return enums.PhaseRunning, deploymentMsg, bindingMsg
	}
	return enums.PhaseRunning, "Realtime runtime is ready", bindingMsg
}

// resolveRunScopedInputs merges and evaluates inputs for the StepRun.
//
// Behavior:
//   - Merges StoryRun inputs, prior step outputs, and step input block.
//   - Evaluates with templates to produce final JSON payload.
//   - Applies input schema defaults when present.
//   - Validates resolved inputs against the schema.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun with input spec.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *v1alpha1.Story: the parent Story (optional for inputs resolution).
//   - inputSchema *runtime.RawExtension: optional schema for defaults/validation.
//   - schemaName string: label for validation errors.
//
// Returns:
//   - []byte: marshaled resolved inputs.
//   - error: non-nil on resolution or marshal failures.
func (r *StepRunReconciler) resolveRunScopedInputs(ctx context.Context, step *runsv1alpha1.StepRun, storyRun *runsv1alpha1.StoryRun, story *v1alpha1.Story, inputSchema *runtime.RawExtension, schemaName string) ([]byte, error) {
	if schemaName == "" {
		schemaName = "Step inputs"
	}

	var resolvedInputs map[string]any
	if isMaterializeStepRun(step) {
		if step == nil || step.Spec.Input == nil || len(step.Spec.Input.Raw) == 0 {
			resolvedInputs = map[string]any{}
		} else {
			if err := json.Unmarshal(step.Spec.Input.Raw, &resolvedInputs); err != nil {
				return nil, fmt.Errorf("failed to unmarshal materialize step input: %w", err)
			}
		}
	} else {
		if step != nil && step.Spec.Input != nil && len(step.Spec.Input.Raw) > 0 {
			if err := templatesafety.ValidateTemplateJSON(step.Spec.Input.Raw); err != nil {
				return nil, err
			}
		}
		storyInputs, err := r.getStoryRunInputs(ctx, storyRun, story)
		if err != nil {
			return nil, fmt.Errorf("failed to get storyrun inputs: %w", err)
		}
		stepOutputs, err := getPriorStepOutputs(ctx, r.Client, storyRun, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get prior step outputs: %w", err)
		}

		withBlock := map[string]any{}
		if step.Spec.Input != nil && len(step.Spec.Input.Raw) > 0 {
			if err := json.Unmarshal(step.Spec.Input.Raw, &withBlock); err != nil {
				return nil, fmt.Errorf("failed to unmarshal step input: %w", err)
			}
		}

		vars := map[string]any{
			"inputs": storyInputs,
			"steps":  stepOutputs,
		}
		if reqErr := r.checkRequiresContext(ctx, step, vars); reqErr != nil {
			return nil, reqErr
		}
		resolvedInputs, err = r.TemplateEvaluator.ResolveWithInputs(ctx, withBlock, vars)
		if err != nil {
			// When evaluator hits a ref during resolution it returns ErrOffloadedDataUsage.
			// For step inputs, leave templates for the SDK instead of materializing.
			var offloaded *templating.ErrOffloadedDataUsage
			if errors.As(err, &offloaded) {
				logr.FromContextOrDiscard(ctx).Info(
					"Step input evaluation hit offloaded data; leaving templates for SDK",
					"stepRun", step.Name,
					"stepID", step.Spec.StepID,
					"reason", offloaded.Reason,
				)
				resolvedInputs = withBlock
				err = nil
			}
			if err != nil {
				err = r.handleTemplateEvalError(err)
				return nil, fmt.Errorf("failed to resolve step inputs: %w", err)
			}
		}
		// Evaluator can "succeed" but return a value that contains nested refs (path_eval returns
		// refs as values; ErrOffloadedDataUsage only for len/sample/etc.). For step inputs,
		// keep refs and let the SDK hydrate them at runtime.
		if resolvedInputs != nil && containsStorageRef(resolvedInputs, 0) {
			logr.FromContextOrDiscard(ctx).Info(
				"Step input includes storage refs; skipping controller materialize and leaving refs for SDK",
				"stepRun", step.Name,
				"stepID", step.Spec.StepID,
			)
		}
	}

	if resolvedInputs == nil {
		resolvedInputs = map[string]any{}
	}
	if inputSchema != nil && len(inputSchema.Raw) > 0 {
		resolved, err := runsinputs.ApplySchemaDefaults(inputSchema.Raw, resolvedInputs)
		if err != nil {
			return nil, fmt.Errorf("failed to apply input schema defaults: %w", err)
		}
		resolvedInputs = resolved
	}
	if !isMaterializeStepRun(step) {
		dehydrated, err := r.dehydrateResolvedInputs(ctx, step, resolvedInputs)
		if err != nil {
			return nil, fmt.Errorf("failed to offload step inputs: %w", err)
		}
		resolvedInputs = dehydrated
	}

	inputBytes, err := json.Marshal(resolvedInputs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal resolved step inputs: %w", err)
	}
	maxInlineBytes := r.maxInlineBytes()
	if len(inputBytes) <= maxInlineBytes {
		if err := r.ensureResolvedInputOnStepRun(ctx, step, inputBytes); err != nil {
			return nil, err
		}
	} else {
		logr.FromContextOrDiscard(ctx).Info(
			"Resolved step inputs exceed inline size; skipping spec.input update",
			"stepRun", step.Name,
			"sizeBytes", len(inputBytes),
			"maxBytes", maxInlineBytes,
		)
	}
	if err := validateJSONInputsBytes(inputBytes, inputSchema, schemaName); err != nil {
		if failErr := r.failStepRunInputValidation(ctx, step, err); failErr != nil {
			return nil, failErr
		}
		return nil, fmt.Errorf("%w: %v", errStepRunInputSchemaInvalid, err)
	}
	return inputBytes, nil
}

func (r *StepRunReconciler) ensureResolvedInputOnStepRun(ctx context.Context, step *runsv1alpha1.StepRun, inputBytes []byte) error {
	if step == nil || step.Spec.Input == nil {
		return nil
	}
	if len(step.Spec.Input.Raw) > 0 && bytes.Equal(step.Spec.Input.Raw, inputBytes) {
		return nil
	}

	key := types.NamespacedName{Name: step.Name, Namespace: step.Namespace}
	return retry.OnError(retry.DefaultRetry, apierrors.IsConflict, func() error {
		current := &runsv1alpha1.StepRun{}
		if err := r.Get(ctx, key, current); err != nil {
			return err
		}
		if current.Spec.Input == nil {
			current.Spec.Input = &runtime.RawExtension{}
		}
		if bytes.Equal(current.Spec.Input.Raw, inputBytes) {
			return nil
		}
		original := current.DeepCopy()
		current.Spec.Input.Raw = append([]byte(nil), inputBytes...)
		return r.Patch(ctx, current, client.MergeFrom(original))
	})
}

func (r *StepRunReconciler) maxInlineBytes() int {
	const fallbackMaxInline = 1024
	if r.ConfigResolver == nil {
		return fallbackMaxInline
	}
	cfg := r.ConfigResolver.GetOperatorConfig()
	if cfg == nil {
		return fallbackMaxInline
	}
	if cfg.Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize > 0 {
		return cfg.Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize
	}
	return fallbackMaxInline
}

func (r *StepRunReconciler) dehydrateResolvedInputs(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	inputs map[string]any,
) (map[string]any, error) {
	if step == nil || inputs == nil {
		return inputs, nil
	}
	manager, err := storage.SharedManager(ctx)
	if err != nil {
		return nil, err
	}
	if manager == nil || manager.GetStore() == nil {
		return inputs, nil
	}
	dehydrated, err := manager.DehydrateInputs(ctx, inputs, step.Name)
	if err != nil {
		return nil, err
	}
	dehydratedMap, ok := dehydrated.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("dehydrated step inputs must be object, got %T", dehydrated)
	}
	return dehydratedMap, nil
}

type cacheEntry struct {
	CreatedAt  time.Time       `json:"createdAt"`
	TTLSeconds *int32          `json:"ttlSeconds,omitempty"`
	Output     json.RawMessage `json:"output"`
}

type cacheMetadata struct {
	Key          string
	Path         string
	Mode         string
	TTLSeconds   *int32
	ReadEnabled  bool
	WriteEnabled bool
}

func cachePolicyEnabled(policy *v1alpha1.CachePolicy) bool {
	if policy == nil {
		return false
	}
	if policy.Enabled != nil {
		return *policy.Enabled
	}
	return true
}

func cacheMode(policy *v1alpha1.CachePolicy) string {
	if policy == nil || policy.Mode == nil {
		return "readWrite"
	}
	mode := strings.TrimSpace(*policy.Mode)
	if mode == "" {
		return "readWrite"
	}
	return mode
}

func cacheModeAllowsRead(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "read", "readwrite":
		return true
	default:
		return false
	}
}

func cacheModeAllowsWrite(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "write", "readwrite":
		return true
	default:
		return false
	}
}

func cachePathForStep(step *runsv1alpha1.StepRun, engramName, engramVersion, key string) string {
	if step == nil || step.Spec.EngramRef == nil || engramName == "" || key == "" {
		return ""
	}
	version := strings.TrimSpace(engramVersion)
	if version == "" {
		version = "latest"
	}
	return filepath.Join("cache", step.Namespace, engramName, version, fmt.Sprintf("%s.json", key))
}

func (r *StepRunReconciler) buildCacheMetadata(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	engram *v1alpha1.Engram,
	policy *v1alpha1.CachePolicy,
	inputBytes []byte,
) (*cacheMetadata, error) {
	if step == nil || step.Spec.EngramRef == nil || engram == nil || !cachePolicyEnabled(policy) {
		return nil, nil
	}
	mode := cacheMode(policy)
	key, err := r.resolveCacheKey(ctx, step, engram, policy, inputBytes)
	if err != nil {
		return nil, err
	}
	version := strings.TrimSpace(step.Spec.EngramRef.Version)
	if version == "" {
		version = strings.TrimSpace(engram.Spec.Version)
	}
	path := cachePathForStep(step, engram.Name, version, key)
	if path == "" {
		return nil, nil
	}
	return &cacheMetadata{
		Key:          key,
		Path:         path,
		Mode:         mode,
		TTLSeconds:   policy.TTLSeconds,
		ReadEnabled:  cacheModeAllowsRead(mode),
		WriteEnabled: cacheModeAllowsWrite(mode),
	}, nil
}

func (r *StepRunReconciler) resolveCacheKey(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	engram *v1alpha1.Engram,
	policy *v1alpha1.CachePolicy,
	inputBytes []byte,
) (string, error) {
	if step == nil || engram == nil {
		return "", fmt.Errorf("cache key requires step and engram")
	}
	version := strings.TrimSpace(step.Spec.EngramRef.Version)
	if version == "" {
		version = strings.TrimSpace(engram.Spec.Version)
	}
	if version == "" {
		version = "latest"
	}

	keyMaterial := ""
	if policy != nil && policy.Key != nil && strings.TrimSpace(*policy.Key) != "" {
		if r.TemplateEvaluator == nil {
			return "", fmt.Errorf("template evaluator is unavailable for cache key")
		}
		inputs := map[string]any{}
		if len(inputBytes) > 0 {
			if err := json.Unmarshal(inputBytes, &inputs); err != nil {
				return "", fmt.Errorf("failed to decode inputs for cache key: %w", err)
			}
		}
		vars := map[string]any{
			"inputs": inputs,
			"step": map[string]any{
				"id":    step.Spec.StepID,
				"run":   step.Name,
				"story": step.Spec.StoryRunRef.Name,
			},
			"engram": map[string]any{
				"name":    engram.Name,
				"version": version,
			},
		}
		resolved, err := r.TemplateEvaluator.ResolveTemplateString(ctx, *policy.Key, vars)
		if err != nil {
			return "", fmt.Errorf("failed to resolve cache key template: %w", err)
		}
		switch v := resolved.(type) {
		case string:
			keyMaterial = v
		default:
			encoded, err := json.Marshal(v)
			if err != nil {
				return "", fmt.Errorf("failed to marshal cache key: %w", err)
			}
			keyMaterial = string(encoded)
		}
		keyMaterial = strings.TrimSpace(keyMaterial)
		if keyMaterial == "" {
			return "", fmt.Errorf("cache key resolved to empty value")
		}
		keyMaterial = fmt.Sprintf("engram:%s|version:%s|key:%s", engram.Name, version, keyMaterial)
	} else {
		keyMaterial = fmt.Sprintf("engram:%s|version:%s|step:%s|input:%s", engram.Name, version, step.Spec.StepID, string(inputBytes))
	}
	if policy != nil && policy.Salt != nil {
		if salt := strings.TrimSpace(*policy.Salt); salt != "" {
			keyMaterial = fmt.Sprintf("%s|salt:%s", keyMaterial, salt)
		}
	}

	sum := sha256.Sum256([]byte(keyMaterial))
	return fmt.Sprintf("%x", sum[:]), nil
}

func (r *StepRunReconciler) applyCacheHit(ctx context.Context, step *runsv1alpha1.StepRun, output json.RawMessage) error {
	if step == nil {
		return nil
	}
	if err := r.setStepRunPhase(ctx, step, enums.PhaseSucceeded, conditions.ReasonCompleted, "Cache hit; reused output."); err != nil {
		return err
	}
	if len(output) == 0 {
		return nil
	}
	return kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		if sr.Status.Output == nil {
			sr.Status.Output = &runtime.RawExtension{}
		}
		sr.Status.Output.Raw = append([]byte(nil), output...)
		now := metav1.Now()
		sr.Status.LastOutputAt = &now
	})
}

func (r *StepRunReconciler) ensureCacheAnnotations(ctx context.Context, step *runsv1alpha1.StepRun, meta *cacheMetadata, log *logging.ControllerLogger) {
	if step == nil || meta == nil {
		return
	}
	patch := client.MergeFrom(step.DeepCopy())
	if step.Annotations == nil {
		step.Annotations = map[string]string{}
	}
	changed := false
	if meta.Key != "" && step.Annotations[cacheAnnotationKey] != meta.Key {
		step.Annotations[cacheAnnotationKey] = meta.Key
		changed = true
	}
	if meta.Mode != "" && step.Annotations[cacheAnnotationMode] != meta.Mode {
		step.Annotations[cacheAnnotationMode] = meta.Mode
		changed = true
	}
	if meta.TTLSeconds != nil {
		value := strconv.Itoa(int(*meta.TTLSeconds))
		if step.Annotations[cacheAnnotationTTL] != value {
			step.Annotations[cacheAnnotationTTL] = value
			changed = true
		}
	} else if _, ok := step.Annotations[cacheAnnotationTTL]; ok {
		delete(step.Annotations, cacheAnnotationTTL)
		changed = true
	}
	if !changed {
		return
	}
	if err := r.Patch(ctx, step, patch); err != nil && log != nil {
		log.Error(err, "Failed to patch cache annotations")
	}
}

func (r *StepRunReconciler) patchCacheStatus(ctx context.Context, step *runsv1alpha1.StepRun, status string, log *logging.ControllerLogger) {
	if step == nil {
		return
	}
	patch := client.MergeFrom(step.DeepCopy())
	if step.Annotations == nil {
		step.Annotations = map[string]string{}
	}
	if step.Annotations[cacheAnnotationStatus] == status {
		return
	}
	step.Annotations[cacheAnnotationStatus] = status
	if err := r.Patch(ctx, step, patch); err != nil && log != nil {
		log.Error(err, "Failed to patch cache status")
	}
}

func (r *StepRunReconciler) tryCacheHit(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	meta *cacheMetadata,
	engramTemplate *catalogv1alpha1.EngramTemplate,
	log *logging.ControllerLogger,
) (bool, error) {
	if step == nil || meta == nil || !meta.ReadEnabled {
		return false, nil
	}
	manager, err := storage.SharedManager(ctx)
	if err != nil {
		return false, err
	}
	if manager == nil || manager.GetStore() == nil {
		return false, nil
	}
	data, err := manager.ReadBlob(ctx, meta.Path)
	if err != nil {
		if storage.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	var entry cacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return false, fmt.Errorf("failed to decode cache entry: %w", err)
	}
	if entry.TTLSeconds != nil && *entry.TTLSeconds > 0 {
		if entry.CreatedAt.IsZero() {
			return false, nil
		}
		if time.Since(entry.CreatedAt) > time.Duration(*entry.TTLSeconds)*time.Second {
			return false, nil
		}
	}
	if len(entry.Output) == 0 {
		return false, nil
	}
	if engramTemplate != nil && engramTemplate.Spec.OutputSchema != nil && len(engramTemplate.Spec.OutputSchema.Raw) > 0 {
		schemaName := fmt.Sprintf("EngramTemplate %s output", engramTemplate.Name)
		output := &runtime.RawExtension{Raw: entry.Output}
		if err := validateJSONOutputRaw(output, engramTemplate.Spec.OutputSchema, schemaName); err != nil {
			if log != nil {
				log.Error(err, "Cached output failed schema validation; ignoring cache", "step", step.Name)
			}
			return false, nil
		}
	}
	if err := r.applyCacheHit(ctx, step, entry.Output); err != nil {
		return false, err
	}
	r.patchCacheStatus(ctx, step, cacheStatusHit, log)
	return true, nil
}

func (r *StepRunReconciler) maybeWriteCache(ctx context.Context, step *runsv1alpha1.StepRun, log *logging.ControllerLogger) {
	if step == nil || step.Status.Output == nil || len(step.Status.Output.Raw) == 0 {
		return
	}
	if step.Spec.EngramRef == nil {
		return
	}
	if step.Annotations == nil {
		return
	}
	if step.Annotations[cacheAnnotationStatus] == cacheStatusStored {
		return
	}
	key := strings.TrimSpace(step.Annotations[cacheAnnotationKey])
	if key == "" {
		return
	}
	mode := step.Annotations[cacheAnnotationMode]
	if !cacheModeAllowsWrite(mode) {
		return
	}
	path := cachePathForStep(step, step.Spec.EngramRef.Name, step.Spec.EngramRef.Version, key)
	if path == "" {
		return
	}
	var ttl *int32
	if rawTTL := strings.TrimSpace(step.Annotations[cacheAnnotationTTL]); rawTTL != "" {
		if parsed, err := strconv.Atoi(rawTTL); err == nil {
			val := int32(parsed)
			ttl = &val
		}
	}
	manager, err := storage.SharedManager(ctx)
	if err != nil {
		if log != nil {
			log.Error(err, "Failed to initialize storage for cache write")
		}
		return
	}
	if manager == nil || manager.GetStore() == nil {
		return
	}
	entry := cacheEntry{
		CreatedAt:  time.Now(),
		TTLSeconds: ttl,
		Output:     append([]byte(nil), step.Status.Output.Raw...),
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		if log != nil {
			log.Error(err, "Failed to marshal cache entry")
		}
		return
	}
	if err := manager.WriteBlob(ctx, path, "application/json", payload); err != nil {
		if log != nil {
			log.Error(err, "Failed to write cache entry", "path", path)
		}
		return
	}
	r.patchCacheStatus(ctx, step, cacheStatusStored, log)
}

// cleanupRunScopedRuntime deletes realtime resources after TTL expiry.
//
// Behavior:
//   - Deletes Deployment, StatefulSet, Service, and TransportBinding.
//   - Returns true when all resources are fully deleted.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to clean up.
//
// Returns:
//   - bool: true if cleanup is complete.
//   - error: non-nil on deletion failures.
func (r *StepRunReconciler) cleanupRunScopedRuntime(ctx context.Context, step *runsv1alpha1.StepRun) (bool, error) {
	logger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(step)
	targets := []realtimeCleanupTarget{
		{
			Object:      &appsv1.Deployment{},
			Key:         types.NamespacedName{Name: step.Name, Namespace: step.Namespace},
			Description: "deployment",
			DeleteOpts:  []client.DeleteOption{client.PropagationPolicy(metav1.DeletePropagationBackground)},
		},
		{
			Object:      &appsv1.StatefulSet{},
			Key:         types.NamespacedName{Name: step.Name, Namespace: step.Namespace},
			Description: "statefulset",
			DeleteOpts:  []client.DeleteOption{client.PropagationPolicy(metav1.DeletePropagationBackground)},
		},
		{
			Object:      &corev1.Service{},
			Key:         types.NamespacedName{Name: step.Name, Namespace: step.Namespace},
			Description: "service",
		},
		{
			Object:      &transportv1alpha1.TransportBinding{},
			Key:         types.NamespacedName{Name: transportbinding.Name(step.Spec.StoryRunRef.Name, step.Spec.StepID), Namespace: step.Namespace},
			Description: "transport binding",
		},
	}

	cleaned := true
	for _, target := range targets {
		done, err := r.ensureRealtimeResourceDeleted(ctx, logger, target)
		if err != nil {
			return false, err
		}
		if !done {
			cleaned = false
		}
	}
	return cleaned, nil
}

type realtimeCleanupTarget struct {
	Object      client.Object
	Key         types.NamespacedName
	Description string
	DeleteOpts  []client.DeleteOption
}

func (r *StepRunReconciler) ensureRealtimeResourceDeleted(
	ctx context.Context,
	logger *logging.ControllerLogger,
	target realtimeCleanupTarget,
) (bool, error) {
	obj := target.Object
	if err := r.Get(ctx, target.Key, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}

	if !obj.GetDeletionTimestamp().IsZero() {
		return false, nil
	}

	logger.Info("Deleting realtime resource after TTL expiry",
		"resource", target.Description,
		"name", target.Key.Name,
	)
	if err := r.Delete(ctx, obj, target.DeleteOpts...); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to delete realtime resource", "resource", target.Description, "name", target.Key.Name)
		return false, err
	}
	return false, nil
}

// deploymentReadyState checks if deployment has desired ready replicas.
//
// Behavior:
//   - Returns false if deployment is nil.
//   - Compares ReadyReplicas against Spec.Replicas.
//
// Arguments:
//   - deployment *appsv1.Deployment: the deployment to check.
//
// Returns:
//   - bool: true if ready.
//   - string: status message.
func deploymentReadyState(deployment *appsv1.Deployment) (bool, string) {
	if deployment == nil {
		return false, "deployment does not exist"
	}
	desired := int32(1)
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}
	if deployment.Status.ReadyReplicas < desired {
		return false, fmt.Sprintf("ready replicas %d/%d", deployment.Status.ReadyReplicas, desired)
	}
	return true, "deployment ready"
}

// setTransportCondition patches StepRun with transport readiness condition.
//
// Behavior:
//   - Updates ConditionTransportReady via conditions.NewConditionManager.
//   - Emits events for significant readiness changes.
//   - Mirrors ObservedGeneration for consistency.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to update.
//   - ready bool: whether transport is ready.
//   - message string: status message.
//
// Returns:
//   - error: non-nil on patch failure.
func (r *StepRunReconciler) setTransportCondition(ctx context.Context, step *runsv1alpha1.StepRun, ready bool, message string) error {
	var (
		eventReady   bool
		eventMessage string
		emitEvent    bool
	)

	if err := kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		s := obj.(*runsv1alpha1.StepRun)
		cm := conditions.NewConditionManager(s.Generation)
		status := metav1.ConditionFalse
		reason := conditions.ReasonAwaitingTransport
		msg := message
		if ready {
			status = metav1.ConditionTrue
			reason = conditions.ReasonTransportReady
			if msg == "" {
				msg = "transport binding ready"
			}
		} else if msg == "" {
			msg = "transport binding not ready"
		}

		prev := conditions.GetCondition(s.Status.Conditions, conditions.ConditionTransportReady)
		if prev == nil || prev.Status != status || prev.Reason != reason || prev.Message != msg {
			emitEvent = true
			eventReady = ready
			eventMessage = msg
		}

		cm.SetCondition(&s.Status.Conditions, conditions.ConditionTransportReady, status, reason, msg)
		s.Status.ObservedGeneration = s.Generation
	}); err != nil {
		return err
	}

	if emitEvent && r.Recorder != nil {
		state := "not ready"
		if eventReady {
			state = "ready"
		}
		r.Recorder.Eventf(
			step,
			corev1.EventTypeNormal,
			eventReasonTransportConditionUpdated,
			"Marked transport %s (%s)",
			state,
			eventMessage,
		)
	}

	return nil
}

// resolveBindingEnvValue fetches TransportBinding and returns encoded env value.
//
// Behavior:
//   - Prefers APIReader over cached client.
//   - Delegates to transportutil.ResolveBindingEnvValue.
//   - Falls back to binding name on failures.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - namespace string: binding namespace.
//   - bindingName string: binding name.
//
// Returns:
//   - string: encoded env value or fallback name.
//   - bool: true if binding was successfully read.
func (r *StepRunReconciler) resolveBindingEnvValue(ctx context.Context, namespace, bindingName string) (string, bool) {
	reader := client.Reader(r.Client)
	readerLabel := "cached_client"
	if r.APIReader != nil {
		reader = r.APIReader
		readerLabel = "api_reader"
	}

	value := transportutil.ResolveBindingEnvValue(ctx, reader, namespace, bindingName)
	if value == "" || value == bindingName {
		metrics.RecordTransportBindingReadFallback(namespace, readerLabel)
		return bindingName, false
	}
	return value, true
}

// ensureRunTransportBinding creates/updates TransportBinding for realtime StepRun.
//
// Behavior:
//   - Resolves Story transport and merges settings.
//   - Creates or updates TransportBinding with owner references.
//   - Records mutation metrics.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun owner.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *v1alpha1.Story: the Story definition.
//   - stepSpec *v1alpha1.Step: the step definition.
//   - resolvedCfg *config.ResolvedExecutionConfig: resolved runtime config for port selection.
//   - bindingName string: name for the TransportBinding.
//
// Returns:
//   - *transportv1alpha1.TransportBinding: the created/updated binding.
//   - error: non-nil on failures.
func (r *StepRunReconciler) ensureRunTransportBinding(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	storyRun *runsv1alpha1.StoryRun,
	story *v1alpha1.Story,
	stepSpec *v1alpha1.Step,
	resolvedCfg *config.ResolvedExecutionConfig,
	bindingName string,
) (*transportv1alpha1.TransportBinding, error) {
	ctxData, err := r.buildTransportBindingContext(ctx, step, story, stepSpec, resolvedCfg, bindingName)
	if err != nil {
		return nil, err
	}

	binding := newTransportBindingResource(step.Namespace, bindingName)
	start := time.Now()
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		return r.applyTransportBindingSpec(ctx, step, storyRun, binding, ctxData)
	})
	duration := time.Since(start)
	mutated := result == controllerutil.OperationResultCreated || result == controllerutil.OperationResultUpdated
	metrics.RecordTransportBindingOperation("steprun", ctxData.bindingAlias, mutated, duration, err)
	if err != nil {
		ctxData.logger.Error(err, "Failed to ensure transport binding", "operationResult", result)
		return nil, err
	}
	ctxData.logger.V(1).Info("Ensured transport binding",
		"operationResult", result,
		"mutated", mutated,
		"duration", duration,
		"bindingAlias", ctxData.bindingAlias,
	)

	if err := r.ensureBindingRoutingStatus(ctx, binding, storyRun, stepSpec, ctxData); err != nil {
		ctxData.logger.V(1).Error(err, "Failed to update transport binding routing status", "binding", binding.Name)
	}

	return r.fetchBindingForReadiness(ctx, step, binding, ctxData.logger)
}

type transportBindingContext struct {
	logger         *logging.ControllerLogger
	transportObj   *transportv1alpha1.Transport
	driver         string
	settings       []byte
	defaultPort    int32
	servicePort    int32
	bindingAlias   string
	routingMode    string // "p2p" or "hub"
	routingInfo    *transportutil.StepRoutingInfo
	hasP2PUpstream bool
}

func (r *StepRunReconciler) buildTransportBindingContext(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	stepSpec *v1alpha1.Step,
	resolvedCfg *config.ResolvedExecutionConfig,
	bindingName string,
) (*transportBindingContext, error) {
	logger := logging.NewControllerLogger(ctx, "steprun-binding").
		WithStepRun(step).
		WithValues("binding", bindingName)

	if story == nil {
		return nil, fmt.Errorf("story reference is required for realtime steps")
	}
	if stepSpec == nil {
		if isMaterializeStepRun(step) {
			stepSpec = &v1alpha1.Step{Name: step.Spec.StepID}
		} else {
			return nil, fmt.Errorf("story step definition is required for realtime steps")
		}
	}

	transportName := ""
	if stepSpec != nil {
		transportName = stepSpec.Transport
	}
	transportObj, decl, err := transportutil.ResolveStoryTransport(ctx, r.Client, story, transportName)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve transport %q for story %s: %w", transportName, story.Name, err)
	}
	if transportObj == nil {
		return nil, fmt.Errorf("story %s does not declare any transports", story.Name)
	}

	driver := strings.TrimSpace(transportObj.Spec.Driver)
	if driver == "" {
		return nil, fmt.Errorf("transport %s is missing driver identifier", transportObj.Name)
	}
	logger = logger.WithValues("transportDriver", driver)

	defaultSettings, err := transportutil.MergeSettingsWithStreaming(transportObj.Spec.DefaultSettings, transportObj.Spec.Streaming)
	if err != nil {
		logger.Error(err, "Failed to encode transport streaming defaults", "transport", transportObj.Name)
		r.recordTransportBindingWarning(ctx, step, eventReasonTransportBindingMergeFailed, fmt.Sprintf("Failed to encode streaming defaults for transport %s: %v", transportObj.Name, err))
		return nil, fmt.Errorf("failed to encode transport streaming defaults: %w", err)
	}
	storySettings, err := transportutil.MergeSettingsWithStreaming(decl.Settings, decl.Streaming)
	if err != nil {
		logger.Error(err, "Failed to encode story transport streaming settings", "transport", transportObj.Name)
		r.recordTransportBindingWarning(ctx, step, eventReasonTransportBindingMergeFailed, fmt.Sprintf("Failed to encode streaming settings for transport %s: %v", transportObj.Name, err))
		return nil, fmt.Errorf("failed to encode story transport streaming settings: %w", err)
	}
	settingsBytes, err := transportutil.MergeSettings(defaultSettings, storySettings)
	if err != nil {
		logger.Error(err, "Failed to merge transport settings", "transport", transportObj.Name)
		r.recordTransportBindingWarning(ctx, step, eventReasonTransportBindingMergeFailed, fmt.Sprintf("Failed to merge settings for transport %s: %v", transportObj.Name, err))
		return nil, fmt.Errorf("failed to merge transport settings: %w", err)
	}

	defaultPort, err := r.resolveConnectorPort(logger)
	if err != nil {
		return nil, err
	}
	servicePort := transportutil.EffectiveServicePort(resolvedCfg, defaultPort)

	bindingAlias := bindingName
	if step.Namespace != "" {
		bindingAlias = fmt.Sprintf("%s/%s", step.Namespace, bindingName)
	}

	// Analyze routing topology for P2P optimization (skip for materialize steps).
	var routingInfo *transportutil.StepRoutingInfo
	var hasP2PUpstream bool
	var analyzer *transportutil.TopologyAnalyzer
	if isMaterializeStepRun(step) {
		routingInfo = &transportutil.StepRoutingInfo{
			StepName:    stepSpec.Name,
			RoutingMode: transportutil.RoutingModeHub,
		}
	} else {
		analyzer = transportutil.NewTopologyAnalyzer(story)
		routingInfo, err = analyzer.AnalyzeStepRouting(stepSpec.Name)
		if err != nil {
			logger.V(1).Info("Failed to analyze routing topology, defaulting to hub routing", "error", err)
			// Non-fatal: default to hub routing
			routingInfo = &transportutil.StepRoutingInfo{
				StepName:    stepSpec.Name,
				RoutingMode: transportutil.RoutingModeHub,
			}
		}
	}

	// Apply explicit routing override from transport settings when present.
	forceHubRouting := false
	if len(settingsBytes) > 0 {
		var settings map[string]any
		if err := json.Unmarshal(settingsBytes, &settings); err == nil {
			overrideMode, ok, raw := transportutil.RoutingModeOverride(settings)
			switch {
			case ok && overrideMode == transportutil.RoutingModeHub:
				logger.V(1).Info("Transport settings force hub routing", "step", stepSpec.Name)
				routingInfo.RoutingMode = transportutil.RoutingModeHub
				forceHubRouting = true
			case ok && overrideMode == transportutil.RoutingModeP2P:
				if routingInfo.RoutingMode != transportutil.RoutingModeP2P {
					logger.V(1).Info("Transport settings requested p2p routing but topology requires hub", "step", stepSpec.Name)
				} else {
					routingInfo.RoutingMode = transportutil.RoutingModeP2P
				}
			case raw != "" && raw != "auto":
				logger.V(1).Info("Transport settings contain invalid routing mode; ignoring", "step", stepSpec.Name, "mode", raw)
			}
		}
	}

	if analyzer != nil {
		hasP2PUpstream = analyzer.HasP2PUpstream(stepSpec.Name)
	}
	if forceHubRouting {
		hasP2PUpstream = false
	}

	return &transportBindingContext{
		logger:         logger,
		transportObj:   transportObj,
		driver:         driver,
		settings:       settingsBytes,
		defaultPort:    defaultPort,
		servicePort:    servicePort,
		bindingAlias:   bindingAlias,
		routingMode:    string(routingInfo.RoutingMode),
		routingInfo:    routingInfo,
		hasP2PUpstream: hasP2PUpstream,
	}, nil
}

func (r *StepRunReconciler) resolveConnectorPort(logger *logging.ControllerLogger) (int32, error) {
	cfgResolver := r.ConfigResolver
	if cfgResolver == nil {
		err := fmt.Errorf("config resolver is not initialized")
		logger.Error(err, "Cannot determine connector endpoint port")
		return 0, err
	}
	operatorCfg := cfgResolver.GetOperatorConfig()
	if operatorCfg == nil {
		err := fmt.Errorf("operator config is not loaded")
		logger.Error(err, "Cannot determine connector endpoint port")
		return 0, err
	}
	return int32(operatorCfg.Controller.Engram.EngramControllerConfig.DefaultGRPCPort), nil
}

func newTransportBindingResource(namespace, name string) *transportv1alpha1.TransportBinding {
	return &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func (r *StepRunReconciler) applyTransportBindingSpec(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	storyRun *runsv1alpha1.StoryRun,
	binding *transportv1alpha1.TransportBinding,
	ctxData *transportBindingContext,
) error {
	binding.Spec.TransportRef = ctxData.transportObj.Name
	binding.Spec.Driver = ctxData.driver
	binding.Spec.StepName = step.Spec.StepID
	binding.Spec.EngramName = step.Name
	binding.Spec.StoryRunRef = &refs.StoryRunReference{
		ObjectReference: refs.ObjectReference{
			Name:      storyRun.Name,
			Namespace: ptr.To(step.Namespace),
		},
		UID: ptr.To(storyRun.UID),
	}

	if err := transportutil.PopulateBindingMedia(binding, ctxData.transportObj); err != nil {
		ctxData.logger.Error(err, "Failed to populate binding media", "transport", ctxData.transportObj.Name)
		r.recordTransportBindingWarning(ctx, step, eventReasonTransportBindingCodecFailed, fmt.Sprintf("Transport %s rejected media configuration: %v", ctxData.transportObj.Name, err))
		return err
	}
	if err := transportutil.ValidateCodecSupport(binding, ctxData.transportObj); err != nil {
		ctxData.logger.Error(err, "Failed to validate binding codec support", "transport", ctxData.transportObj.Name)
		r.recordTransportBindingWarning(ctx, step, eventReasonTransportBindingCodecFailed, fmt.Sprintf("Transport %s rejected codec settings: %v", ctxData.transportObj.Name, err))
		return err
	}

	if len(ctxData.settings) > 0 {
		binding.Spec.RawSettings = &runtime.RawExtension{Raw: append([]byte(nil), ctxData.settings...)}
	} else {
		binding.Spec.RawSettings = nil
	}

	binding.Spec.ConnectorEndpoint = transportutil.LocalConnectorEndpoint(ctxData.defaultPort)

	// Add routing annotations for P2P optimization
	if binding.Annotations == nil {
		binding.Annotations = make(map[string]string)
	}
	binding.Annotations["transport.bubustack.io/routing-mode"] = ctxData.routingMode
	if ctxData.routingInfo != nil && len(ctxData.routingInfo.DownstreamSteps) > 0 {
		binding.Annotations["transport.bubustack.io/downstream-steps"] = strings.Join(ctxData.routingInfo.DownstreamSteps, ",")
	}

	return controllerutil.SetControllerReference(step, binding, r.Scheme)
}

func (r *StepRunReconciler) ensureBindingRoutingStatus(
	ctx context.Context,
	binding *transportv1alpha1.TransportBinding,
	storyRun *runsv1alpha1.StoryRun,
	stepSpec *v1alpha1.Step,
	ctxData *transportBindingContext,
) error {
	if binding == nil || ctxData == nil || storyRun == nil || stepSpec == nil {
		return nil
	}
	if strings.TrimSpace(binding.Namespace) == "" || strings.TrimSpace(storyRun.Name) == "" || strings.TrimSpace(stepSpec.Name) == "" {
		return nil
	}

	port := ctxData.servicePort
	if port <= 0 {
		port = 50051
	}

	upstreamHost := ""
	if ctxData.routingInfo != nil && ctxData.routingInfo.RoutingMode == transportutil.RoutingModeP2P && len(ctxData.routingInfo.DownstreamSteps) == 1 {
		downstream := strings.TrimSpace(ctxData.routingInfo.DownstreamSteps[0])
		if downstream != "" {
			serviceName := kubeutil.ComposeName(storyRun.Name, downstream)
			upstreamHost = fmt.Sprintf("%s.%s.svc.cluster.local:%d", serviceName, binding.Namespace, port)
		}
	}

	downstreamHost := ""
	if ctxData.hasP2PUpstream {
		serviceName := kubeutil.ComposeName(storyRun.Name, stepSpec.Name)
		downstreamHost = fmt.Sprintf("%s.%s.svc.cluster.local:%d", serviceName, binding.Namespace, port)
	}

	if binding.Status.UpstreamHost == upstreamHost && binding.Status.DownstreamHost == downstreamHost {
		return nil
	}

	return kubeutil.RetryableStatusPatch(ctx, r.Client, binding, func(obj client.Object) {
		tb := obj.(*transportv1alpha1.TransportBinding)
		tb.Status.UpstreamHost = upstreamHost
		tb.Status.DownstreamHost = downstreamHost
	})
}

func (r *StepRunReconciler) fetchBindingForReadiness(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	binding *transportv1alpha1.TransportBinding,
	logger *logging.ControllerLogger,
) (*transportv1alpha1.TransportBinding, error) {
	var refreshed transportv1alpha1.TransportBinding
	reader := r.APIReader
	readerLabel := "apiReader"
	if reader == nil {
		reader = r.Client
		readerLabel = "client"
	}
	if err := reader.Get(ctx, types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &refreshed); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Transport binding not yet visible to reader; waiting before evaluating readiness", "reader", readerLabel)
			if r.Recorder != nil {
				r.Recorder.Eventf(
					step,
					corev1.EventTypeNormal,
					eventReasonTransportBindingPending,
					"Transport binding %s not yet visible via %s reader",
					binding.Name,
					readerLabel,
				)
			}
			return nil, errTransportBindingPending
		}
		return nil, err
	}
	return &refreshed, nil
}

// recordTransportBindingWarning emits a warning event for transport binding issues.
//
// Behavior:
//   - Returns early if recorder or step is nil.
//   - Emits a labeled warning event with the given reason and message.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the event target.
//   - reason string: event reason.
//   - message string: event message.
//
// Returns:
//   - None.
func (r *StepRunReconciler) recordTransportBindingWarning(ctx context.Context, step *runsv1alpha1.StepRun, reason, message string) {
	r.emitStepRunLabeledEvent(ctx, step, corev1.EventTypeWarning, reason, message)
}

// ensureEngramTransportAnnotation patches Engram with transport binding annotation.
//
// Behavior:
//   - Trims and sanitizes the binding value.
//   - Patches Engram.Annotations[contracts.TransportBindingAnnotation] if changed.
//   - Emits events for invalid input or successful annotation.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - engram *v1alpha1.Engram: the Engram to annotate.
//   - bindingValue string: the raw binding value.
//   - step *runsv1alpha1.StepRun: for context in events.
//
// Returns:
//   - error: non-nil on patch failure.
func (r *StepRunReconciler) ensureEngramTransportAnnotation(ctx context.Context, engram *v1alpha1.Engram, bindingValue string, step *runsv1alpha1.StepRun) error {
	if engram == nil {
		return nil
	}
	bindingValue = strings.TrimSpace(bindingValue)
	if bindingValue == "" {
		return nil
	}
	meta := newTransportAnnotationMeta(ctx, engram, step)
	sanitized := coretransport.SanitizeBindingAnnotationValue(bindingValue)
	if sanitized == "" {
		r.handleAnnotationSanitizeFailure(engram, meta)
		return nil
	}
	if !annotationNeedsUpdate(engram, sanitized) {
		return nil
	}
	return r.patchTransportAnnotation(ctx, engram, sanitized, meta)
}

type transportAnnotationMeta struct {
	logger         *logging.ControllerLogger
	storyRunName   string
	stepIdentifier string
	stepRunName    string
}

func newTransportAnnotationMeta(ctx context.Context, engram *v1alpha1.Engram, step *runsv1alpha1.StepRun) transportAnnotationMeta {
	meta := transportAnnotationMeta{
		logger: logging.NewControllerLogger(ctx, "steprun-binding").
			WithValues("engram", engram.Name, "namespace", engram.Namespace),
		storyRunName:   metrics.UnknownLabelValue,
		stepIdentifier: metrics.UnknownLabelValue,
		stepRunName:    metrics.UnknownLabelValue,
	}
	if step == nil {
		meta.logger = meta.logger.WithValues("storyRun", meta.storyRunName, "stepID", meta.stepIdentifier, "stepRun", meta.stepRunName)
		return meta
	}

	if step.Spec.StoryRunRef.Name != "" {
		meta.storyRunName = step.Spec.StoryRunRef.Name
	}
	if step.Spec.StepID != "" {
		meta.stepIdentifier = step.Spec.StepID
	} else if step.Labels[contracts.ParentStepLabel] != "" {
		meta.stepIdentifier = step.Labels[contracts.ParentStepLabel]
	}
	if step.Name != "" {
		meta.stepRunName = step.Name
	}
	meta.logger = meta.logger.WithStepRun(step).WithValues("storyRun", meta.storyRunName, "stepID", meta.stepIdentifier)
	return meta
}

func (r *StepRunReconciler) handleAnnotationSanitizeFailure(engram *v1alpha1.Engram, meta transportAnnotationMeta) {
	meta.logger.Info("Skipping transport annotation; unable to sanitize binding value")
	metrics.RecordTransportBindingAnnotationSanitizeFailure(engram.Namespace, meta.storyRunName, meta.stepIdentifier)
	meta.logger.V(2).Info("Transport binding annotation sanitize failure", "stepRun", meta.stepRunName)
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(
		engram,
		corev1.EventTypeWarning,
		eventReasonTransportBindingAnnotationInvalid,
		"Failed to sanitize transport binding for Engram %s/%s (storyRun=%s, step=%s, stepRun=%s)",
		engram.Namespace,
		engram.Name,
		meta.storyRunName,
		meta.stepIdentifier,
		meta.stepRunName,
	)
}

func annotationNeedsUpdate(engram *v1alpha1.Engram, sanitized string) bool {
	return engram.Annotations == nil || engram.Annotations[contracts.TransportBindingAnnotation] != sanitized
}

func (r *StepRunReconciler) patchTransportAnnotation(
	ctx context.Context,
	engram *v1alpha1.Engram,
	sanitized string,
	meta transportAnnotationMeta,
) error {
	patch := client.MergeFrom(engram.DeepCopy())
	if engram.Annotations == nil {
		engram.Annotations = make(map[string]string, 1)
	}
	engram.Annotations[contracts.TransportBindingAnnotation] = sanitized
	if err := r.Patch(ctx, engram, patch); err != nil {
		meta.logger.V(1).Info("Failed to patch transport binding annotation", "sanitized", sanitized, "error", err)
		if r.Recorder != nil {
			r.Recorder.Eventf(
				engram,
				corev1.EventTypeWarning,
				eventReasonTransportBindingAnnotationPatchFailed,
				"Failed to record transport binding %s on Engram %s/%s (storyRun=%s, step=%s, stepRun=%s): %v",
				sanitized,
				engram.Namespace,
				engram.Name,
				meta.storyRunName,
				meta.stepIdentifier,
				meta.stepRunName,
				err,
			)
		}
		return err
	}
	meta.logger.V(1).Info("Annotated Engram with transport binding", "sanitized", sanitized)
	if r.Recorder != nil {
		r.Recorder.Eventf(
			engram,
			corev1.EventTypeNormal,
			eventReasonTransportBindingAnnotated,
			"Recorded transport binding %s on Engram %s/%s (storyRun=%s, step=%s, stepRun=%s)",
			sanitized,
			engram.Namespace,
			engram.Name,
			meta.storyRunName,
			meta.stepIdentifier,
			meta.stepRunName,
		)
	}
	return nil
}

// desiredRunScopedService renders the Service manifest that fronts a realtime
// StepRun, mirroring Impulse metadata precedence (controller defaults <
// execution-config overrides < Story overrides).
//
// Arguments:
//   - step *runsv1alpha1.StepRun: supplies namespace/name, labels, and owner identity.
//   - story *v1alpha1.Story: optional Story pointer whose labels/annotations should propagate.
//   - resolvedCfg *config.ResolvedExecutionConfig: provides Service labels, annotations,
//     and ports resolved from Engram templates.
//
// Returns:
//   - *corev1.Service: synthesized Service manifest passed to reconcileRunScopedService.
//
// Side Effects:
//   - None; helper only constructs and returns a Service struct.
func (r *StepRunReconciler) desiredRunScopedService(step *runsv1alpha1.StepRun, story *v1alpha1.Story, resolvedCfg *config.ResolvedExecutionConfig) *corev1.Service {
	selector := runsidentity.SelectorLabels(step.Spec.StoryRunRef.Name)
	selector[contracts.StepRunLabelKey] = safeLabelValue(step.Name)
	if story != nil {
		selector[contracts.StoryLabelKey] = story.Name
	}

	labels := kubeutil.MergeStringMaps(
		map[string]string{
			"app.kubernetes.io/name":       "bobrapet-realtime-runtime",
			"app.kubernetes.io/managed-by": "steprun-controller",
		},
		resolvedCfg.ServiceLabels,
	)
	if labels == nil {
		labels = map[string]string{}
	}

	annotations := kubeutil.MergeStringMaps(resolvedCfg.ServiceAnnotations)

	ports := kubeutil.CloneServicePorts(resolvedCfg.ServicePorts)
	if len(ports) == 0 {
		defaultPort := int32(r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort)
		ports = []corev1.ServicePort{{
			Name:       "grpc",
			Protocol:   corev1.ProtocolTCP,
			Port:       defaultPort,
			TargetPort: intstr.FromString("grpc"),
		}}
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        step.Name,
			Namespace:   step.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Selector: kubeutil.MergeStringMaps(selector),
			Ports:    ports,
		},
	}
}

// reconcileRunScopedService ensures the per-StepRun Service exists and matches
// the desired spec by delegating drift detection to kubeutil.EnsureService.
//
// Arguments:
//   - ctx context.Context: reconciliation context forwarded to kubeutil.EnsureService.
//   - owner *runsv1alpha1.StepRun: controller owner reference applied to the Service.
//   - desired *corev1.Service: manifest generated by desiredRunScopedService.
//
// Returns:
//   - *corev1.Service: reconciled Service fetched/created by kubeutil.EnsureService.
//   - error: create/update errors from kubeutil.EnsureService.
//
// Side Effects:
//   - Issues Get/Create/Patch calls inside kubeutil.EnsureService and logs create/update events.
func (r *StepRunReconciler) reconcileRunScopedService(ctx context.Context, owner *runsv1alpha1.StepRun, desired *corev1.Service) (*corev1.Service, error) {
	service, result, err := kubeutil.EnsureService(ctx, r.Client, r.Scheme, owner, desired)
	if err != nil {
		return nil, err
	}
	switch result {
	case controllerutil.OperationResultCreated:
		logging.NewControllerLogger(ctx, "steprun-service").WithStepRun(owner).Info("Created Service for realtime StepRun", "service", desired.Name)
	case controllerutil.OperationResultUpdated:
		logging.NewControllerLogger(ctx, "steprun-service").WithStepRun(owner).Info("Updated Service for realtime StepRun", "service", desired.Name)
	}
	return service, nil
}

// desiredRunScopedDeployment builds a Deployment manifest for realtime StepRun.
//
// Behavior:
//   - Constructs pod template with env vars, volumes, and security context.
//   - Applies transport binding and storage configuration.
//   - Returns the Deployment for reconciliation.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun owner.
//   - story *v1alpha1.Story: the parent Story.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - engram *v1alpha1.Engram: the Engram configuration.
//   - template *catalogv1alpha1.EngramTemplate: the template.
//   - resolvedCfg *config.ResolvedExecutionConfig: resolved settings.
//   - storageTimeout string: formatted storage timeout.
//   - stepTimeout time.Duration: step execution timeout.
//   - bindingEnv string: transport binding env value.
//   - bindingInfo *transportpb.BindingInfo: binding details.
//   - allowInsecure bool: whether to allow insecure connections.
//
// Returns:
//   - *appsv1.Deployment: the constructed Deployment.
//   - error: non-nil on failures.
func (r *StepRunReconciler) desiredRunScopedDeployment(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	storyRun *runsv1alpha1.StoryRun,
	engram *v1alpha1.Engram,
	template *catalogv1alpha1.EngramTemplate,
	resolvedCfg *config.ResolvedExecutionConfig,
	storageTimeout string,
	stepTimeout time.Duration,
	bindingEnv string,
	bindingInfo *transportpb.BindingInfo,
	allowInsecure bool,
) (*appsv1.Deployment, error) {
	operatorCfg := r.ConfigResolver.GetOperatorConfig()
	runtimeEnvCfg := toRuntimeEnvConfig(operatorCfg.Controller.Engram.EngramControllerConfig)
	defaultPort := transportutil.EffectiveServicePort(resolvedCfg, int32(operatorCfg.Controller.Engram.EngramControllerConfig.DefaultGRPCPort))

	labels, podLabels := buildRealtimeLabels(step, story, engram)
	podAnnotations := buildRealtimeAnnotations(step, story, bindingEnv, bindingInfo)

	schemaName := fmt.Sprintf("EngramTemplate %s inputs", template.Name)
	baseEnv, err := r.buildRealtimeBaseEnv(ctx, step, story, storyRun, engram, template.Spec.InputSchema, schemaName, bindingEnv, bindingInfo, allowInsecure, stepTimeout, runtimeEnvCfg, operatorCfg)
	if err != nil {
		return nil, err
	}
	secretArtifacts := podspec.BuildSecretArtifacts(template.Spec.SecretSchema, resolvedCfg.Secrets)
	envForDeployment, volumes, volumeMounts := composeRealtimeArtifacts(baseEnv, secretArtifacts)

	podSpec := r.buildRuntimePodTemplate(
		resolvedCfg,
		operatorCfg,
		podLabels,
		podAnnotations,
		envForDeployment,
		secretArtifacts.EnvFrom,
		volumes,
		volumeMounts,
		[]corev1.ContainerPort{{
			Name:          "grpc",
			ContainerPort: defaultPort,
		}},
		corev1.RestartPolicyAlways,
	)

	podspec.ApplyStorageEnv(resolvedCfg, &podSpec.Spec, &podSpec.Spec.Containers[0], storageTimeout)

	if err := r.ensureRealtimeTLS(ctx, allowInsecure, step.Namespace, engram, operatorCfg, &podSpec); err != nil {
		return nil, err
	}

	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      step.Name,
			Namespace: step.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{contracts.StepRunLabelKey: safeLabelValue(step.Name)}},
			Template: podSpec,
		},
	}, nil
}

func resolveLifecycleSettings(binding *transportv1alpha1.TransportBinding, logger *logging.ControllerLogger) *transportv1alpha1.TransportLifecycleSettings {
	if binding == nil || binding.Spec.RawSettings == nil || len(binding.Spec.RawSettings.Raw) == 0 {
		return nil
	}
	var settings transportv1alpha1.TransportStreamingSettings
	if err := json.Unmarshal(binding.Spec.RawSettings.Raw, &settings); err != nil {
		if logger != nil {
			logger.V(1).Info("Failed to decode transport streaming settings for lifecycle", "binding", binding.Name, "error", err)
		}
		return nil
	}
	return settings.Lifecycle
}

func applyRealtimeDeploymentStrategy(deployment *appsv1.Deployment, lifecycle *transportv1alpha1.TransportLifecycleSettings) {
	if deployment == nil || lifecycle == nil {
		return
	}
	strategy := resolveDeploymentStrategy(lifecycle.Strategy)
	if strategy == nil {
		return
	}
	deployment.Spec.Strategy = *strategy
}

func resolveDeploymentStrategy(strategy transportv1alpha1.TransportUpgradeStrategy) *appsv1.DeploymentStrategy {
	switch strategy {
	case transportv1alpha1.TransportUpgradeBlueGreen:
		return &appsv1.DeploymentStrategy{
			Type: appsv1.RollingUpdateDeploymentStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDeployment{
				MaxSurge:       intstrPtr(1),
				MaxUnavailable: intstrPtr(0),
			},
		}
	case transportv1alpha1.TransportUpgradeRolling, transportv1alpha1.TransportUpgradeDrainCutover:
		return &appsv1.DeploymentStrategy{
			Type: appsv1.RollingUpdateDeploymentStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDeployment{
				MaxSurge:       intstrPtr(0),
				MaxUnavailable: intstrPtr(1),
			},
		}
	default:
		return nil
	}
}

func intstrPtr(val int) *intstr.IntOrString {
	out := intstr.FromInt(val)
	return &out
}

func deriveHandoffStatus(lifecycle *transportv1alpha1.TransportLifecycleSettings, deployment *appsv1.Deployment) *runsv1alpha1.HandoffStatus {
	status := &runsv1alpha1.HandoffStatus{
		Phase:   runsv1alpha1.HandoffPhasePending,
		Message: "awaiting realtime deployment",
	}
	if lifecycle != nil && lifecycle.Strategy != "" {
		strategy := v1alpha1.TransportUpgradeStrategy(lifecycle.Strategy)
		status.Strategy = &strategy
	}
	if deployment == nil {
		now := metav1.Now()
		status.UpdatedAt = &now
		return status
	}

	desired := int32(1)
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}
	progressing := deployment.Generation > deployment.Status.ObservedGeneration ||
		deployment.Status.UpdatedReplicas < desired ||
		deployment.Status.AvailableReplicas < desired ||
		deployment.Status.UnavailableReplicas > 0

	if !progressing {
		status.Phase = runsv1alpha1.HandoffPhaseReady
		status.Message = "handoff stable"
	} else if lifecycle != nil && (lifecycle.Strategy == transportv1alpha1.TransportUpgradeDrainCutover || lifecycle.Strategy == transportv1alpha1.TransportUpgradeBlueGreen) {
		status.Phase = runsv1alpha1.HandoffPhaseDraining
		status.Message = "draining in-flight messages before cutover"
	} else {
		status.Phase = runsv1alpha1.HandoffPhaseCutover
		status.Message = "deployment rollout in progress"
	}

	now := metav1.Now()
	status.UpdatedAt = &now
	return status
}

func hasDeploymentStrategyOverride(deployment *appsv1.Deployment) bool {
	if deployment == nil {
		return false
	}
	return deployment.Spec.Strategy.Type != "" || deployment.Spec.Strategy.RollingUpdate != nil
}

func buildRealtimeLabels(step *runsv1alpha1.StepRun, story *v1alpha1.Story, engram *v1alpha1.Engram) (map[string]string, map[string]string) {
	labels := runsidentity.SelectorLabels(step.Spec.StoryRunRef.Name)
	labels["app.kubernetes.io/name"] = "bobrapet-realtime-runtime"
	labels["app.kubernetes.io/managed-by"] = "steprun-controller"
	labels[contracts.StepRunLabelKey] = safeLabelValue(step.Name)
	labels[contracts.StepLabelKey] = step.Spec.StepID
	if correlation := correlationIDFromAnnotations(step); correlation != "" {
		ensureCorrelationLabel(labels, correlation)
	}
	if engram != nil {
		labels[contracts.EngramLabelKey] = engram.Name
	}
	if story != nil {
		labels[contracts.StoryLabelKey] = story.Name
	}

	podLabels := make(map[string]string, len(labels))
	maps.Copy(podLabels, labels)
	return labels, podLabels
}

func safeLabelValue(name string) string {
	return runsidentity.SelectorLabels(name)[contracts.StoryRunLabelKey]
}

func buildStepRunEventLabels(step *runsv1alpha1.StepRun) map[string]string {
	if step == nil {
		return nil
	}
	labels := map[string]string{}
	if step.Labels != nil {
		maps.Copy(labels, step.Labels)
	}
	storyRunName := strings.TrimSpace(step.Spec.StoryRunRef.Name)
	if storyRunName != "" {
		labels[contracts.StoryRunLabelKey] = safeLabelValue(storyRunName)
	}
	if step.Name != "" {
		labels[contracts.StepRunLabelKey] = safeLabelValue(step.Name)
	}
	if step.Spec.StepID != "" {
		labels[contracts.StepLabelKey] = safeLabelValue(step.Spec.StepID)
	}
	if step.Spec.EngramRef != nil && step.Spec.EngramRef.Name != "" {
		labels[contracts.EngramLabelKey] = step.Spec.EngramRef.Name
	}
	if correlation := correlationIDFromAnnotations(step); correlation != "" {
		ensureCorrelationLabel(labels, correlation)
	}
	return labels
}

func (r *StepRunReconciler) emitStepRunLabeledEvent(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	eventType string,
	reason string,
	message string,
) {
	if r == nil || r.Recorder == nil || step == nil {
		return
	}
	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return
	}
	eventLabels := buildStepRunEventLabels(step)
	now := metav1.Now()
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-", step.Name),
			Namespace:    step.Namespace,
			Labels:       eventLabels,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:       "StepRun",
			Namespace:  step.Namespace,
			Name:       step.Name,
			UID:        step.UID,
			APIVersion: runsv1alpha1.GroupVersion.String(),
		},
		Reason:         reason,
		Message:        trimmed,
		Type:           eventType,
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
		Source: corev1.EventSource{
			Component: "steprun-controller",
		},
	}
	if err := r.Create(ctx, ev); err != nil && !apierrors.IsAlreadyExists(err) {
		if log := logging.NewReconcileLogger(ctx, "steprun-event"); log != nil {
			log.Error(err, "Failed to emit labeled StepRun event", "reason", reason)
		}
	}
}

func buildRealtimeAnnotations(
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	bindingEnv string,
	bindingInfo *transportpb.BindingInfo,
) map[string]string {
	annotations := map[string]string{
		contracts.StoryRunAnnotation: step.Spec.StoryRunRef.Name,
		contracts.StepAnnotation:     step.Spec.StepID,
	}
	if story != nil {
		annotations[contracts.StoryAnnotation] = story.Name

		// Add routing annotations for P2P optimization
		if !isMaterializeStepRun(step) {
			analyzer := transportutil.NewTopologyAnalyzer(story)
			if routingInfo, err := analyzer.AnalyzeStepRouting(step.Spec.StepID); err == nil {
				routingMode := routingInfo.RoutingMode
				if override, ok := routingModeFromBindingSettings(bindingInfo); ok && override == transportutil.RoutingModeHub {
					// Binding settings override topology analysis. Keep pod annotation
					// aligned with binding so connector-webhook resolves the same mode.
					routingMode = transportutil.RoutingModeHub
				}
				annotations["transport.bubustack.io/routing-mode"] = string(routingMode)
				if len(routingInfo.DownstreamSteps) > 0 {
					annotations["transport.bubustack.io/downstream-steps"] = strings.Join(routingInfo.DownstreamSteps, ",")
				}
			}
		}
	}
	if bindingEnv != "" {
		annotations[contracts.TransportBindingAnnotation] = bindingEnv
	}
	return annotations
}

func routingModeFromBindingSettings(bindingInfo *transportpb.BindingInfo) (transportutil.RoutingMode, bool) {
	if bindingInfo == nil || len(bindingInfo.GetPayload()) == 0 {
		return "", false
	}
	var settings map[string]any
	if err := json.Unmarshal(bindingInfo.GetPayload(), &settings); err != nil {
		return "", false
	}
	mode, ok, _ := transportutil.RoutingModeOverride(settings)
	return mode, ok
}

func (r *StepRunReconciler) buildRealtimeBaseEnv(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	storyRun *runsv1alpha1.StoryRun,
	engram *v1alpha1.Engram,
	inputSchema *runtime.RawExtension,
	inputSchemaName string,
	bindingEnv string,
	bindingInfo *transportpb.BindingInfo,
	allowInsecure bool,
	stepTimeout time.Duration,
	runtimeEnvCfg coreenv.Config,
	operatorCfg *config.OperatorConfig,
) ([]corev1.EnvVar, error) {
	meta := realtimeMetadata(step, story, storyRun, engram)
	baseEnv := coreenv.BuildBaseEnv(meta, runtimeEnvCfg, operatorCfg.Controller.TracePropagationEnabled)

	startedAt := metav1.Now()
	if step.Status.StartedAt != nil {
		startedAt = *step.Status.StartedAt
	}
	coreenv.AppendStartedAtEnv(&baseEnv, startedAt)

	configBytes, err := r.evaluateStepConfigForRealtime(ctx, step, storyRun, story, inputSchema, inputSchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate step config for realtime deployment: %w", err)
	}
	baseEnv = append(baseEnv, buildConfigEnvVars(configBytes)...)

	storyInputsBytes, err := r.getStoryRunInputsBytes(ctx, storyRun, story)
	if err != nil {
		return nil, fmt.Errorf("failed to get story inputs: %w", err)
	}
	if len(storyInputsBytes) == 0 {
		storyInputsBytes = []byte("{}")
	}
	baseEnv = append(baseEnv, corev1.EnvVar{Name: contracts.TriggerDataEnv, Value: string(storyInputsBytes)})

	if transportsJSON := marshalStoryTransports(story); transportsJSON != "" {
		baseEnv = append(baseEnv, corev1.EnvVar{Name: contracts.TransportsEnv, Value: transportsJSON})
	}
	if bindingEnv != "" {
		baseEnv = append(baseEnv, corev1.EnvVar{Name: contracts.TransportBindingEnv, Value: bindingEnv})
	}
	baseEnv = coretransport.AppendTransportMetadataEnv(baseEnv, bindingInfo)
	baseEnv = coretransport.AppendTransportHeartbeatEnv(baseEnv, operatorCfg.Controller.Transport.HeartbeatInterval)
	baseEnv = coretransport.AppendBindingEnvOverrides(baseEnv, bindingInfo)
	baseEnv = transportutil.ApplyTransportSecurityEnv(baseEnv, allowInsecure)
	if stepTimeout > 0 {
		baseEnv = append(baseEnv, corev1.EnvVar{Name: contracts.StepTimeoutEnv, Value: stepTimeout.String()})
	}
	return baseEnv, nil
}

func realtimeMetadata(step *runsv1alpha1.StepRun, story *v1alpha1.Story, storyRun *runsv1alpha1.StoryRun, engram *v1alpha1.Engram) coreenv.Metadata {
	storyName := ""
	if story != nil {
		storyName = story.Name
	}
	workloadMode := ""
	if engram != nil {
		workloadMode = string(engram.Spec.Mode)
	}
	return coreenv.Metadata{
		StoryName:        storyName,
		StoryRunName:     storyRun.Name,
		StepName:         step.Spec.StepID,
		StepRunName:      step.Name,
		StepRunNamespace: step.Namespace,
		WorkloadMode:     workloadMode,
	}
}

func marshalStoryTransports(story *v1alpha1.Story) string {
	if story == nil {
		return ""
	}
	if len(story.Status.Transports) > 0 {
		if payload, err := transportutil.MarshalStoryTransportStatuses(story.Status.Transports); err == nil {
			return payload
		}
	}
	if payload, err := transportutil.MarshalStoryTransports(story.Spec.Transports); err == nil {
		return payload
	}
	return ""
}

func composeRealtimeArtifacts(baseEnv []corev1.EnvVar, artifacts podspec.SecretArtifacts) ([]corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount) {
	envVars := append([]corev1.EnvVar{}, baseEnv...)
	envVars = append(envVars, artifacts.EnvVars...)

	volumeMounts := append([]corev1.VolumeMount(nil), artifacts.VolumeMounts...)
	volumes := append([]corev1.Volume(nil), artifacts.Volumes...)

	return envVars, volumes, volumeMounts
}

func (r *StepRunReconciler) ensureRealtimeTLS(
	ctx context.Context,
	allowInsecure bool,
	namespace string,
	engram *v1alpha1.Engram,
	operatorCfg *config.OperatorConfig,
	podSpec *corev1.PodTemplateSpec,
) error {
	if allowInsecure {
		return nil
	}
	secretName := transportutil.ResolveTLSSecretName(engram, operatorCfg.Controller.Transport.GRPC.DefaultTLSSecret)
	if secretName == "" {
		return fmt.Errorf("transport TLS secret not configured and insecure transport disabled")
	}
	return r.configureTLSEnvAndMounts(ctx, namespace, secretName, &podSpec.Spec.Volumes, &podSpec.Spec.Containers[0].VolumeMounts, &podSpec.Spec.Containers[0].Env)
}

// reconcileRunScopedDeployment ensures Deployment exists and matches desired spec.
//
// Behavior:
//   - Delegates to workload.Ensure for create/update.
//   - Checks PodTemplate and Replicas for drift.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - owner *runsv1alpha1.StepRun: the owner reference.
//   - desired *appsv1.Deployment: the desired Deployment spec.
//
// Returns:
//   - *appsv1.Deployment: the reconciled Deployment.
//   - error: non-nil on failures.
func (r *StepRunReconciler) reconcileRunScopedDeployment(ctx context.Context, owner *runsv1alpha1.StepRun, desired *appsv1.Deployment) (*appsv1.Deployment, error) {
	key := types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}
	logger := logging.NewControllerLogger(ctx, "steprun-workload").WithStepRun(owner).Logr()

	return workload.Ensure(ctx, workload.EnsureOptions[*appsv1.Deployment]{
		Client:         r.Client,
		Scheme:         r.Scheme,
		Owner:          owner,
		NamespacedName: key,
		Logger:         logger,
		WorkloadType:   "Deployment",
		NewEmpty:       func() *appsv1.Deployment { return &appsv1.Deployment{} },
		BuildDesired: func() (*appsv1.Deployment, error) {
			return desired.DeepCopy(), nil
		},
		NeedsUpdate: func(current, desired *appsv1.Deployment) bool {
			strategyOverride := hasDeploymentStrategyOverride(desired)
			return workload.PodTemplateChanged(&current.Spec.Template, &desired.Spec.Template) ||
				!reflect.DeepEqual(current.Spec.Replicas, desired.Spec.Replicas) ||
				(strategyOverride && !reflect.DeepEqual(current.Spec.Strategy, desired.Spec.Strategy))
		},
		ApplyUpdate: func(current, desired *appsv1.Deployment) {
			current.Spec.Template = desired.Spec.Template
			current.Spec.Replicas = desired.Spec.Replicas
			if hasDeploymentStrategyOverride(desired) {
				current.Spec.Strategy = desired.Spec.Strategy
			}
		},
	})
}

// classifyExitCode maps container exit codes to ExitClass for retry logic.
//
// Behavior:
//   - Returns Success for 0, Retry for signals/timeouts.
//   - Returns Terminal for application errors.
//   - Returns RateLimited for 429.
//
// Arguments:
//   - code int: the exit code.
//
// Returns:
//   - enums.ExitClass: the classification.
func classifyExitCode(code int) enums.ExitClass {
	switch {
	case code == -1:
		// Sentinel: exit code could not be determined (pod evicted, API error)
		return enums.ExitClassUnknown
	case code == 0:
		return enums.ExitClassSuccess
	case code == 124:
		// GNU timeout convention; SDK uses this for BUBU_STEP_TIMEOUT
		// Timeouts are often retryable (e.g., increase timeout or retry during lower load)
		return enums.ExitClassRetry
	case code == 137:
		// SIGKILL - often OOMKilled or evicted; retryable with more resources
		return enums.ExitClassRetry
	case code == 143:
		// SIGTERM - graceful shutdown; typically retryable
		return enums.ExitClassRetry
	case code == 125, code == 126, code == 127:
		// Command not found / not executable - terminal error
		return enums.ExitClassTerminal
	case code == 429:
		// HTTP 429 Too Many Requests (if exit code is HTTP status) - rate limited
		return enums.ExitClassRateLimited
	case code >= 1 && code <= 127:
		// Application errors - default to terminal (don't retry logic errors)
		// Operators can override this based on LastFailureMsg analysis
		return enums.ExitClassTerminal
	case code >= 128 && code <= 255:
		// Signal termination - often retryable (transient issues)
		return enums.ExitClassRetry
	default:
		// Unknown exit code - mark as terminal to avoid infinite retries
		return enums.ExitClassTerminal
	}
}

// evaluateStepConfigForRealtime evaluates 'with' for realtime steps (inputs only).
//
// Behavior:
//   - Evaluates only 'inputs.*' references.
//   - Leaves 'steps.*' unevaluated (for runtime hub).
//   - Returns raw config if steps references are found.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//
// Returns:
//   - []byte: marshaled evaluated config.
//   - error: non-nil on evaluation failures.
func (r *StepRunReconciler) evaluateStepConfigForRealtime(ctx context.Context, step *runsv1alpha1.StepRun, storyRun *runsv1alpha1.StoryRun, story *v1alpha1.Story, inputSchema *runtime.RawExtension, schemaName string) ([]byte, error) {
	stepName := ""
	if step != nil {
		stepName = step.Name
	}
	storyName := ""
	if storyRun != nil {
		storyName = storyRun.Name
	}
	log := logr.FromContextOrDiscard(ctx).WithValues("step", stepName, "story", storyName)
	log.Info("Template: evaluating 'with' block for realtime step (inputs.* only)")

	if isMaterializeStepRun(step) {
		log.Info("Template: skipping config evaluation for materialize step")
		return []byte("{}"), nil
	}

	if step == nil || step.Spec.Input == nil || len(step.Spec.Input.Raw) == 0 {
		log.Info("Template: returning empty config (no input)")
		return []byte("{}"), nil
	}
	if err := templatesafety.ValidateTemplateJSON(step.Spec.Input.Raw); err != nil {
		return nil, err
	}

	// Get story trigger inputs for template context
	// Note: Step outputs are NOT available - they're per-packet in realtime mode
	storyInputs, err := r.getStoryRunInputs(ctx, storyRun, story)
	if err != nil {
		return nil, fmt.Errorf("failed to get story inputs for realtime config evaluation: %w", err)
	}

	// Parse config from StepRun.Spec.Input (Engram.Spec.With + Step.With)
	configMap := map[string]any{}
	if err := json.Unmarshal(step.Spec.Input.Raw, &configMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal step config: %w", err)
	}

	// Evaluate with inputs.* context only
	vars := map[string]any{
		"inputs": storyInputs,
	}
	log.Info("Template: evaluating with inputs context", "configKeys", len(configMap), "hasInputs", len(storyInputs) > 0)

	// Evaluate - expressions referencing steps.* will fail (expected)
	evaluatedConfig, err := r.TemplateEvaluator.ResolveWithInputs(ctx, configMap, vars)
	if err != nil {
		var blocked *templating.ErrEvaluationBlocked
		if errors.As(err, &blocked) && strings.Contains(blocked.Reason, "steps") {
			return nil, fmt.Errorf("invalid 'with' config for realtime step: references steps.*; use the 'runtime' field instead")
		}
		err = r.handleTemplateEvalError(err)
		log.Error(err, "Template: evaluation failed")
		return nil, fmt.Errorf("failed to evaluate 'with' config for realtime step: %w", err)
	}
	log.Info("Template: evaluation succeeded")

	if evaluatedConfig == nil {
		evaluatedConfig = map[string]any{}
	}
	if inputSchema != nil && len(inputSchema.Raw) > 0 {
		resolved, err := runsinputs.ApplySchemaDefaults(inputSchema.Raw, evaluatedConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to apply input schema defaults: %w", err)
		}
		evaluatedConfig = resolved
	}

	// Marshal evaluated config
	configBytes, err := json.Marshal(evaluatedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal evaluated config: %w", err)
	}
	if schemaName == "" {
		schemaName = "Step inputs"
	}
	if err := validateJSONInputsBytes(configBytes, inputSchema, schemaName); err != nil {
		return nil, fmt.Errorf("step config failed schema validation: %w", err)
	}

	log.Info("Template: returning evaluated static config", "size", len(configBytes))
	return configBytes, nil
}

// evaluateStepConfig evaluates template expressions in merged config for batch steps.
//
// Behavior:
//   - Merges story inputs and prior step outputs.
//   - Evaluates all template expressions in config.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//
// Returns:
//   - []byte: marshaled evaluated config.
//   - error: non-nil on evaluation failures.
func (r *StepRunReconciler) evaluateStepConfig(ctx context.Context, step *runsv1alpha1.StepRun, storyRun *runsv1alpha1.StoryRun, story *v1alpha1.Story, inputSchema *runtime.RawExtension, schemaName string) ([]byte, error) {
	stepName := ""
	if step != nil {
		stepName = step.Name
	}
	storyName := ""
	if storyRun != nil {
		storyName = storyRun.Name
	}
	log := logr.FromContextOrDiscard(ctx).WithValues("step", stepName, "story", storyName)
	hasInput := step != nil && step.Spec.Input != nil
	inputSize := 0
	if hasInput {
		inputSize = len(step.Spec.Input.Raw)
	}
	log.Info("Template: evaluateStepConfig called", "hasInput", hasInput, "inputSize", inputSize)

	if isMaterializeStepRun(step) {
		log.Info("Template: skipping config evaluation for materialize step")
		return []byte("{}"), nil
	}

	if !hasInput || len(step.Spec.Input.Raw) == 0 {
		log.Info("Template: returning empty config (no input)")
		return []byte("{}"), nil
	}
	if err := templatesafety.ValidateTemplateJSON(step.Spec.Input.Raw); err != nil {
		return nil, err
	}

	// Get story inputs for template context
	storyInputs, err := r.getStoryRunInputs(ctx, storyRun, story)
	if err != nil {
		return nil, fmt.Errorf("failed to get story inputs for config evaluation: %w", err)
	}

	// Get prior step outputs for template context
	stepOutputs, err := getPriorStepOutputs(ctx, r.Client, storyRun, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get step outputs for config evaluation: %w", err)
	}

	// Parse merged config from StepRun.Spec.Input (Engram.Spec.With + Step.With)
	mergedConfig := map[string]any{}
	if err := json.Unmarshal(step.Spec.Input.Raw, &mergedConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal step config: %w", err)
	}

	// Evaluate template expressions
	vars := map[string]any{
		"inputs": storyInputs,
		"steps":  stepOutputs,
	}
	var evaluatedConfig map[string]any
	log.Info("Template: before evaluation", "configKeys", len(mergedConfig), "hasInputs", len(storyInputs) > 0)
	evaluatedConfig, err = r.TemplateEvaluator.ResolveWithInputs(ctx, mergedConfig, vars)
	if err != nil {
		var offloaded *templating.ErrOffloadedDataUsage
		if errors.As(err, &offloaded) {
			log.Info(
				"Step config evaluation hit offloaded data; leaving templates for SDK",
				"stepID", step.Spec.StepID,
				"reason", offloaded.Reason,
			)
			evaluatedConfig = mergedConfig
			err = nil
		}
		if err != nil {
			err = r.handleTemplateEvalError(err)
			log.Error(err, "Template: evaluation failed")
			return nil, fmt.Errorf("failed to evaluate template in step config: %w", err)
		}
	}
	if err == nil && evaluatedConfig != nil {
		log.Info("Template: evaluation succeeded")
	}
	// Resolved config may contain nested refs; keep refs and let the SDK hydrate them at runtime.
	if evaluatedConfig != nil && containsStorageRef(evaluatedConfig, 0) {
		log.Info(
			"Step config includes storage refs; skipping controller materialize and leaving refs for SDK",
			"stepID", step.Spec.StepID,
		)
	}
	if evaluatedConfig == nil {
		evaluatedConfig = map[string]any{}
	}
	if inputSchema != nil && len(inputSchema.Raw) > 0 {
		resolved, err := runsinputs.ApplySchemaDefaults(inputSchema.Raw, evaluatedConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to apply input schema defaults: %w", err)
		}
		evaluatedConfig = resolved
	}

	// Marshal evaluated config
	configBytes, err := json.Marshal(evaluatedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal evaluated config: %w", err)
	}
	if schemaName == "" {
		schemaName = "Step inputs"
	}
	if err := validateJSONInputsBytes(configBytes, inputSchema, schemaName); err != nil {
		return nil, fmt.Errorf("step config failed schema validation: %w", err)
	}

	log.Info("Template: returning evaluated config", "size", len(configBytes))
	return configBytes, nil
}

func (r *StepRunReconciler) handleTemplateEvalError(err error) error {
	if err == nil {
		return nil
	}
	var offloaded *templating.ErrOffloadedDataUsage
	if errors.As(err, &offloaded) {
		policy := resolveOffloadedPolicy(r.ConfigResolver)
		if shouldBlockOffloaded(policy) {
			return &templating.ErrEvaluationBlocked{Reason: offloaded.Reason}
		}
		return offloaded
	}
	return err
}

// getStoryRunInputsBytes returns StoryRun inputs as marshaled JSON with defaults applied.
//
// Behavior:
//   - Delegates to runsinputs.ResolveStoryRunInputsBytes.
//
// Arguments:
//   - ctx context.Context: unused.
//   - storyRun *runsv1alpha1.StoryRun: the StoryRun.
//
// Returns:
//   - []byte: marshaled inputs.
//   - error: non-nil on marshal failure.
func (r *StepRunReconciler) getStoryRunInputsBytes(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *v1alpha1.Story) ([]byte, error) {
	_ = ctx
	return runsinputs.ResolveStoryRunInputsBytes(story, storyRun)
}

// buildConfigEnvVars creates BUBU_STEP_CONFIG env var from config bytes.
//
// Behavior:
//   - Delegates to kubeutil.ConfigEnvVars.
//
// Arguments:
//   - configBytes []byte: the config JSON.
//
// Returns:
//   - []corev1.EnvVar: the config env vars.
func buildConfigEnvVars(configBytes []byte) []corev1.EnvVar {
	return kubeutil.ConfigEnvVars(configBytes)
}

func (r *StepRunReconciler) buildTemplateContextEnvVar(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	storyRun *runsv1alpha1.StoryRun,
	story *v1alpha1.Story,
) (*corev1.EnvVar, error) {
	if step == nil || storyRun == nil {
		return nil, nil
	}
	storyInputs, err := r.getStoryRunInputs(ctx, storyRun, story)
	if err != nil {
		return nil, err
	}
	stepOutputs, err := getPriorStepOutputs(ctx, r.Client, storyRun, nil)
	if err != nil {
		return nil, err
	}
	compactTemplateStepOutputs(stepOutputs)
	payload := map[string]any{
		"inputs": storyInputs,
		"steps":  stepOutputs,
	}
	compactTemplateContextPayload(payload)
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if shouldDebugTemplateContext(step, storyRun) {
		logTemplateContextDebug(ctx, step, storyRun, raw, payload)
		r.maybePatchTemplateContextSummary(ctx, step, raw, payload)
	}
	return &corev1.EnvVar{Name: contracts.TemplateContextEnv, Value: string(raw)}, nil
}

const (
	templateContextDebugAnnotation   = "bubustack.io/debug-template-context"
	templateContextSummaryAnnotation = "bubustack.io/template-context-summary"
	templateContextSizeAnnotation    = "bubustack.io/template-context-size"
	templateContextPreviewBytes      = 8 * 1024
	templateContextTopSteps          = 5
)

func shouldDebugTemplateContext(step *runsv1alpha1.StepRun, storyRun *runsv1alpha1.StoryRun) bool {
	if hasDebugAnnotation(step.GetAnnotations()) {
		return true
	}
	return hasDebugAnnotation(storyRun.GetAnnotations())
}

func hasDebugAnnotation(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	value, ok := annotations[templateContextDebugAnnotation]
	if !ok {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func logTemplateContextDebug(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	storyRun *runsv1alpha1.StoryRun,
	raw []byte,
	payload map[string]any,
) {
	log := logr.FromContextOrDiscard(ctx).WithValues(
		"step", step.Name,
		"story", storyRun.Name,
	)
	summary := summarizeTemplateContext(raw, payload)
	preview := truncateBytes(raw, templateContextPreviewBytes)
	log.Info(
		"Template: context debug",
		"sizeBytes", summary.SizeBytes,
		"stepsCount", summary.StepsCount,
		"inputsBytes", summary.InputsBytes,
		"stepsBytes", summary.StepsBytes,
		"largestSteps", summary.LargestSteps,
		"preview", preview,
	)
}

func (r *StepRunReconciler) maybePatchTemplateContextSummary(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	raw []byte,
	payload map[string]any,
) {
	summary := summarizeTemplateContext(raw, payload)
	summaryBytes, err := json.Marshal(summary)
	if err != nil {
		logr.FromContextOrDiscard(ctx).Error(err, "Template: failed to marshal context summary", "step", step.Name)
		return
	}
	summaryValue := string(summaryBytes)
	current := step.GetAnnotations()
	if current != nil {
		if current[templateContextSummaryAnnotation] == summaryValue &&
			current[templateContextSizeAnnotation] == fmt.Sprintf("%d", summary.SizeBytes) {
			return
		}
	}
	patched := step.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[templateContextSummaryAnnotation] = summaryValue
	patched.Annotations[templateContextSizeAnnotation] = fmt.Sprintf("%d", summary.SizeBytes)
	if err := r.Patch(ctx, patched, client.MergeFrom(step)); err != nil {
		logr.FromContextOrDiscard(ctx).Error(err, "Template: failed to patch context summary", "step", step.Name)
	}
}

type templateContextSummary struct {
	SizeBytes    int                          `json:"sizeBytes"`
	InputsBytes  int                          `json:"inputsBytes,omitempty"`
	StepsBytes   int                          `json:"stepsBytes,omitempty"`
	StepsCount   int                          `json:"stepsCount"`
	LargestSteps []templateContextStepSummary `json:"largestSteps,omitempty"`
}

type templateContextStepSummary struct {
	Name            string `json:"name"`
	Bytes           int    `json:"bytes"`
	HasStorageRef   bool   `json:"hasStorageRef,omitempty"`
	HasResultParsed bool   `json:"hasResultParsed,omitempty"`
}

func summarizeTemplateContext(raw []byte, payload map[string]any) templateContextSummary {
	summary := templateContextSummary{SizeBytes: len(raw)}
	if payload == nil {
		return summary
	}
	if inputs, ok := payload["inputs"]; ok {
		summary.InputsBytes = estimateJSONSize(inputs)
	}
	stepsRaw, ok := payload["steps"].(map[string]any)
	if !ok {
		return summary
	}
	summary.StepsCount = len(stepsRaw)
	summary.StepsBytes = estimateJSONSize(stepsRaw)
	if len(stepsRaw) == 0 {
		return summary
	}

	steps := make([]templateContextStepSummary, 0, len(stepsRaw))
	for name, entry := range stepsRaw {
		size := estimateJSONSize(entry)
		steps = append(steps, templateContextStepSummary{
			Name:            name,
			Bytes:           size,
			HasStorageRef:   containsKey(entry, "$bubuStorageRef", 0),
			HasResultParsed: containsKey(entry, "resultParsed", 0),
		})
	}

	sort.Slice(steps, func(i, j int) bool {
		return steps[i].Bytes > steps[j].Bytes
	})
	if len(steps) > templateContextTopSteps {
		steps = steps[:templateContextTopSteps]
	}
	summary.LargestSteps = steps
	return summary
}

func estimateJSONSize(value any) int {
	if value == nil {
		return 0
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return len(bytes)
}

func truncateBytes(raw []byte, max int) string {
	if len(raw) == 0 || max <= 0 {
		return ""
	}
	if len(raw) <= max {
		return string(raw)
	}
	if max < 64 {
		return string(raw[:max]) + "…"
	}
	head := max / 2
	tail := max - head
	return string(raw[:head]) + "…" + string(raw[len(raw)-tail:])
}

func containsKey(value any, key string, depth int) bool {
	if depth > 8 || value == nil {
		return false
	}
	switch v := value.(type) {
	case map[string]any:
		if _, ok := v[key]; ok {
			return true
		}
		for _, entry := range v {
			if containsKey(entry, key, depth+1) {
				return true
			}
		}
	case []any:
		for _, entry := range v {
			if containsKey(entry, key, depth+1) {
				return true
			}
		}
	}
	return false
}

func envVarSizeBytes(job *batchv1.Job, name string) int {
	if job == nil || name == "" {
		return 0
	}
	for _, container := range job.Spec.Template.Spec.Containers {
		for _, env := range container.Env {
			if env.Name == name {
				return len(env.Value)
			}
		}
	}
	return 0
}

func compactTemplateStepOutputs(outputs map[string]any) {
	// Do not strip resultParsed: downstream steps and templates (e.g. .output.data.resultParsed)
	// must resolve at runtime. Size is capped by compactTemplateContextPayload/shrinkLargeTemplateValue.
}

const templateContextMaxValueBytes = 64 * 1024

func compactTemplateContextPayload(payload map[string]any) {
	if payload == nil {
		return
	}
	// Single limit for entire payload. Do not bypass: large inline payloads cause
	// webhook to reject StepRun creation (spec.input / context too large). Large
	// step outputs must be storage refs; controller passes refs in context and
	// SDK resolves them internally.
	shrinkLargeTemplateValue(payload, templateContextMaxValueBytes)
}

func shrinkLargeTemplateValue(value any, maxBytes int) any {
	if maxBytes <= 0 || value == nil {
		return value
	}
	switch v := value.(type) {
	case map[string]any:
		if trimmed, ok := trimStorageRefMap(v); ok {
			return trimmed
		}
		if size, ok := jsonSizeOverLimit(v, maxBytes); ok {
			return buildTemplatePrunedSummary("map", size, v)
		}
		for key, entry := range v {
			v[key] = shrinkLargeTemplateValue(entry, maxBytes)
		}
	case []any:
		if size, ok := jsonSizeOverLimit(v, maxBytes); ok {
			return buildTemplatePrunedSummary("array", size, nil)
		}
		for i, entry := range v {
			v[i] = shrinkLargeTemplateValue(entry, maxBytes)
		}
	}
	return value
}

func jsonSizeOverLimit(value any, maxBytes int) (int, bool) {
	if maxBytes <= 0 {
		return 0, false
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return 0, false
	}
	size := len(raw)
	return size, size > maxBytes
}

func trimStorageRefMap(value map[string]any) (map[string]any, bool) {
	if value == nil {
		return nil, false
	}
	ref, ok := value["$bubuStorageRef"]
	if !ok {
		return nil, false
	}
	trimmed := map[string]any{
		"$bubuStorageRef": ref,
	}
	if path, ok := value["$bubuStoragePath"]; ok {
		trimmed["$bubuStoragePath"] = path
	}
	if contentType, ok := value["$bubuStorageContentType"]; ok {
		trimmed["$bubuStorageContentType"] = contentType
	}
	if schema, ok := value["$bubuStorageSchema"]; ok {
		trimmed["$bubuStorageSchema"] = schema
	}
	if schemaVer, ok := value["$bubuStorageSchemaVersion"]; ok {
		trimmed["$bubuStorageSchemaVersion"] = schemaVer
	}
	if len(value) == len(trimmed) {
		return value, true
	}
	return trimmed, true
}

func buildTemplatePrunedSummary(kind string, sizeBytes int, value any) map[string]any {
	summary := map[string]any{
		"$bubuOmitted": true,
		"reason":       "template_context_size_limit",
		"type":         kind,
		"sizeBytes":    sizeBytes,
	}
	if value != nil {
		summary["hasStorageRef"] = containsKey(value, "$bubuStorageRef", 0)
	}
	return summary
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
func (r *StepRunReconciler) nextRequeueDelay() time.Duration {
	const fallbackBase = 5 * time.Second
	const fallbackMax = 30 * time.Second

	operatorCfg := r.ConfigResolver.GetOperatorConfig()
	if operatorCfg == nil {
		return rec.JitteredRequeueDelay(0, 0, fallbackBase, fallbackMax)
	}
	return rec.JitteredRequeueDelay(operatorCfg.Controller.RequeueBaseDelay, operatorCfg.Controller.RequeueMaxDelay, fallbackBase, fallbackMax)
}

// findStoryStep finds a step by name in the Story spec.
//
// Behavior:
//   - Returns nil if story is nil.
//   - Iterates steps and returns matching step.
//
// Arguments:
//   - story *v1alpha1.Story: the Story to search.
//   - stepID string: the step name to find.
//
// Returns:
//   - *v1alpha1.Step: the found step, or nil.
func findStoryStep(story *v1alpha1.Story, stepID string) *v1alpha1.Step {
	if story == nil {
		return nil
	}
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if step.Name == stepID {
			return step
		}
	}
	for i := range story.Spec.Compensations {
		step := &story.Spec.Compensations[i]
		if step.Name == stepID {
			return step
		}
	}
	for i := range story.Spec.Finally {
		step := &story.Spec.Finally[i]
		if step.Name == stepID {
			return step
		}
	}
	return nil
}

// checkRequiresContext validates that all requires paths declared on the Story step
// resolve to non-nil values in the template context. This catches nil upstream outputs
// before the step is scheduled, providing a clear error instead of silently passing null.
func (r *StepRunReconciler) checkRequiresContext(ctx context.Context, step *runsv1alpha1.StepRun, vars map[string]any) error {
	story, err := r.getStoryForStep(ctx, step)
	if err != nil {
		return nil // Don't block on transient errors
	}
	storyStep := findStoryStep(story, step.Spec.StepID)
	if storyStep == nil || len(storyStep.Requires) == 0 {
		return nil
	}
	for _, path := range storyStep.Requires {
		val := resolveNestedPath(vars, strings.Split(path, "."))
		if val == nil {
			message := fmt.Sprintf("required context '%s' is nil — upstream step produced no output for this key", path)
			if failErr := r.failStepRunRequiresContext(ctx, step, message); failErr != nil {
				return failErr
			}
			return fmt.Errorf("%w: %s", errStepRunRequiresContextNil, path)
		}
	}
	return nil
}

func (r *StepRunReconciler) failStepRunRequiresContext(ctx context.Context, step *runsv1alpha1.StepRun, message string) error {
	if err := r.setStepRunPhase(ctx, step, enums.PhaseFailed, conditions.ReasonDependencyFailed, message); err != nil {
		return err
	}
	return kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.LastFailureMsg = message
		sr.Status.ExitClass = enums.ExitClassTerminal
		sr.Status.NextRetryAt = nil
	})
}

// resolveNestedPath traverses a nested map structure following the given path segments.
// Returns nil if any segment is missing or the intermediate value is not a map.
func resolveNestedPath(data map[string]any, parts []string) any {
	var current any = data
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = m[part]
	}
	return current
}

// getStoryForStep fetches the Story referenced by the StepRun's parent StoryRun.
//
// Behavior:
//   - Gets parent StoryRun first.
//   - Loads Story via StoryRef.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun.
//
// Returns:
//   - *v1alpha1.Story: the parent Story.
//   - error: non-nil on fetch failure.
func (r *StepRunReconciler) getStoryForStep(ctx context.Context, step *runsv1alpha1.StepRun) (*v1alpha1.Story, error) {
	storyRun, err := r.getParentStoryRun(ctx, step)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(storyRun.Spec.StoryRef.Name) == "" {
		return nil, fmt.Errorf("storyrun %s missing spec.storyRef.name", storyRun.Name)
	}
	story, key, err := refs.LoadNamespacedReference(
		ctx,
		r.Client,
		storyRun,
		&storyRun.Spec.StoryRef,
		func() *v1alpha1.Story { return &v1alpha1.Story{} },
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("get parent Story %s: %w", key.String(), err)
	}
	return story, nil
}

// SetupWithManager configures the controller with watches and predicates.
//
// Behavior:
//   - Watches StepRun, Job, Deployment, Service, TransportBinding.
//   - Maps Engram and EngramTemplate changes to affected StepRuns.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//   - opts controller.Options: controller options.
//
// Returns:
//   - error: non-nil on setup failure.
func (r *StepRunReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	r.Recorder = mgr.GetEventRecorderFor("steprun-controller")

	// Filter to only reconcile on generation changes (spec updates), not status updates
	generationPredicate := predicate.GenerationChangedPredicate{}
	jobStatusPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			_, ok := e.Object.(*batchv1.Job)
			return ok
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldJob, ok1 := e.ObjectOld.(*batchv1.Job)
			newJob, ok2 := e.ObjectNew.(*batchv1.Job)
			if !ok1 || !ok2 {
				return false
			}
			if oldJob.GetGeneration() != newJob.GetGeneration() {
				return true
			}
			return !apiequality.Semantic.DeepEqual(oldJob.Status, newJob.Status)
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
	materializePredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			step, ok := e.Object.(*runsv1alpha1.StepRun)
			return ok && isMaterializeStepRun(step) && step.Status.Phase.IsTerminal()
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			newStep, ok := e.ObjectNew.(*runsv1alpha1.StepRun)
			if !ok || !isMaterializeStepRun(newStep) {
				return false
			}
			oldStep, ok := e.ObjectOld.(*runsv1alpha1.StepRun)
			if !ok {
				return newStep.Status.Phase.IsTerminal()
			}
			return !oldStep.Status.Phase.IsTerminal() && newStep.Status.Phase.IsTerminal()
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StepRun{}, builder.WithPredicates(generationPredicate)).
		WithOptions(opts).
		Owns(&batchv1.Job{}, builder.WithPredicates(jobStatusPredicate)).
		Owns(&appsv1.Deployment{}, builder.WithPredicates(generationPredicate)).
		Owns(&corev1.Service{}, builder.WithPredicates(generationPredicate)).
		Owns(&transportv1alpha1.TransportBinding{}, builder.WithPredicates(generationPredicate)).
		Watches(&runsv1alpha1.StepRun{}, handler.EnqueueRequestsFromMapFunc(r.mapMaterializeToTargets), builder.WithPredicates(materializePredicate)).
		Watches(&v1alpha1.Engram{}, handler.EnqueueRequestsFromMapFunc(r.mapEngramToStepRuns), builder.WithPredicates(generationPredicate)).
		Watches(&catalogv1alpha1.EngramTemplate{}, handler.EnqueueRequestsFromMapFunc(r.mapEngramTemplateToStepRuns), builder.WithPredicates(generationPredicate)).
		Complete(r)
}

// mapEngramToStepRuns finds StepRuns referencing a given Engram.
//
// Behavior:
//   - Uses field index to find matching StepRuns.
//   - Returns reconcile requests for affected StepRuns.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - obj client.Object: the Engram that changed.
//
// Returns:
//   - []reconcile.Request: requests for affected StepRuns.
func (r *StepRunReconciler) mapEngramToStepRuns(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logging.NewReconcileLogger(ctx, "steprun-mapper").WithValues("engram", obj.GetName())
	var stepRuns runsv1alpha1.StepRunList
	indexKey := fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
	reqs, err := refs.EnqueueByField(ctx, r.Client, &stepRuns, contracts.IndexStepRunEngramRef, indexKey)
	if err != nil {
		log.Error(err, "failed to list stepruns for engram")
		return nil
	}

	return reqs
}

// mapEngramTemplateToStepRuns finds StepRuns affected by EngramTemplate change.
//
// Behavior:
//   - Finds Engrams using the template.
//   - Finds non-terminal StepRuns using those Engrams.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - obj client.Object: the EngramTemplate that changed.
//
// Returns:
//   - []reconcile.Request: requests for affected StepRuns.
func (r *StepRunReconciler) mapEngramTemplateToStepRuns(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logging.NewReconcileLogger(ctx, "steprun-mapper").WithValues("engram-template", obj.GetName())

	// 1. Find all Engrams that reference this EngramTemplate.
	var engrams v1alpha1.EngramList
	// Note: We list across all namespaces because Engrams in any namespace can reference a cluster-scoped template.
	if err := r.List(ctx, &engrams, client.MatchingFields{contracts.IndexEngramTemplateRef: obj.GetName()}); err != nil {
		log.Error(err, "failed to list engrams for engramtemplate")
		return nil
	}

	if len(engrams.Items) == 0 {
		return nil
	}

	// 2. For each of those Engrams, find all the StepRuns that reference them.
	var allRequests []reconcile.Request
	for _, engram := range engrams.Items {
		var stepRuns runsv1alpha1.StepRunList
		indexKey := fmt.Sprintf("%s/%s", engram.GetNamespace(), engram.GetName())
		if err := r.List(ctx, &stepRuns, client.MatchingFields{contracts.IndexStepRunEngramRef: indexKey}); err != nil {
			log.Error(err, "failed to list stepruns for engram", "engram", engram.GetName())
			continue // Continue to the next engram
		}

		for _, item := range stepRuns.Items {
			if item.Status.Phase.IsTerminal() {
				continue
			}
			allRequests = append(allRequests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      item.GetName(),
					Namespace: item.GetNamespace(),
				},
			})
		}
	}

	return allRequests
}

func (r *StepRunReconciler) mapMaterializeToTargets(ctx context.Context, obj client.Object) []reconcile.Request {
	step, ok := obj.(*runsv1alpha1.StepRun)
	if !ok || !isMaterializeStepRun(step) {
		return nil
	}
	if !step.Status.Phase.IsTerminal() {
		return nil
	}
	if step.Annotations == nil {
		return nil
	}
	targetStep := strings.TrimSpace(step.Annotations[contracts.MaterializeTargetAnnotation])
	if targetStep == "" {
		return nil
	}
	storyRunName := strings.TrimSpace(step.Spec.StoryRunRef.Name)
	if storyRunName == "" && step.Labels != nil {
		storyRunName = strings.TrimSpace(step.Labels[contracts.StoryRunLabelKey])
	}
	if storyRunName == "" {
		return nil
	}
	targetName := kubeutil.ComposeName(storyRunName, targetStep)
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: targetName, Namespace: step.Namespace}}}
}

// configureTLSEnvAndMounts adds TLS volume, mounts, and env vars.
//
// Behavior:
//   - Fetches TLS secret.
//   - Adds volume if not exists.
//   - Adds volume mount if not exists.
//   - Adds TLS-related env vars.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - namespace string: secret namespace.
//   - secretName string: TLS secret name.
//   - volumes *[]corev1.Volume: target volumes slice.
//   - volumeMounts *[]corev1.VolumeMount: target mounts slice.
//   - envVars *[]corev1.EnvVar: target env vars slice.
//
// Returns:
//   - error: non-nil on fetch failure.
func (r *StepRunReconciler) configureTLSEnvAndMounts(
	ctx context.Context,
	namespace, secretName string,
	volumes *[]corev1.Volume,
	volumeMounts *[]corev1.VolumeMount,
	envVars *[]corev1.EnvVar,
) error {
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secretName}, &secret); err != nil {
		return err
	}

	const volumeName = "engram-tls"
	if !volumeExists(*volumes, volumeName) {
		*volumes = append(*volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: secretName},
			},
		})
	}

	const mountPath = "/var/run/tls"
	if !volumeMountExists(*volumeMounts, volumeName, mountPath) {
		*volumeMounts = append(*volumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: mountPath,
			ReadOnly:  true,
		})
	}

	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCTLSCertFileEnv, Value: mountPath + "/tls.crt"})
	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCTLSKeyFileEnv, Value: mountPath + "/tls.key"})
	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCAFileEnv, Value: mountPath + "/ca.crt"})
	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCClientCertFileEnv, Value: mountPath + "/tls.crt"})
	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCClientKeyFileEnv, Value: mountPath + "/tls.key"})
	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCRequireTLSEnv, Value: "true"})

	return nil
}

// toRuntimeEnvConfig converts EngramControllerConfig to coreenv.Config.
//
// Behavior:
//   - Maps all GRPC and runtime settings to core env config struct.
//
// Arguments:
//   - cfg config.EngramControllerConfig: the source config.
//
// Returns:
//   - coreenv.Config: the mapped config.
func toRuntimeEnvConfig(cfg config.EngramControllerConfig) coreenv.Config {
	return coreenv.Config{
		DefaultGRPCPort:                       cfg.DefaultGRPCPort,
		DefaultGRPCHeartbeatIntervalSeconds:   cfg.DefaultGRPCHeartbeatIntervalSeconds,
		DefaultMaxInlineSize:                  cfg.DefaultMaxInlineSize,
		DefaultStorageTimeoutSeconds:          cfg.DefaultStorageTimeoutSeconds,
		DefaultGracefulShutdownTimeoutSeconds: cfg.DefaultGracefulShutdownTimeoutSeconds,
		DefaultMaxRecvMsgBytes:                cfg.DefaultMaxRecvMsgBytes,
		DefaultMaxSendMsgBytes:                cfg.DefaultMaxSendMsgBytes,
		DefaultDialTimeoutSeconds:             cfg.DefaultDialTimeoutSeconds,
		DefaultChannelBufferSize:              cfg.DefaultChannelBufferSize,
		DefaultReconnectMaxRetries:            cfg.DefaultReconnectMaxRetries,
		DefaultReconnectBaseBackoffMillis:     cfg.DefaultReconnectBaseBackoffMillis,
		DefaultReconnectMaxBackoffSeconds:     cfg.DefaultReconnectMaxBackoffSeconds,
		DefaultHangTimeoutSeconds:             cfg.DefaultHangTimeoutSeconds,
		DefaultMessageTimeoutSeconds:          cfg.DefaultMessageTimeoutSeconds,
	}
}

// volumeExists checks if a volume with the given name exists.
//
// Behavior:
//   - Iterates volumes and returns true if name matches.
//
// Arguments:
//   - volumes []corev1.Volume: the volumes to search.
//   - name string: the volume name to find.
//
// Returns:
//   - bool: true if found.
func volumeExists(volumes []corev1.Volume, name string) bool {
	for i := range volumes {
		if volumes[i].Name == name {
			return true
		}
	}
	return false
}

// volumeMountExists checks if a mount with the given name and path exists.
//
// Behavior:
//   - Iterates mounts and returns true if name and path match.
//
// Arguments:
//   - mounts []corev1.VolumeMount: the mounts to search.
//   - name string: the mount name.
//   - mountPath string: the mount path.
//
// Returns:
//   - bool: true if found.
func volumeMountExists(mounts []corev1.VolumeMount, name, mountPath string) bool {
	for i := range mounts {
		if mounts[i].Name == name && mounts[i].MountPath == mountPath {
			return true
		}
	}
	return false
}

// appendEnvIfMissing appends an env var only if not already present.
//
// Behavior:
//   - Checks existing env vars by name.
//   - Appends only if name not found.
//
// Arguments:
//   - envVars *[]corev1.EnvVar: target slice to modify.
//   - envVar corev1.EnvVar: the env var to add.
//
// Returns:
//   - None.
//
// Side Effects:
//   - May append to envVars slice.
func appendEnvIfMissing(envVars *[]corev1.EnvVar, envVar corev1.EnvVar) {
	for _, existing := range *envVars {
		if existing.Name == envVar.Name {
			return
		}
	}
	*envVars = append(*envVars, envVar)
}

// getTLSSecretName extracts TLS secret name from object annotations.
//
// Behavior:
//   - Returns empty if obj is nil.
//   - Checks EngramTLSSecretAnnotation first.
//   - Falls back to TLSSecretAnnotation.
//
// Arguments:
//   - obj *metav1.ObjectMeta: the object with annotations.
//
// Returns:
//   - string: the TLS secret name, or empty.
func getTLSSecretName(obj *metav1.ObjectMeta) string {
	if obj == nil {
		return ""
	}
	if v, ok := obj.Annotations[contracts.EngramTLSSecretAnnotation]; ok && v != "" {
		return v
	}
	if v, ok := obj.Annotations[contracts.TLSSecretAnnotation]; ok && v != "" {
		return v
	}
	return ""
}
