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
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	config "github.com/bubustack/bobrapet/internal/config"
	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/podspec"
	rec "github.com/bubustack/bobrapet/pkg/reconcile"
	"github.com/bubustack/bobrapet/pkg/refs"
	runstatus "github.com/bubustack/bobrapet/pkg/runs/status"
	"github.com/bubustack/bobrapet/pkg/workload"
	"github.com/bubustack/core/contracts"
)

// ImpulseReconciler reconciles Impulse resources.
//
// Responsibilities:
//   - Resolve ImpulseTemplate and target Story.
//   - Resolve execution config and apply overrides.
//   - Ensure ServiceAccount and RBAC when using managed SA.
//   - Reconcile Service and workload (Deployment or StatefulSet).
//   - Aggregate trigger statistics from StoryRuns.
//
// Anti-storm strategy:
//   - Impulse itself reconciles only on spec changes (GenerationChangedPredicate).
//   - Owned workloads reconcile only on spec changes (GenerationChangedPredicate).
//   - StoryRun watch reconciles on meaningful lifecycle transitions only (phase/timestamps),
//     ignoring annotation-only changes caused by token marking.
type ImpulseReconciler struct {
	config.ControllerDependencies
	Recorder record.EventRecorder
}

const (
	impulseStatusRefreshInterval = 10 * time.Second

	triggerDedupeModeEnv        = contracts.PrefixEnv + "TRIGGER_DEDUPE_MODE"
	triggerDedupeKeyTemplateEnv = contracts.PrefixEnv + "TRIGGER_DEDUPE_KEY_TEMPLATE"
	triggerRetryMaxAttemptsEnv  = contracts.PrefixEnv + "TRIGGER_RETRY_MAX_ATTEMPTS"
	triggerRetryBaseDelayEnv    = contracts.PrefixEnv + "TRIGGER_RETRY_BASE_DELAY"
	triggerRetryMaxDelayEnv     = contracts.PrefixEnv + "TRIGGER_RETRY_MAX_DELAY"
	triggerRetryBackoffEnv      = contracts.PrefixEnv + "TRIGGER_RETRY_BACKOFF"
	triggerThrottleRateEnv      = contracts.PrefixEnv + "TRIGGER_THROTTLE_RATE_PER_SECOND"
	triggerThrottleBurstEnv     = contracts.PrefixEnv + "TRIGGER_THROTTLE_BURST"
	triggerThrottleMaxInFlight  = contracts.PrefixEnv + "TRIGGER_THROTTLE_MAX_IN_FLIGHT"
)

var errNamespaceTerminating = errors.New("impulse namespace is terminating")

// +kubebuilder:rbac:groups=bubustack.io,resources=impulses,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=bubustack.io,resources=impulses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bubustack.io,resources=impulses/finalizers,verbs=update
// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=impulsetemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/status,verbs=get;patch;update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete

// Reconcile converges Impulse runtime objects and status.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.0/pkg/reconcile
//
// Behavior:
//   - Applies optional reconcile timeout from operator config.
//   - Loads Impulse and short-circuits for deletion/terminal phases.
//   - Loads ImpulseTemplate and target Story, blocking on NotFound.
//   - Resolves execution config and ensures SA/RBAC, Service, and workload.
//   - Syncs StoryRun-derived trigger statistics to Impulse status.
//   - Returns errors for controller-runtime backoff; watches handle re-triggers.
//
// Arguments:
//   - ctx context.Context: propagated to all sub-operations.
//   - req ctrl.Request: contains the Impulse namespace and name.
//
// Returns:
//   - ctrl.Result: typically empty on success; requeue handled by errors/watches.
//   - error: on transient failures for controller-runtime backoff.
func (r *ImpulseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	logger := logging.NewControllerLogger(ctx, "impulse").WithValues("impulse", req.NamespacedName)

	timeout := time.Duration(0)
	if r.ConfigResolver != nil && r.ConfigResolver.GetOperatorConfig() != nil {
		timeout = r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	}
	if timeout <= 0 {
		logger.V(1).Info("Impulse reconcile timeout disabled; running without deadline")
	} else {
		logger.V(1).Info("Applying reconcile timeout", "timeout", timeout.String())
	}

	return rec.Run(ctx, req, rec.RunOptions{
		Controller: "impulse",
		Timeout:    timeout,
	}, rec.RunHooks[*v1alpha1.Impulse]{
		Get: func(ctx context.Context, req ctrl.Request) (*v1alpha1.Impulse, error) {
			return r.fetchImpulse(ctx, req.NamespacedName)
		},
		HandleNormal: func(ctx context.Context, impulse *v1alpha1.Impulse) (ctrl.Result, error) {
			impulseLogger := logger.WithImpulse(impulse)
			return rec.RunPipeline(ctx, impulse, rec.PipelineHooks[*v1alpha1.Impulse, *impulseContext]{
				Prepare: func(ctx context.Context, impulse *v1alpha1.Impulse) (*impulseContext, *ctrl.Result, error) {
					if r.handleImpulseShortCircuit(impulse, impulseLogger) {
						return nil, &ctrl.Result{}, nil
					}
					ctxData, shortCircuit, prepErr := r.prepareImpulseContext(ctx, impulse, impulseLogger)
					return ctxData, shortCircuit, prepErr
				},
				Ensure: func(ctx context.Context, impulse *v1alpha1.Impulse, ictx *impulseContext) (ctrl.Result, error) {
					return r.ensureImpulseWorkloads(ctx, ictx, impulseLogger)
				},
			})
		},
	})
}

type impulseContext struct {
	impulse        *v1alpha1.Impulse
	template       *catalogv1alpha1.ImpulseTemplate
	config         *config.ResolvedExecutionConfig
	deliveryPolicy *v1alpha1.TriggerDeliveryPolicy
	workloadMode   enums.WorkloadMode
	serviceName    string
}

// prepareImpulseContext resolves the ImpulseTemplate, target Story, and execution
// configuration required to build downstream workloads.
//
// It may return a short-circuit result when reconciliation should stop (for example,
// template/story lookup requested a no-op requeue).
//
// Returns:
//   - *impulseContext: non-nil when reconciliation should proceed.
//   - *ctrl.Result: non-nil when reconciliation should stop and return this result.
//   - error: transient failures that should trigger backoff retries.
func (r *ImpulseReconciler) prepareImpulseContext(
	ctx context.Context,
	impulse *v1alpha1.Impulse,
	log *logging.ControllerLogger,
) (*impulseContext, *ctrl.Result, error) {
	if impulse == nil {
		return nil, nil, nil
	}
	template, stop, err := r.loadImpulseTemplate(ctx, impulse, log)
	if err != nil {
		return nil, nil, err
	}
	if stop {
		return nil, &ctrl.Result{}, nil
	}

	if _, stop, err = r.loadTargetStory(ctx, impulse, log); err != nil {
		return nil, nil, err
	} else if stop {
		return nil, &ctrl.Result{}, nil
	}

	resolvedConfig, err := r.resolveImpulseConfig(ctx, impulse, template, log)
	if err != nil {
		return nil, nil, err
	}

	deliveryPolicy := resolveTriggerDeliveryPolicy(template, impulse)
	mode, unsupportedMode := r.desiredWorkloadMode(impulse)
	serviceName, invalidServiceName := r.resolveImpulseServiceName(impulse)

	ictx := &impulseContext{
		impulse:        impulse,
		template:       template,
		config:         resolvedConfig,
		deliveryPolicy: deliveryPolicy,
		workloadMode:   mode,
		serviceName:    serviceName,
	}
	if unsupportedMode != "" {
		message := fmt.Sprintf("Unsupported workload mode %q; defaulting to %s", unsupportedMode, enums.WorkloadModeDeployment)
		log.Info(message)
		kubeutil.RecordEvent(
			r.Recorder,
			log.V(0),
			impulse,
			corev1.EventTypeWarning,
			"UnsupportedWorkloadMode",
			message,
		)
	}
	if invalidServiceName != "" {
		message := fmt.Sprintf("Invalid StatefulSet serviceName %q; defaulting to %q", invalidServiceName, serviceName)
		log.Info(message)
		kubeutil.RecordEvent(
			r.Recorder,
			log.V(0),
			impulse,
			corev1.EventTypeWarning,
			"InvalidServiceName",
			message,
		)
	}
	return ictx, nil, nil
}

// ensureImpulseWorkloads provisions the ServiceAccount/RBAC, Service, and workload
// resources required for the provided impulse context before refreshing trigger
// statistics (internal/controller/impulse_controller.go:200-244).
//
// Arguments:
//   - ctx context.Context: shared cancellation boundary for every helper invoked.
//   - ictx *impulseContext: bundles the Impulse, template, resolved config, workload mode, and service name.
//   - log *logging.ControllerLogger: emits failures while reconciling dependencies.
//
// Returns:
//   - ctrl.Result: the result from the selected workload reconciliation path.
//   - error: propagated when any dependency reconciliation fails.
//
// Side Effects:
//   - Creates or patches controller-owned ServiceAccounts, RBAC bindings, Services, and workloads.
//   - Updates Impulse status and StoryRun trigger annotations via syncImpulseTriggerStats.
func (r *ImpulseReconciler) ensureImpulseWorkloads(
	ctx context.Context,
	ictx *impulseContext,
	log *logging.ControllerLogger,
) (ctrl.Result, error) {
	if ictx == nil {
		return ctrl.Result{}, nil
	}

	managedSA, err := r.ensureImpulseServiceAccount(ctx, ictx.impulse, ictx.config, log)
	if err != nil {
		if errors.Is(err, errNamespaceTerminating) {
			log.Info("Impulse namespace is terminating; skipping workload reconciliation until deletion completes")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to ensure ServiceAccount for impulse")
		return ctrl.Result{}, err
	}
	if managedSA {
		extraRules := resolveImpulseRBACRules(ictx.template, ictx.impulse)
		if err := r.ensureImpulseRBAC(ctx, ictx.impulse, ictx.config.ServiceAccountName, extraRules); err != nil {
			log.Error(err, "Failed to ensure impulse RBAC")
			return ctrl.Result{}, err
		}
	}
	if err := r.ensureStorageSecret(ctx, ictx.impulse.Namespace, ictx.config, log); err != nil {
		return ctrl.Result{}, err
	}

	requireService := ictx.workloadMode == enums.WorkloadModeStatefulSet
	if _, serviceErr := r.reconcileService(ctx, ictx.impulse, ictx.config, ictx.serviceName, requireService); serviceErr != nil {
		return ctrl.Result{}, r.handleServiceError(ctx, ictx.impulse, serviceErr)
	}

	result, reconErr := r.reconcileImpulseWorkload(ctx, ictx)
	if reconErr != nil {
		return result, reconErr
	}

	// Must still run even if only StoryRuns changed (phase transitions).
	if err := r.syncImpulseTriggerStats(ctx, ictx.impulse); err != nil {
		log.Error(err, "Failed to sync impulse trigger statistics")
		return ctrl.Result{}, err
	}

	return result, nil
}

func (r *ImpulseReconciler) ensureStorageSecret(ctx context.Context, targetNamespace string, execCfg *config.ResolvedExecutionConfig, log *logging.ControllerLogger) error {
	if execCfg == nil || execCfg.Storage == nil || execCfg.Storage.S3 == nil || execCfg.Storage.S3.Authentication.SecretRef == nil {
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
	secretName := execCfg.Storage.S3.Authentication.SecretRef.Name
	if err := kubeutil.EnsureSecretCopy(ctx, reader, r.Client, sourceNamespace, targetNamespace, secretName); err != nil {
		log.Error(err, "Failed to sync storage secret", "secret", secretName, "sourceNamespace", sourceNamespace)
		return err
	}
	log.V(1).Info("Synced storage secret into workload namespace", "secret", secretName, "sourceNamespace", sourceNamespace)
	return nil
}

// ensureWorkload ensures a namespaced workload (e.g. Deployment/StatefulSet) exists for
// the given Impulse. If the workload is missing it is created with a controller reference.
// If it exists and is controlled by the Impulse, controlled fields are patched when drift
// is detected.
//
// Type parameter:
//   - T must be a pointer to a Kubernetes API type that implements client.Object.
//
// Parameters:
//   - emptyObj: zero-value object used as the read target for client.Get.
//   - buildFn: returns the desired workload object.
//   - needsUpdateFn: returns true when an update is required.
//   - applyUpdateFn: mutates current to apply desired changes (must not change immutable fields).
//
// Returns:
//   - T: the current workload object (post-create or post-patch).
//   - error: get/create/patch errors, or an adoption safety error when the existing resource is not controlled by Impulse.
func (r *ImpulseReconciler) ensureWorkload(
	ctx context.Context,
	ictx *impulseContext,
	workloadType string,
	reconcileFn func() (*int32, *int32, error),
) (ctrl.Result, error) {
	replicas, readyReplicas, reconErr := reconcileFn()
	if reconErr != nil {
		return ctrl.Result{}, r.handleWorkloadError(ctx, ictx.impulse, workloadType, reconErr)
	}

	if err := r.updateImpulseStatus(ctx, ictx.impulse, ictx.workloadMode, *replicas, *readyReplicas); err != nil {
		return ctrl.Result{}, err
	}

	if readyReplicas == nil || replicas == nil {
		return ctrl.Result{}, nil
	}
	if *readyReplicas == 0 || (*replicas > 0 && *readyReplicas < *replicas) {
		return ctrl.Result{RequeueAfter: impulseStatusRefreshInterval}, nil
	}

	return ctrl.Result{}, nil
}

// reconcileImpulseWorkload selects the appropriate workload reconcile closure based on
// ictx.workloadMode and routes its replica counts through ensureWorkload so Deployment
// and StatefulSet modes share identical status plumbing.
//
// Behavior:
//   - Switches on ictx.workloadMode to select StatefulSet or Deployment path.
//   - Wraps each reconcile path in a closure that returns replica counts.
//   - Delegates to ensureWorkload which handles status updates uniformly.
//
// Arguments:
//   - ctx context.Context: propagated to workload reconcilers.
//   - ictx *impulseContext: supplies workloadMode, template, config, and service name.
//
// Returns:
//   - ctrl.Result: result from ensureWorkload (typically empty on success).
//   - error: propagated from the selected workload reconciler.
func (r *ImpulseReconciler) reconcileImpulseWorkload(ctx context.Context, ictx *impulseContext) (ctrl.Result, error) {
	switch ictx.workloadMode {
	case enums.WorkloadModeStatefulSet:
		return r.ensureWorkload(ctx, ictx, "StatefulSet", func() (*int32, *int32, error) {
			statefulSet, err := r.reconcileStatefulSetWorkload(ctx, ictx.impulse, ictx.template, ictx.config, ictx.deliveryPolicy, ictx.serviceName)
			if err != nil {
				return nil, nil, err
			}
			return &statefulSet.Status.Replicas, &statefulSet.Status.ReadyReplicas, nil
		})
	default:
		return r.ensureWorkload(ctx, ictx, "Deployment", func() (*int32, *int32, error) {
			deployment, err := r.reconcileDeployment(ctx, ictx.impulse, ictx.template, ictx.config, ictx.deliveryPolicy)
			if err != nil {
				return nil, nil, err
			}
			return &deployment.Status.Replicas, &deployment.Status.ReadyReplicas, nil
		})
	}
}

// ensureWorkload ensures a namespaced workload (e.g. Deployment/StatefulSet) exists for
// the given Impulse. If the workload is missing it is created with a controller reference.
// If it exists and is controlled by the Impulse, controlled fields are patched when drift
// is detected.
//
// Type parameter:
//   - T must be a pointer to a Kubernetes API type that implements client.Object.
//
// Parameters:
//   - emptyObj: zero-value object used as the read target for client.Get.
//   - buildFn: returns the desired workload object.
//   - needsUpdateFn: returns true when an update is required.
//   - applyUpdateFn: mutates current to apply desired changes (must not change immutable fields).
//
// Returns:
//   - T: the current workload object (post-create or post-patch).
//   - error: get/create/patch errors, or an adoption safety error when the existing resource is not controlled by Impulse.
//
// reconcileDeployment builds the desired Deployment manifest for an Impulse and
// delegates Get/Create/Patch logic to pkg/workload.Ensure so the managed workload
// matches the current template and execution config.
func (r *ImpulseReconciler) reconcileDeployment(
	ctx context.Context,
	impulse *v1alpha1.Impulse,
	template *catalogv1alpha1.ImpulseTemplate,
	execCfg *config.ResolvedExecutionConfig,
	deliveryPolicy *v1alpha1.TriggerDeliveryPolicy,
) (*appsv1.Deployment, error) {
	engramTemplate := convertImpulseTemplate(template)
	identity := newImpulseIdentity(impulse)
	logger := logging.NewControllerLogger(ctx, "impulse-deployment").WithImpulse(impulse).Logr()

	return workload.Ensure(ctx, workload.EnsureOptions[*appsv1.Deployment]{
		Client: r.Client,
		Scheme: r.Scheme,
		Owner:  impulse,
		NamespacedName: types.NamespacedName{
			Name:      identity.name,
			Namespace: impulse.Namespace,
		},
		Logger:       logger,
		WorkloadType: "Deployment",
		NewEmpty:     func() *appsv1.Deployment { return &appsv1.Deployment{} },
		BuildDesired: func() (*appsv1.Deployment, error) {
			return r.buildDeploymentForImpulse(impulse, engramTemplate, execCfg, deliveryPolicy), nil
		},
		NeedsUpdate: func(current, desired *appsv1.Deployment) bool {
			return deploymentSpecChanged(current, desired)
		},
		ApplyUpdate: func(current, desired *appsv1.Deployment) {
			current.Spec.Template = desired.Spec.Template
		},
	})
}

// reconcileStatefulSetWorkload reconciles the StatefulSet flavor of an Impulse
// by building the desired spec (including Service wiring) and delegating the
// Get/Create/Patch loop to pkg/workload.Ensure.
//
// Behavior:
//   - Converts ImpulseTemplate to EngramTemplate for shared helpers.
//   - Builds StatefulSet with service name for headless Service.
//   - Delegates to workload.Ensure for Get/Create/Patch operations.
//
// Arguments:
//   - ctx context.Context: propagated to workload operations.
//   - impulse *v1alpha1.Impulse: owner for the StatefulSet.
//   - template *catalogv1alpha1.ImpulseTemplate: provides execution settings.
//   - execCfg *config.ResolvedExecutionConfig: resolved execution configuration.
//   - serviceName string: headless Service name for StatefulSet.
//
// Returns:
//   - *appsv1.StatefulSet: the reconciled StatefulSet.
//   - error: on create/update failures.
func (r *ImpulseReconciler) reconcileStatefulSetWorkload(
	ctx context.Context,
	impulse *v1alpha1.Impulse,
	template *catalogv1alpha1.ImpulseTemplate,
	execCfg *config.ResolvedExecutionConfig,
	deliveryPolicy *v1alpha1.TriggerDeliveryPolicy,
	serviceName string,
) (*appsv1.StatefulSet, error) {
	engramTemplate := convertImpulseTemplate(template)
	identity := newImpulseIdentity(impulse)
	logger := logging.NewControllerLogger(ctx, "impulse-statefulset").WithImpulse(impulse).Logr()

	return workload.Ensure(ctx, workload.EnsureOptions[*appsv1.StatefulSet]{
		Client: r.Client,
		Scheme: r.Scheme,
		Owner:  impulse,
		NamespacedName: types.NamespacedName{
			Name:      identity.name,
			Namespace: impulse.Namespace,
		},
		Logger:       logger,
		WorkloadType: "StatefulSet",
		NewEmpty:     func() *appsv1.StatefulSet { return &appsv1.StatefulSet{} },
		BuildDesired: func() (*appsv1.StatefulSet, error) {
			return r.buildStatefulSetForImpulse(impulse, engramTemplate, execCfg, deliveryPolicy, serviceName), nil
		},
		NeedsUpdate: func(current, desired *appsv1.StatefulSet) bool {
			return statefulSetSpecChanged(current, desired)
		},
		ApplyUpdate: func(current, desired *appsv1.StatefulSet) {
			current.Spec.Template = desired.Spec.Template
			current.Spec.PodManagementPolicy = desired.Spec.PodManagementPolicy
		},
	})
}

// fetchImpulse retrieves the Impulse identified by key and leaves error
// classification (for example client.IgnoreNotFound) to the caller.
//
// Behavior:
//   - Performs a Get on the Impulse resource.
//   - Returns the cached object for in-place mutation by downstream helpers.
//   - Wraps errors with context for debugging.
//
// Arguments:
//   - ctx context.Context: propagated to the Get call.
//   - key types.NamespacedName: the Impulse's namespace and name.
//
// Returns:
//   - *v1alpha1.Impulse: the fetched Impulse, or nil on error.
//   - error: wrapped Get error (caller handles NotFound classification).
func (r *ImpulseReconciler) fetchImpulse(ctx context.Context, key types.NamespacedName) (*v1alpha1.Impulse, error) {
	var impulse v1alpha1.Impulse
	if err := r.Get(ctx, key, &impulse); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, err
		}
		return nil, fmt.Errorf("get impulse %s: %w", key.String(), err)
	}
	return &impulse, nil
}

// handleImpulseShortCircuit decides whether reconciliation should stop early
// for deleted or terminal Impulses.
//
// Behavior:
//   - Returns true when the Impulse is being deleted (DeletionTimestamp set).
//   - Returns true when Status.Phase is terminal (Succeeded/Canceled/Compensated).
//   - Logs at V(1) level when short-circuiting.
//
// Arguments:
//   - impulse *v1alpha1.Impulse: the Impulse to check.
//   - log *logging.ControllerLogger: for debug logging, may be nil.
//
// Returns:
//   - bool: true when reconciliation should stop, false to continue.
func (r *ImpulseReconciler) handleImpulseShortCircuit(impulse *v1alpha1.Impulse, log *logging.ControllerLogger) bool {
	if impulse == nil {
		return false
	}
	if !impulse.DeletionTimestamp.IsZero() {
		if log != nil {
			log.V(1).Info("Impulse is deleting, short-circuiting reconcile")
		}
		return true
	}
	switch impulse.Status.Phase {
	case enums.PhaseSucceeded, enums.PhaseCanceled, enums.PhaseCompensated:
		if log != nil {
			log.V(1).Info("Impulse is in a terminal phase, short-circuiting reconcile", "phase", impulse.Status.Phase)
		}
		return true
	}
	return false
}

// loadImpulseTemplate fetches the ImpulseTemplate referenced by the Impulse
// and blocks further reconciliation when it is missing.
//
// Behavior:
//   - Loads the cluster-scoped ImpulseTemplate by name.
//   - On NotFound, emits event, sets Impulse to Blocked phase, and returns stop=true.
//   - On other errors, returns the error for controller-runtime backoff.
//
// Arguments:
//   - ctx context.Context: propagated to the Get call.
//   - impulse *v1alpha1.Impulse: supplies the template reference.
//   - log *logging.ControllerLogger: for error logging.
//
// Returns:
//   - *catalogv1alpha1.ImpulseTemplate: the loaded template, or nil.
//   - bool: true to stop reconciliation (NotFound case).
//   - error: non-nil on transient failures.
func (r *ImpulseReconciler) loadImpulseTemplate(ctx context.Context, impulse *v1alpha1.Impulse, log *logging.ControllerLogger) (*catalogv1alpha1.ImpulseTemplate, bool, error) {
	result := rec.LoadClusterTemplate(
		ctx,
		r.Client,
		impulse.Spec.TemplateRef.Name,
		"ImpulseTemplate",
		func() *catalogv1alpha1.ImpulseTemplate { return &catalogv1alpha1.ImpulseTemplate{} },
	)

	if result.Object != nil {
		return result.Object, false, nil
	}

	if result.Stop {
		log.Error(fmt.Errorf("template not found"), "ImpulseTemplate not found", "template", impulse.Spec.TemplateRef.Name)
		rec.Emit(r.Recorder, impulse, corev1.EventTypeWarning, result.EventReason, result.BlockMessage)
		if patchErr := r.setImpulsePhase(ctx, impulse, enums.PhaseBlocked, result.BlockMessage); patchErr != nil {
			return nil, false, patchErr
		}
		return nil, true, nil
	}

	return nil, false, result.Error
}

// loadTargetStory fetches the Story referenced by the Impulse and blocks
// reconciliation when it is missing.
//
// Behavior:
//   - Resolves the Story using namespace-aware reference resolution.
//   - On NotFound, emits event, sets Impulse to Blocked phase, and returns stop=true.
//   - Avoids repeated status patches when already blocked with same message.
//
// Arguments:
//   - ctx context.Context: propagated to the Get call.
//   - impulse *v1alpha1.Impulse: supplies the story reference.
//   - log *logging.ControllerLogger: for debug and error logging.
//
// Returns:
//   - *v1alpha1.Story: the loaded Story, or nil.
//   - bool: true to stop reconciliation (NotFound case).
//   - error: non-nil on transient failures.
func (r *ImpulseReconciler) loadTargetStory(ctx context.Context, impulse *v1alpha1.Impulse, log *logging.ControllerLogger) (*v1alpha1.Story, bool, error) {
	var cfg *config.ControllerConfig
	if r != nil && r.ConfigResolver != nil {
		if resolved := r.ConfigResolver.GetOperatorConfig(); resolved != nil {
			cfg = &resolved.Controller
		}
	}

	targetNamespace := refs.ResolveNamespace(impulse, &impulse.Spec.StoryRef.ObjectReference)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		r.Client,
		cfg,
		impulse,
		"bubustack.io",
		"Impulse",
		"bubustack.io",
		"Story",
		targetNamespace,
		impulse.Spec.StoryRef.Name,
		"StoryRef",
	); err != nil {
		message := fmt.Sprintf("StoryRef rejected by cross-namespace policy: %v", err)

		currentReady := conditions.GetCondition(impulse.Status.Conditions, conditions.ConditionReady)
		alreadyFailed := impulse.Status.Phase == enums.PhaseFailed &&
			currentReady != nil &&
			currentReady.Message == message

		if alreadyFailed {
			log.V(1).Info("Story reference still rejected by policy", "story", fmt.Sprintf("%s/%s", targetNamespace, impulse.Spec.StoryRef.Name))
			return nil, true, nil
		}

		log.Info("Story reference rejected by policy", "story", fmt.Sprintf("%s/%s", targetNamespace, impulse.Spec.StoryRef.Name))
		if r.Recorder != nil {
			r.Recorder.Event(impulse, corev1.EventTypeWarning, conditions.ReasonStoryReferenceInvalid, message)
		}
		if patchErr := r.setImpulsePhase(ctx, impulse, enums.PhaseFailed, message); patchErr != nil {
			return nil, false, patchErr
		}
		return nil, true, nil
	}

	story, storyKey, err := refs.LoadNamespacedReference(
		ctx,
		r.Client,
		impulse,
		&impulse.Spec.StoryRef,
		func() *v1alpha1.Story { return &v1alpha1.Story{} },
		nil,
	)
	if err != nil {
		if apierrors.IsNotFound(err) {
			message := fmt.Sprintf("Story '%s' not found in namespace '%s'", storyKey.Name, storyKey.Namespace)

			currentReady := conditions.GetCondition(impulse.Status.Conditions, conditions.ConditionReady)
			alreadyBlocked := impulse.Status.Phase == enums.PhaseBlocked &&
				currentReady != nil &&
				currentReady.Reason == conditions.ReasonStoryNotFound &&
				currentReady.Message == message

			if alreadyBlocked {
				log.V(1).Info("Story reference still not available", "story", storyKey.String())
				return nil, true, nil
			}

			log.Info("Story reference not found", "story", storyKey.String())
			if r.Recorder != nil {
				r.Recorder.Event(impulse, corev1.EventTypeWarning, conditions.ReasonStoryNotFound, message)
			}
			if patchErr := r.setImpulsePhase(ctx, impulse, enums.PhaseBlocked, message); patchErr != nil {
				return nil, false, patchErr
			}
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("failed to get Story '%s/%s': %w", storyKey.Namespace, storyKey.Name, err)
	}
	return story, false, nil
}

// resolveImpulseConfig produces the execution configuration for an Impulse by
// merging operator defaults, template settings, inline secrets, and spec
// overrides before workload reconciliation continues.
//
// Behavior:
//   - Converts ImpulseTemplate to EngramTemplate for shared resolution.
//   - Resolves base config via ConfigResolver.ResolveExecutionConfig.
//   - Merges impulse.Spec.Secrets into the resolved config.
//   - Applies execution overrides from impulse.Spec.Execution.
//   - On failure, sets Impulse to Failed phase and emits event.
//
// Arguments:
//   - ctx context.Context: propagated to config resolution.
//   - impulse *v1alpha1.Impulse: supplies secrets and execution overrides.
//   - template *catalogv1alpha1.ImpulseTemplate: base template for config.
//   - log *logging.ControllerLogger: for error logging.
//
// Returns:
//   - *config.ResolvedExecutionConfig: the merged execution config.
//   - error: on resolution or override failures.
func (r *ImpulseReconciler) resolveImpulseConfig(
	ctx context.Context,
	impulse *v1alpha1.Impulse,
	template *catalogv1alpha1.ImpulseTemplate,
	log *logging.ControllerLogger,
) (*config.ResolvedExecutionConfig, error) {
	if template == nil {
		return nil, fmt.Errorf("impulse template is nil for impulse %q", impulse.Name)
	}
	engramTemplate := convertImpulseTemplate(template)
	resolvedConfig, resolveErr := r.ConfigResolver.ResolveExecutionConfig(ctx, nil, nil, nil, engramTemplate, nil)
	if resolveErr != nil {
		if r.Recorder != nil {
			r.Recorder.Event(impulse, corev1.EventTypeWarning, conditions.ReasonInvalidConfiguration,
				fmt.Sprintf("Failed to resolve execution config: %s", resolveErr))
		}
		if patchErr := r.setImpulsePhase(ctx, impulse, enums.PhaseFailed,
			fmt.Sprintf("Failed to resolve configuration: %s", resolveErr)); patchErr != nil {
			log.Error(patchErr, "Failed to update impulse status")
			return nil, patchErr
		}
		return nil, fmt.Errorf("failed to resolve execution config for impulse '%s': %w", impulse.Name, resolveErr)
	}

	if len(impulse.Spec.Secrets) > 0 {
		if resolvedConfig.Secrets == nil {
			resolvedConfig.Secrets = make(map[string]string, len(impulse.Spec.Secrets))
		}
		maps.Copy(resolvedConfig.Secrets, impulse.Spec.Secrets)
	}

	overrideCtx := config.WithExecutionOverrideLayer(ctx, "impulse_overrides")
	if err := r.ConfigResolver.ApplyExecutionOverrides(overrideCtx, impulse.Spec.Execution, resolvedConfig); err != nil {
		if r.Recorder != nil {
			r.Recorder.Event(impulse, corev1.EventTypeWarning, conditions.ReasonInvalidConfiguration,
				fmt.Sprintf("Failed to apply execution overrides: %s", err))
		}
		return nil, fmt.Errorf("failed to apply impulse overrides: %w", err)
	}
	return resolvedConfig, nil
}

// ensureImpulseServiceAccount creates or adopts the controller-managed ServiceAccount
// for an Impulse and mutates the resolved execution config to point at it.
//
// Behavior:
//   - Returns false when a custom ServiceAccountName is specified (user-managed).
//   - Creates/updates a controller-managed SA named "{impulse}-impulse-runner".
//   - Applies storage annotations (e.g., for S3 IRSA) to the SA.
//   - Mutates resolved.ServiceAccountName and AutomountServiceAccountToken.
//
// Arguments:
//   - ctx context.Context: propagated to Create/Update calls.
//   - impulse *v1alpha1.Impulse: owner for the ServiceAccount.
//   - resolved *config.ResolvedExecutionConfig: mutated with SA name on success.
//   - log *logging.ControllerLogger: for logging events.
//
// Returns:
//   - bool: true when SA is controller-managed (RBAC should follow).
//   - error: on ServiceAccount create/update failures.
func (r *ImpulseReconciler) ensureImpulseServiceAccount(
	ctx context.Context,
	impulse *v1alpha1.Impulse,
	resolved *config.ResolvedExecutionConfig,
	log *logging.ControllerLogger,
) (bool, error) {
	if resolved == nil {
		return false, nil
	}

	annotations := podspec.StorageServiceAccountAnnotations(resolved)
	managed := resolved.ServiceAccountName == "" || resolved.ServiceAccountName == "default"
	if !managed {
		kubeutil.RecordEvent(
			r.Recorder,
			log.V(0),
			impulse,
			corev1.EventTypeNormal,
			"CustomServiceAccount",
			fmt.Sprintf("Using user-provided serviceAccountName %q; controller will not annotate or manage it.", resolved.ServiceAccountName),
		)
		if len(annotations) > 0 {
			log.Info("Skipping automatic ServiceAccount annotations because a custom serviceAccountName is set",
				"serviceAccount", resolved.ServiceAccountName)
		}
		return false, nil
	}

	saName := fmt.Sprintf("%s-impulse-runner", impulse.Name)
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:        saName,
			Namespace:   impulse.Namespace,
			Annotations: annotations,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		return controllerutil.SetControllerReference(impulse, sa, r.Scheme)
	}); err != nil {
		if isNamespaceTerminatingError(err) {
			return false, errNamespaceTerminating
		}
		return false, err
	}

	// resolved is shared with workload builders; mutate in place so pod templates
	// reference the controller-managed ServiceAccount and mount projected tokens.
	resolved.ServiceAccountName = saName
	resolved.AutomountServiceAccountToken = true
	return true, nil
}

// ensureImpulseRBAC reconciles the Role and RoleBinding needed for the managed
// ServiceAccount to create StoryRuns on behalf of an Impulse.
//
// Behavior:
//   - Creates/updates a Role granting StoryRun create/get/list/watch/delete/patch/update.
//   - Creates/updates a RoleBinding binding the Role to the ServiceAccount.
//   - Uses rbacutil helpers with ControllerReference for ownership.
//
// Arguments:
//   - ctx context.Context: propagated to RBAC operations.
//   - impulse *v1alpha1.Impulse: owner for the RBAC resources.
//   - saName string: the ServiceAccount name to bind.
//
// Returns:
//   - error: on Role or RoleBinding create/update failures.
func (r *ImpulseReconciler) ensureImpulseRBAC(ctx context.Context, impulse *v1alpha1.Impulse, saName string, extraRules []rbacv1.PolicyRule) error {
	if saName == "" {
		return fmt.Errorf("serviceAccountName is required to configure RBAC")
	}

	roleRules := []rbacv1.PolicyRule{
		{
			APIGroups: []string{"runs.bubustack.io"},
			Resources: []string{"storyruns"},
			Verbs:     []string{"create", "get", "list", "watch", "delete", "patch", "update"},
		},
		{
			APIGroups: []string{"runs.bubustack.io"},
			Resources: []string{"storyruns/status"},
			Verbs:     []string{"get", "patch", "update"},
		},
		{
			APIGroups: []string{"bubustack.io"},
			Resources: []string{"impulses"},
			Verbs:     []string{"get", "list"},
		},
		{
			APIGroups: []string{"bubustack.io"},
			Resources: []string{"impulses/status"},
			Verbs:     []string{"get", "patch", "update"},
		},
	}
	if len(extraRules) > 0 {
		roleRules = mergePolicyRules(roleRules, extraRules)
	}
	if _, err := kubeutil.EnsureRole(ctx, r.Client, r.Scheme, impulse, saName, impulse.Namespace, roleRules, kubeutil.ControllerReference); err != nil {
		return fmt.Errorf("failed to ensure impulse Role: %w", err)
	}

	subjects := []rbacv1.Subject{{
		Kind:      "ServiceAccount",
		Name:      saName,
		Namespace: impulse.Namespace,
	}}
	roleRef := rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: saName}
	if _, err := kubeutil.EnsureRoleBinding(ctx, r.Client, r.Scheme, impulse, saName, impulse.Namespace, roleRef, subjects, kubeutil.ControllerReference); err != nil {
		return fmt.Errorf("failed to ensure impulse RoleBinding: %w", err)
	}
	return nil
}

func resolveImpulseRBACRules(template *catalogv1alpha1.ImpulseTemplate, impulse *v1alpha1.Impulse) []rbacv1.PolicyRule {
	if impulse != nil && impulse.Spec.Execution != nil && impulse.Spec.Execution.RBAC != nil {
		return mergePolicyRules(nil, impulse.Spec.Execution.RBAC.Rules)
	}
	if template == nil || template.Spec.Execution == nil || template.Spec.Execution.RBAC == nil {
		return nil
	}
	return mergePolicyRules(nil, template.Spec.Execution.RBAC.Rules)
}

func mergePolicyRules(base []rbacv1.PolicyRule, extras []rbacv1.PolicyRule) []rbacv1.PolicyRule {
	merged := make([]rbacv1.PolicyRule, 0, len(base)+len(extras))
	merged = append(merged, base...)
	merged = append(merged, extras...)

	unique := map[string]rbacv1.PolicyRule{}
	for _, rule := range merged {
		unique[policyRuleKey(rule)] = rule
	}
	out := make([]rbacv1.PolicyRule, 0, len(unique))
	for _, rule := range unique {
		out = append(out, rule)
	}
	sort.Slice(out, func(i, j int) bool {
		return policyRuleKey(out[i]) < policyRuleKey(out[j])
	})
	return out
}

func policyRuleKey(rule rbacv1.PolicyRule) string {
	apiGroups := append([]string(nil), rule.APIGroups...)
	resources := append([]string(nil), rule.Resources...)
	resourceNames := append([]string(nil), rule.ResourceNames...)
	verbs := append([]string(nil), rule.Verbs...)
	nonResource := append([]string(nil), rule.NonResourceURLs...)

	sort.Strings(apiGroups)
	sort.Strings(resources)
	sort.Strings(resourceNames)
	sort.Strings(verbs)
	sort.Strings(nonResource)

	return strings.Join([]string{
		strings.Join(apiGroups, ","),
		strings.Join(resources, ","),
		strings.Join(resourceNames, ","),
		strings.Join(verbs, ","),
		strings.Join(nonResource, ","),
	}, "|")
}

func isNamespaceTerminatingError(err error) bool {
	return apierrors.IsForbidden(err) && strings.Contains(err.Error(), "being terminated")
}

// handleReconcileError emits an event and marks the Impulse failed for the given
// component before returning the wrapped reconcile error to controller-runtime.
//
// Behavior:
//   - Logs the error with component context.
//   - Emits a warning event with "{Component}ReconcileFailed" reason.
//   - Sets Impulse phase to Failed with descriptive message.
//   - Returns wrapped error for controller-runtime backoff.
//
// Arguments:
//   - ctx context.Context: for status patching.
//   - impulse *v1alpha1.Impulse: the affected Impulse.
//   - component string: identifies which component failed (e.g., "Deployment").
//   - err error: the underlying error.
//
// Returns:
//   - error: always non-nil, wrapped with component context.
func (r *ImpulseReconciler) handleReconcileError(ctx context.Context, impulse *v1alpha1.Impulse, component string, err error) error {
	message := fmt.Sprintf("Failed to reconcile %s: %s", component, err)
	cl := logging.NewControllerLogger(ctx, "impulse").WithValues("impulse", impulse.Name, "namespace", impulse.Namespace)
	cl.Error(err, "Reconciliation component failed", "component", component)
	kubeutil.RecordEvent(
		r.Recorder,
		cl.V(0),
		impulse,
		corev1.EventTypeWarning,
		fmt.Sprintf("%sReconcileFailed", component),
		message,
	)
	if patchErr := r.setImpulsePhase(ctx, impulse, enums.PhaseFailed, message); patchErr != nil {
		return patchErr
	}
	return fmt.Errorf("failed to reconcile %s for impulse '%s': %w", component, impulse.Name, err)
}

// handleWorkloadError wraps workload failures using handleReconcileError so status
// and events clearly identify which workload type failed.
//
// Behavior:
//   - Delegates to handleReconcileError with workload type as component.
//   - Used for Deployment and StatefulSet reconciliation failures.
//
// Arguments:
//   - ctx context.Context: for status patching.
//   - impulse *v1alpha1.Impulse: the affected Impulse.
//   - workloadType string: the workload type (e.g., "Deployment", "StatefulSet").
//   - reconcileErr error: the underlying error.
//
// Returns:
//   - error: always non-nil, from handleReconcileError.
func (r *ImpulseReconciler) handleWorkloadError(ctx context.Context, impulse *v1alpha1.Impulse, workloadType string, reconcileErr error) error {
	return r.handleReconcileError(ctx, impulse, workloadType, reconcileErr)
}

// handleServiceError reports Service reconciliation failures through handleReconcileError
// with a consistent component label.
//
// Behavior:
//   - Delegates to handleReconcileError with "Service" as the component.
//   - Used when Service create/update/delete operations fail.
//
// Arguments:
//   - ctx context.Context: for status patching.
//   - impulse *v1alpha1.Impulse: the affected Impulse.
//   - serviceErr error: the underlying error.
//
// Returns:
//   - error: always non-nil, from handleReconcileError.
func (r *ImpulseReconciler) handleServiceError(ctx context.Context, impulse *v1alpha1.Impulse, serviceErr error) error {
	return r.handleReconcileError(ctx, impulse, "Service", serviceErr)
}

// syncImpulseTriggerStats scans StoryRuns referencing the Impulse and counts
// trigger tokens from their status to update Impulse counters/timestamps.
//
// Behavior:
//   - Returns immediately when the impulse is nil so callers can pass optional pointers.
//   - Lists StoryRuns via listStoryRunsByImpulse, which prefers the spec.impulseRef.name
//     index and falls back to a namespace-wide List on failure.
//   - Reads trigger tokens from StoryRun.Status.TriggerTokens (set by StoryRun controller
//     during phase transitions) instead of patching annotations, eliminating annotation
//     churn and conflicts from multiple writers.
//   - Tracks the most recent trigger/success timestamps and writes counters plus
//     ObservedGeneration via kubeutil.RetryableStatusUpdate.
//
// Arguments:
//   - ctx context.Context: propagated to the StoryRun list and the status update.
//   - impulse *v1alpha1.Impulse: supplies namespace/name, current counters, and generation data.
//
// Returns:
//   - error: wraps failures from listStoryRunsByImpulse or kubeutil.RetryableStatusUpdate.
func (r *ImpulseReconciler) syncImpulseTriggerStats(ctx context.Context, impulse *v1alpha1.Impulse) error {
	if impulse == nil {
		return nil
	}

	runs, err := r.listStoryRunsByImpulse(ctx, impulse)
	if err != nil {
		return fmt.Errorf("list StoryRuns referencing impulse %s: %w", impulse.Name, err)
	}

	stats := runstatus.AggregateImpulseTriggerStats(impulse, runs)

	metrics.RecordImpulseThrottledTriggers(impulse.Namespace, impulse.Name, impulse.Status.ThrottledTriggers)

	return kubeutil.RetryableStatusUpdate(ctx, r.Client, impulse, func(obj client.Object) {
		i := obj.(*v1alpha1.Impulse)
		runstatus.ApplyImpulseTriggerStats(i, impulse.Generation, stats)
	})
}

// listStoryRunsByImpulse returns StoryRuns in the Impulse namespace that
// reference the provided impulse name (internal/controller/impulse_controller.go:654-741).
//
// Behavior:
//   - Attempts to use the spec.impulseRef.name field index for efficient lookups
//     and returns immediately on success.
//   - Logs and records a controller index fallback metric before scanning the entire namespace
//     when the index list fails, then filters runs by Spec.ImpulseRef.Name.
//
// Arguments:
//   - ctx context.Context: propagated to each List call.
//   - impulse *v1alpha1.Impulse: supplies the namespace and name used for filtering.
//
// Returns:
//   - []runsv1alpha1.StoryRun: either the indexed result set or the filtered fallback slice.
//   - error: non-nil only when both list attempts fail.
//
// Side Effects:
//   - Issues up to two API List calls against StoryRuns in the impulse namespace.
func (r *ImpulseReconciler) listStoryRunsByImpulse(ctx context.Context, impulse *v1alpha1.Impulse) ([]runsv1alpha1.StoryRun, error) {
	var runs runsv1alpha1.StoryRunList
	nsOpt := client.InNamespace(impulse.Namespace)
	impulseKey := refs.NamespacedKey(impulse.Namespace, impulse.Name)
	if err := r.List(ctx, &runs, nsOpt, client.MatchingFields{contracts.IndexStoryRunImpulseRef: impulseKey}); err == nil {
		return runs.Items, nil
	} else {
		logging.NewControllerLogger(ctx, "impulse-triggers").
			WithValues("impulse", impulse.Name, "namespace", impulse.Namespace).
			Error(err, "StoryRun impulse index lookup failed; falling back to namespace scan")
		metrics.RecordControllerIndexFallback("impulse", contracts.IndexStoryRunImpulseRef)
	}

	if err := r.List(ctx, &runs, nsOpt); err != nil {
		return nil, err
	}

	filtered := make([]runsv1alpha1.StoryRun, 0, len(runs.Items))
	for _, run := range runs.Items {
		if run.Spec.ImpulseRef == nil || run.Spec.ImpulseRef.Name != impulse.Name {
			continue
		}
		filtered = append(filtered, run)
	}
	return filtered, nil
}

// updateImpulseStatus recalculates the Impulse phase/Ready condition based on replica
// counts and writes the result via kubeutil.RetryableStatusPatch.
//
// Behavior:
//   - Calls deriveImpulsePhase to classify the workload snapshot.
//   - Returns immediately when the current phase and ReadyReplicas already match the desired state.
//   - Uses kubeutil.RetryableStatusPatch to update phase, ObservedGeneration, replica counters, and
//     the Ready condition through conditions.Manager.
//
// Arguments:
//   - ctx context.Context: forwarded to kubeutil.RetryableStatusPatch.
//   - impulse *v1alpha1.Impulse: status object being mutated.
//   - mode enums.WorkloadMode: influences deriveImpulsePhase messaging.
//   - replicas, readyReplicas int32: workload counts copied into status and Ready condition logic.
//
// Returns:
//   - error: nil on success or a wrapped status-patch error on failure.
func (r *ImpulseReconciler) updateImpulseStatus(ctx context.Context, impulse *v1alpha1.Impulse, mode enums.WorkloadMode, replicas, readyReplicas int32) error {
	newPhase, message := deriveImpulsePhase(mode, replicas, readyReplicas)
	if impulse.Status.Phase == newPhase && impulse.Status.ReadyReplicas == readyReplicas {
		return nil
	}

	err := kubeutil.RetryableStatusPatch(ctx, r.Client, impulse, func(obj client.Object) {
		i := obj.(*v1alpha1.Impulse)
		i.Status.Phase = newPhase
		i.Status.ObservedGeneration = i.Generation
		i.Status.Replicas = replicas
		i.Status.ReadyReplicas = readyReplicas

		cm := conditions.NewConditionManager(i.Generation)
		if readyReplicas > 0 && (replicas == 0 || readyReplicas == replicas) {
			cm.SetCondition(&i.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonDeploymentReady, message)
		} else {
			cm.SetCondition(&i.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonReconciling, message)
		}
	})
	if err != nil {
		return fmt.Errorf("failed to update impulse status: %w", err)
	}
	return nil
}

// setImpulsePhase patches Impulse.Status.Phase plus the Ready condition using the
// shared runstatus helper so phase/condition updates stay aligned with StepRun and
// StoryRun controllers.
//
// Arguments:
//   - ctx context.Context: handed to the patch helper.
//   - impulse *v1alpha1.Impulse: resource whose status is updated.
//   - phase enums.Phase: desired phase to store.
//   - message string: human-readable condition message.
//
// Returns:
//   - error: nil on success or the status-patch error from kubeutil.RetryableStatusPatch.
func (r *ImpulseReconciler) setImpulsePhase(ctx context.Context, impulse *v1alpha1.Impulse, phase enums.Phase, message string) error {
	return runstatus.PatchImpulsePhase(ctx, r.Client, impulse, phase, message)
}

type workloadMetadata struct {
	Name      string
	Namespace string
	Labels    map[string]string
	Replicas  int32
	PodSpec   corev1.PodTemplateSpec
}

// buildWorkloadBase assembles shared workload metadata (name, labels, replicas,
// and pod template) for Impulse workloads so Deployment/StatefulSet builders
// stay aligned.
//
// Behavior:
//   - Uses newImpulseIdentity to derive a deterministic name plus selector labels.
//   - Fixes replicas at one per the WorkloadSpec contract.
//   - Delegates pod construction to buildImpulsePodTemplate so all workload types
//     reuse the same container/env/resource wiring.
//
// Arguments:
//   - impulse *v1alpha1.Impulse: supplies namespace/name for identity derivation.
//   - engramTemplate *catalogv1alpha1.EngramTemplate: forwards secret schema data
//     into buildImpulsePodTemplate.
//   - execCfg *config.ResolvedExecutionConfig: contributes service account, probes,
//     storage, and resource settings consumed by the pod builder.
//
// Returns:
//   - workloadMetadata: includes the workload name, namespace, shared labels map,
//     single-replica pointer, and PodTemplateSpec.
//
// Notes:
//   - The returned Labels map is shared by pointer; callers must copy it before
//     injecting component-specific keys to keep selectors aligned.
func (r *ImpulseReconciler) buildWorkloadBase(
	impulse *v1alpha1.Impulse,
	engramTemplate *catalogv1alpha1.EngramTemplate,
	execCfg *config.ResolvedExecutionConfig,
	deliveryPolicy *v1alpha1.TriggerDeliveryPolicy,
) workloadMetadata {
	identity := newImpulseIdentity(impulse)
	replicas := int32(1)
	podSpec := r.buildImpulsePodTemplate(impulse, execCfg, identity.selectorLabels, engramTemplate, deliveryPolicy)

	return workloadMetadata{
		Name:      identity.name,
		Namespace: impulse.Namespace,
		Labels:    identity.selectorLabels, // shared pointer; copy before per-resource mutations.
		Replicas:  replicas,
		PodSpec:   podSpec,
	}
}

// buildDeploymentForImpulse returns the desired Deployment for an Impulse using
// the shared workload metadata from buildWorkloadBase.
//
// Behavior:
//   - Pulls name, labels, replica pointer, and pod template from buildWorkloadBase
//     so Deployments and StatefulSets evolve together.
//   - Copies the shared label map into metadata and the selector’s MatchLabels to
//     guarantee pods remain selectable by Services/StatefulSets.
//
// Arguments:
//   - impulse *v1alpha1.Impulse: provides namespace/name for metadata.
//   - engramTemplate *catalogv1alpha1.EngramTemplate: forwards template secrets to the pod.
//   - execCfg *config.ResolvedExecutionConfig: supplies execution details consumed by the base helper.
//
// Returns:
//   - *appsv1.Deployment: in-memory Deployment object ready for apply/patch flows.
func (r *ImpulseReconciler) buildDeploymentForImpulse(
	impulse *v1alpha1.Impulse,
	engramTemplate *catalogv1alpha1.EngramTemplate,
	execCfg *config.ResolvedExecutionConfig,
	deliveryPolicy *v1alpha1.TriggerDeliveryPolicy,
) *appsv1.Deployment {
	base := r.buildWorkloadBase(impulse, engramTemplate, execCfg, deliveryPolicy)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      base.Name,
			Namespace: base.Namespace,
			Labels:    base.Labels, // copy before mutation to avoid selector drift.
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &base.Replicas,
			Selector: &metav1.LabelSelector{MatchLabels: base.Labels},
			Template: base.PodSpec,
		},
	}
}

// buildStatefulSetForImpulse returns the StatefulSet used when Impulse workloads
// run in StatefulSet mode, combining shared metadata with the provided Service.
//
// Behavior:
//   - Reuses buildWorkloadBase for name/labels/replicas/pod template.
//   - Sets Spec.ServiceName to the caller-supplied headless Service.
//   - Propagates spec.workload.statefulSet.podManagementPolicy when present.
//
// Arguments:
//   - impulse *v1alpha1.Impulse: contributes namespace and workload overrides.
//   - engramTemplate *catalogv1alpha1.EngramTemplate: forwards secret artifacts.
//   - execCfg *config.ResolvedExecutionConfig: feeds execution details into the base helper.
//   - serviceName string: headless Service backing the StatefulSet.
//
// Returns:
//   - *appsv1.StatefulSet: ready for reconciliation via reconcileWorkloadGeneric.
func (r *ImpulseReconciler) buildStatefulSetForImpulse(
	impulse *v1alpha1.Impulse,
	engramTemplate *catalogv1alpha1.EngramTemplate,
	execCfg *config.ResolvedExecutionConfig,
	deliveryPolicy *v1alpha1.TriggerDeliveryPolicy,
	serviceName string,
) *appsv1.StatefulSet {
	base := r.buildWorkloadBase(impulse, engramTemplate, execCfg, deliveryPolicy)

	stsSpec := appsv1.StatefulSetSpec{
		ServiceName: serviceName,
		Selector:    &metav1.LabelSelector{MatchLabels: base.Labels},
		Template:    base.PodSpec,
		Replicas:    &base.Replicas,
	}
	if impulse.Spec.Workload != nil && impulse.Spec.Workload.StatefulSet != nil && impulse.Spec.Workload.StatefulSet.PodManagementPolicy != nil {
		stsSpec.PodManagementPolicy = appsv1.PodManagementPolicyType(*impulse.Spec.Workload.StatefulSet.PodManagementPolicy)
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      base.Name,
			Namespace: impulse.Namespace,
			Labels:    base.Labels, // shared map; copy before per-resource mutations.
		},
		Spec: stsSpec,
	}
}

// buildImpulsePodTemplate returns the PodTemplateSpec shared by Deployment and
// StatefulSet builders, wiring Impulse metadata, execution config, storage env,
// and Engram secrets into a single pod definition.
//
// Behavior:
//   - Emits env vars for Impulse/Story identity plus the debug flag and embeds
//     spec.with when provided.
//   - Applies service account, automount, probes, resources, and security contexts
//     from the resolved execution config.
//   - Computes storage timeouts via FormatStorageTimeout, applies storage env using
//     ApplyStorageEnv, and injects Engram secret artifacts via ApplySecretArtifacts.
//
// Arguments:
//   - impulse *v1alpha1.Impulse: supplies namespace/name, Story reference, and With payload.
//   - execCfg *config.ResolvedExecutionConfig: contributes service account, security contexts,
//     probes, resources, and storage configuration.
//   - labels map[string]string: applied to the pod template so selectors stay aligned.
//   - engramTemplate *catalogv1alpha1.EngramTemplate: drives secret artifact attachments.
//
// Returns:
//   - corev1.PodTemplateSpec: pod template consumed by Deployment/StatefulSet builders.
//
// Notes:
//   - spec.with contents become plain-text env vars; move secrets to referenced Secrets when needed.
//   - Env/volume ordering must stay deterministic so spec-drift detection remains stable.
func (r *ImpulseReconciler) buildImpulsePodTemplate(
	impulse *v1alpha1.Impulse,
	execCfg *config.ResolvedExecutionConfig,
	labels map[string]string,
	engramTemplate *catalogv1alpha1.EngramTemplate,
	deliveryPolicy *v1alpha1.TriggerDeliveryPolicy,
) corev1.PodTemplateSpec {
	storyKey := impulse.Spec.StoryRef.ToNamespacedName(impulse)
	envVars := []corev1.EnvVar{
		{Name: contracts.ImpulseNameEnv, Value: impulse.Name},
		{Name: contracts.ImpulseNamespaceEnv, Value: impulse.Namespace},
		{Name: contracts.TargetStoryNameEnv, Value: storyKey.Name},
		{Name: contracts.TargetStoryNamespaceEnv, Value: storyKey.Namespace},
		{Name: contracts.DebugEnv, Value: strconv.FormatBool(execCfg.DebugLogs)},
	}
	if impulse.Spec.With != nil && impulse.Spec.With.Raw != nil {
		kubeutil.AppendConfigEnvVar(&envVars, impulse.Spec.With.Raw)
	}
	appendTriggerDeliveryEnvVars(&envVars, deliveryPolicy)
	appendTriggerThrottleEnvVars(&envVars, impulse.Spec.Throttle)

	podSpec := podspec.Build(podspec.Config{
		ContainerName:  "impulse",
		Labels:         labels,
		EnvVars:        envVars,
		ResolvedConfig: execCfg,
	})

	fallbackTimeout := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultStorageTimeoutSeconds
	storageTimeout := podspec.FormatStorageTimeout(execCfg, fallbackTimeout)
	podspec.ApplyStorageEnv(execCfg, &podSpec.Spec, &podSpec.Spec.Containers[0], storageTimeout)
	podspec.ApplySecretArtifacts(engramTemplate, execCfg, &podSpec)
	return podSpec
}

func appendTriggerDeliveryEnvVars(envVars *[]corev1.EnvVar, policy *v1alpha1.TriggerDeliveryPolicy) {
	if envVars == nil || policy == nil {
		return
	}
	if policy.Dedupe != nil {
		if policy.Dedupe.Mode != nil {
			if mode := strings.TrimSpace(string(*policy.Dedupe.Mode)); mode != "" {
				*envVars = append(*envVars, corev1.EnvVar{Name: triggerDedupeModeEnv, Value: mode})
			}
		}
		if policy.Dedupe.KeyTemplate != nil {
			if tmpl := strings.TrimSpace(*policy.Dedupe.KeyTemplate); tmpl != "" {
				*envVars = append(*envVars, corev1.EnvVar{Name: triggerDedupeKeyTemplateEnv, Value: tmpl})
			}
		}
	}
	if policy.Retry != nil {
		if policy.Retry.MaxAttempts != nil {
			value := strconv.FormatInt(int64(*policy.Retry.MaxAttempts), 10)
			*envVars = append(*envVars, corev1.EnvVar{Name: triggerRetryMaxAttemptsEnv, Value: value})
		}
		if policy.Retry.BaseDelay != nil {
			if delay := strings.TrimSpace(*policy.Retry.BaseDelay); delay != "" {
				*envVars = append(*envVars, corev1.EnvVar{Name: triggerRetryBaseDelayEnv, Value: delay})
			}
		}
		if policy.Retry.MaxDelay != nil {
			if delay := strings.TrimSpace(*policy.Retry.MaxDelay); delay != "" {
				*envVars = append(*envVars, corev1.EnvVar{Name: triggerRetryMaxDelayEnv, Value: delay})
			}
		}
		if policy.Retry.Backoff != nil {
			if backoff := strings.TrimSpace(string(*policy.Retry.Backoff)); backoff != "" {
				*envVars = append(*envVars, corev1.EnvVar{Name: triggerRetryBackoffEnv, Value: backoff})
			}
		}
	}
}

func appendTriggerThrottleEnvVars(envVars *[]corev1.EnvVar, throttle *v1alpha1.TriggerThrottlePolicy) {
	if envVars == nil || throttle == nil {
		return
	}
	if throttle.MaxInFlight != nil {
		value := strconv.FormatInt(int64(*throttle.MaxInFlight), 10)
		*envVars = append(*envVars, corev1.EnvVar{Name: triggerThrottleMaxInFlight, Value: value})
	}
	if throttle.RatePerSecond != nil {
		value := strconv.FormatInt(int64(*throttle.RatePerSecond), 10)
		*envVars = append(*envVars, corev1.EnvVar{Name: triggerThrottleRateEnv, Value: value})
	}
	if throttle.Burst != nil {
		value := strconv.FormatInt(int64(*throttle.Burst), 10)
		*envVars = append(*envVars, corev1.EnvVar{Name: triggerThrottleBurstEnv, Value: value})
	}
}

func resolveTriggerDeliveryPolicy(template *catalogv1alpha1.ImpulseTemplate, impulse *v1alpha1.Impulse) *v1alpha1.TriggerDeliveryPolicy {
	var resolved *v1alpha1.TriggerDeliveryPolicy
	if template != nil && template.Spec.DeliveryPolicy != nil {
		resolved = convertTriggerDeliveryPolicy(template.Spec.DeliveryPolicy)
	}
	if impulse == nil {
		return resolved
	}
	return mergeTriggerDeliveryPolicy(resolved, impulse.Spec.DeliveryPolicy)
}

func mergeTriggerDeliveryPolicy(base, override *v1alpha1.TriggerDeliveryPolicy) *v1alpha1.TriggerDeliveryPolicy {
	if override == nil {
		return base
	}
	if base == nil {
		return cloneTriggerDeliveryPolicy(override)
	}
	if override.Dedupe != nil {
		if base.Dedupe == nil {
			base.Dedupe = &v1alpha1.TriggerDedupePolicy{}
		}
		if override.Dedupe.Mode != nil {
			val := *override.Dedupe.Mode
			base.Dedupe.Mode = &val
		}
		if override.Dedupe.KeyTemplate != nil {
			val := *override.Dedupe.KeyTemplate
			base.Dedupe.KeyTemplate = &val
		}
	}
	if override.Retry != nil {
		if base.Retry == nil {
			base.Retry = &v1alpha1.TriggerRetryPolicy{}
		}
		if override.Retry.MaxAttempts != nil {
			val := *override.Retry.MaxAttempts
			base.Retry.MaxAttempts = &val
		}
		if override.Retry.BaseDelay != nil {
			val := *override.Retry.BaseDelay
			base.Retry.BaseDelay = &val
		}
		if override.Retry.MaxDelay != nil {
			val := *override.Retry.MaxDelay
			base.Retry.MaxDelay = &val
		}
		if override.Retry.Backoff != nil {
			val := *override.Retry.Backoff
			base.Retry.Backoff = &val
		}
	}
	return base
}

func convertTriggerDeliveryPolicy(in *catalogv1alpha1.TriggerDeliveryPolicy) *v1alpha1.TriggerDeliveryPolicy {
	if in == nil {
		return nil
	}
	out := &v1alpha1.TriggerDeliveryPolicy{}
	if in.Dedupe != nil {
		out.Dedupe = &v1alpha1.TriggerDedupePolicy{}
		if in.Dedupe.Mode != nil {
			val := v1alpha1.TriggerDedupeMode(*in.Dedupe.Mode)
			out.Dedupe.Mode = &val
		}
		if in.Dedupe.KeyTemplate != nil {
			val := *in.Dedupe.KeyTemplate
			out.Dedupe.KeyTemplate = &val
		}
	}
	if in.Retry != nil {
		out.Retry = &v1alpha1.TriggerRetryPolicy{}
		if in.Retry.MaxAttempts != nil {
			val := *in.Retry.MaxAttempts
			out.Retry.MaxAttempts = &val
		}
		if in.Retry.BaseDelay != nil {
			val := *in.Retry.BaseDelay
			out.Retry.BaseDelay = &val
		}
		if in.Retry.MaxDelay != nil {
			val := *in.Retry.MaxDelay
			out.Retry.MaxDelay = &val
		}
		if in.Retry.Backoff != nil {
			val := *in.Retry.Backoff
			out.Retry.Backoff = &val
		}
	}
	return out
}

func cloneTriggerDeliveryPolicy(in *v1alpha1.TriggerDeliveryPolicy) *v1alpha1.TriggerDeliveryPolicy {
	if in == nil {
		return nil
	}
	out := &v1alpha1.TriggerDeliveryPolicy{}
	if in.Dedupe != nil {
		out.Dedupe = &v1alpha1.TriggerDedupePolicy{}
		if in.Dedupe.Mode != nil {
			val := *in.Dedupe.Mode
			out.Dedupe.Mode = &val
		}
		if in.Dedupe.KeyTemplate != nil {
			val := *in.Dedupe.KeyTemplate
			out.Dedupe.KeyTemplate = &val
		}
	}
	if in.Retry != nil {
		out.Retry = &v1alpha1.TriggerRetryPolicy{}
		if in.Retry.MaxAttempts != nil {
			val := *in.Retry.MaxAttempts
			out.Retry.MaxAttempts = &val
		}
		if in.Retry.BaseDelay != nil {
			val := *in.Retry.BaseDelay
			out.Retry.BaseDelay = &val
		}
		if in.Retry.MaxDelay != nil {
			val := *in.Retry.MaxDelay
			out.Retry.MaxDelay = &val
		}
		if in.Retry.Backoff != nil {
			val := *in.Retry.Backoff
			out.Retry.Backoff = &val
		}
	}
	return out
}

// reconcileService ensures the Service for an Impulse matches the desired state.
//
// Behavior:
//   - If no service ports are configured and require is false, deletes any orphan Service.
//   - If require is true but no ports exist, returns an error.
//   - Otherwise, creates or updates the Service via kubeutil.EnsureService.
//   - Logs create/update/noop outcomes at appropriate verbosity levels.
//
// Arguments:
//   - ctx context.Context: propagated to API calls.
//   - impulse *v1alpha1.Impulse: owner reference for the Service.
//   - execCfg *config.ResolvedExecutionConfig: supplies service ports, labels, and annotations.
//   - serviceName string: the desired Service name.
//   - require bool: when true, errors if no ports are configured (StatefulSet mode).
//
// Returns:
//   - *corev1.Service: the reconciled Service, or nil if deleted/skipped.
//   - error: on create/update/delete failures or when require is true with no ports.
func (r *ImpulseReconciler) reconcileService(ctx context.Context, impulse *v1alpha1.Impulse, execCfg *config.ResolvedExecutionConfig, serviceName string, require bool) (*corev1.Service, error) {
	log := logging.NewReconcileLogger(ctx, "impulse-service").WithValues("impulse", impulse.Name)

	if !hasServicePorts(execCfg) {
		existing := &corev1.Service{}
		err := r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: impulse.Namespace}, existing)
		return r.handleServiceWithoutPorts(ctx, existing, err, log, require)
	}

	desired := r.buildServiceForImpulse(impulse, execCfg, serviceName)
	service, result, err := kubeutil.EnsureService(ctx, r.Client, r.Scheme, impulse, desired)
	if err != nil {
		return nil, err
	}
	switch result {
	case controllerutil.OperationResultCreated:
		log.Info("Created Service for Impulse", "service", serviceName)
	case controllerutil.OperationResultUpdated:
		log.Info("Updated Service for Impulse", "service", serviceName)
	default:
		log.V(1).Info("Service already up to date", "service", serviceName)
	}
	return service, nil
}

// hasServicePorts reports whether the execution config defines any service ports.
//
// Behavior:
//   - Returns false when execCfg is nil or ServicePorts slice is empty.
//   - Used to determine whether a Service should be created for the Impulse.
//
// Arguments:
//   - execCfg *config.ResolvedExecutionConfig: the resolved execution configuration.
//
// Returns:
//   - bool: true if at least one service port is configured.
func hasServicePorts(execCfg *config.ResolvedExecutionConfig) bool {
	return execCfg != nil && len(execCfg.ServicePorts) > 0
}

// composeImpulseWorkloadName builds a deterministic workload name from Impulse metadata.
//
// Behavior:
//   - Combines impulse name, optional template name (if different), and "impulse" suffix.
//   - Uses kubeutil.Compose to ensure the result is DNS-compliant.
//   - Returns "impulse" when the input is nil.
//
// Arguments:
//   - impulse *v1alpha1.Impulse: supplies name and template reference.
//
// Returns:
//   - string: the composed workload name.
func composeImpulseWorkloadName(impulse *v1alpha1.Impulse) string {
	if impulse == nil {
		return "impulse"
	}
	parts := []string{impulse.Name}
	templateName := impulse.Spec.TemplateRef.Name
	if templateName != "" && templateName != impulse.Name {
		parts = append(parts, templateName)
	}
	parts = append(parts, "impulse")
	return kubeutil.ComposeName(parts...)
}

// impulseIdentity bundles a workload name and selector labels for an Impulse.
type impulseIdentity struct {
	name           string
	selectorLabels map[string]string
}

// newImpulseIdentity derives the workload name and standard Kubernetes labels for an Impulse.
//
// Behavior:
//   - Calls composeImpulseWorkloadName to derive a DNS-compliant name.
//   - Returns labels following app.kubernetes.io conventions (name, instance, managed-by).
//
// Arguments:
//   - impulse *v1alpha1.Impulse: supplies the impulse name for the instance label.
//
// Returns:
//   - impulseIdentity: contains the name and selectorLabels for workload/service creation.
func newImpulseIdentity(impulse *v1alpha1.Impulse) impulseIdentity {
	name := composeImpulseWorkloadName(impulse)
	labels := map[string]string{
		"app.kubernetes.io/name":       "bobrapet-impulse",
		"app.kubernetes.io/instance":   impulse.Name,
		"app.kubernetes.io/managed-by": "bobrapet-operator",
	}
	return impulseIdentity{name: name, selectorLabels: labels}
}

// handleServiceWithoutPorts manages the Service when no ports are configured.
//
// Behavior:
//   - If require is true (StatefulSet mode), returns an error since Services are mandatory.
//   - If the existing Service lookup returned NotFound, logs and returns nil (no action).
//   - Otherwise, deletes the orphaned Service using kubeutil.DeleteIfExists.
//
// Arguments:
//   - ctx context.Context: propagated to delete operations.
//   - existing *corev1.Service: the existing Service from a prior Get call.
//   - err error: the error from the Get call (may be NotFound).
//   - log *logging.ControllerLogger: for logging actions.
//   - require bool: when true, ports are mandatory and absence is an error.
//
// Returns:
//   - *corev1.Service: always nil (Service is deleted or absent).
//   - error: on require violation or delete failure.
func (r *ImpulseReconciler) handleServiceWithoutPorts(
	ctx context.Context,
	existing *corev1.Service,
	err error,
	log *logging.ControllerLogger,
	require bool,
) (*corev1.Service, error) {
	if require {
		return nil, fmt.Errorf("service ports are required when workload mode is StatefulSet")
	}
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Service already absent for Impulse; skipping delete")
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get Service: %w", err)
	}
	if deleteErr := kubeutil.DeleteIfExists(ctx, r.Client, existing, log.WithValues("service", existing.Name)); deleteErr != nil {
		return nil, fmt.Errorf("failed to delete Service without ports: %w", deleteErr)
	}
	return nil, nil
}

// buildServiceForImpulse constructs the desired Service manifest for an Impulse.
//
// Behavior:
//   - Derives selector labels from newImpulseIdentity for pod selection.
//   - Merges labels and annotations from execCfg and impulse.Spec.Service overrides.
//   - Clones service ports from execCfg.ServicePorts.
//   - Applies service type from impulse.Spec.Service or defaults to ClusterIP.
//
// Arguments:
//   - impulse *v1alpha1.Impulse: supplies namespace and service customization.
//   - execCfg *config.ResolvedExecutionConfig: contributes ports, labels, and annotations.
//   - serviceName string: the Service name to use.
//
// Returns:
//   - *corev1.Service: the desired Service manifest (not yet created/applied).
func (r *ImpulseReconciler) buildServiceForImpulse(impulse *v1alpha1.Impulse, execCfg *config.ResolvedExecutionConfig, serviceName string) *corev1.Service {
	identity := newImpulseIdentity(impulse)
	selectorLabels := kubeutil.MergeStringMaps(identity.selectorLabels)
	if selectorLabels == nil {
		selectorLabels = map[string]string{}
	}

	serviceLabels := kubeutil.MergeStringMaps(selectorLabels, execCfg.ServiceLabels)
	if impulse.Spec.Service != nil && len(impulse.Spec.Service.Labels) > 0 {
		serviceLabels = kubeutil.MergeStringMaps(serviceLabels, impulse.Spec.Service.Labels)
	}

	annotations := kubeutil.MergeStringMaps(execCfg.ServiceAnnotations)
	if impulse.Spec.Service != nil && len(impulse.Spec.Service.Annotations) > 0 {
		annotations = kubeutil.MergeStringMaps(annotations, impulse.Spec.Service.Annotations)
	}

	ports := kubeutil.CloneServicePorts(execCfg.ServicePorts)
	serviceType := corev1.ServiceTypeClusterIP
	if impulse.Spec.Service != nil && impulse.Spec.Service.Type != "" {
		serviceType = corev1.ServiceType(impulse.Spec.Service.Type)
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        serviceName,
			Namespace:   impulse.Namespace,
			Labels:      serviceLabels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:     serviceType,
			Selector: selectorLabels,
			Ports:    ports,
		},
	}
}

// desiredWorkloadMode normalizes the requested workload mode and reports
// unsupported values so callers can emit warnings/events.
//
// Returns:
//   - enums.WorkloadMode: validated mode (Deployment or StatefulSet).
//   - string: unsupported mode when defaulting occurred; empty string when input was valid/empty.
func (r *ImpulseReconciler) desiredWorkloadMode(impulse *v1alpha1.Impulse) (enums.WorkloadMode, string) {
	if impulse == nil || impulse.Spec.Workload == nil || impulse.Spec.Workload.Mode == "" {
		return enums.WorkloadModeDeployment, ""
	}
	switch impulse.Spec.Workload.Mode {
	case enums.WorkloadModeDeployment, enums.WorkloadModeStatefulSet:
		return impulse.Spec.Workload.Mode, ""
	default:
		return enums.WorkloadModeDeployment, string(impulse.Spec.Workload.Mode)
	}
}

// resolveImpulseServiceName returns the Service name Impulse workloads should
// use, honoring the StatefulSet override when present and DNS-valid.
//
// Returns:
//   - string: validated Service name.
//   - string: rejected override when validation fails; empty string otherwise.
func (r *ImpulseReconciler) resolveImpulseServiceName(impulse *v1alpha1.Impulse) (string, string) {
	if impulse == nil {
		return "", ""
	}
	if impulse.Spec.Workload != nil && impulse.Spec.Workload.StatefulSet != nil {
		override := impulse.Spec.Workload.StatefulSet.ServiceName
		if override != "" {
			if len(validation.IsDNS1123Label(override)) == 0 {
				return override, ""
			}
			return newImpulseIdentity(impulse).name, override
		}
	}
	return newImpulseIdentity(impulse).name, ""
}

// deriveImpulsePhase converts workload mode and replica readiness into an Impulse
// phase plus the status message published to users.
//
// Behavior:
//   - Returns PhasePending when no replicas are ready.
//   - Returns PhaseRunning with scaling message when partially ready.
//   - Returns PhaseRunning with active message when fully ready.
//
// Arguments:
//   - mode enums.WorkloadMode: Deployment or StatefulSet, used for messaging.
//   - replicas int32: desired replica count.
//   - readyReplicas int32: current ready replica count.
//
// Returns:
//   - enums.Phase: the computed phase for the Impulse status.
//   - string: human-readable status message.
func deriveImpulsePhase(mode enums.WorkloadMode, replicas, readyReplicas int32) (enums.Phase, string) {
	workloadKind := "Deployment"
	if mode == enums.WorkloadModeStatefulSet {
		workloadKind = "StatefulSet"
	}

	switch {
	case readyReplicas == 0:
		return enums.PhasePending, fmt.Sprintf("Waiting for %s pods to become ready.", workloadKind)
	case replicas > 0 && readyReplicas < replicas:
		return enums.PhaseRunning, fmt.Sprintf("%s is scaling up.", workloadKind)
	default:
		return enums.PhaseRunning, "Impulse is active and listening for events."
	}
}

// convertImpulseTemplate projects an ImpulseTemplate into the EngramTemplate
// schema expected by shared execution-config helpers.
//
// Behavior:
//   - Returns nil when template is nil to allow nil propagation.
//   - Copies TemplateSpec fields (Version, Description, Image, etc.) into an EngramTemplate.
//   - Enables reuse of shared helpers that expect EngramTemplate.
//
// Arguments:
//   - template *catalogv1alpha1.ImpulseTemplate: the source ImpulseTemplate.
//
// Returns:
//   - *catalogv1alpha1.EngramTemplate: the projected template, or nil.
func convertImpulseTemplate(template *catalogv1alpha1.ImpulseTemplate) *catalogv1alpha1.EngramTemplate {
	if template == nil {
		return nil
	}
	return &catalogv1alpha1.EngramTemplate{
		Spec: catalogv1alpha1.EngramTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Version:        template.Spec.Version,
				Description:    template.Spec.Description,
				Image:          template.Spec.Image,
				SupportedModes: template.Spec.SupportedModes,
				Execution:      template.Spec.Execution,
				SecretSchema:   template.Spec.SecretSchema,
				ConfigSchema:   template.Spec.ConfigSchema,
			},
		},
	}
}

// SetupWithManager wires Impulse watches with storm-proof predicates.
//
// Behavior:
//   - Registers the controller for Impulse resources with GenerationChangedPredicate.
//   - Watches ImpulseTemplates and Stories to requeue dependent Impulses.
//   - Watches StoryRuns with meaningful-update predicate to update trigger stats.
//   - Owns Deployments, StatefulSets, and Services with generation-only predicates.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//   - opts controller.Options: controller configuration (concurrency, etc.).
//
// Returns:
//   - error: nil on success, or controller setup error.
//
// Side Effects:
//   - Registers the event recorder for impulse-controller.
func (r *ImpulseReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	r.Recorder = mgr.GetEventRecorderFor("impulse-controller")

	workloadPred := predicate.GenerationChangedPredicate{}
	genOnly := predicate.GenerationChangedPredicate{}

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Impulse{}, builder.WithPredicates(genOnly)).
		Watches(&catalogv1alpha1.ImpulseTemplate{}, handler.EnqueueRequestsFromMapFunc(r.mapImpulseTemplateToImpulses), builder.WithPredicates(genOnly)).
		Watches(&v1alpha1.Story{}, handler.EnqueueRequestsFromMapFunc(r.mapStoryToImpulses), builder.WithPredicates(genOnly)).
		Watches(&runsv1alpha1.StoryRun{}, handler.EnqueueRequestsFromMapFunc(r.mapStoryRunToImpulse), builder.WithPredicates(rec.StoryRunMeaningfulUpdates())).
		Owns(&appsv1.Deployment{}, builder.WithPredicates(workloadPred)).
		Owns(&appsv1.StatefulSet{}, builder.WithPredicates(workloadPred)).
		Owns(&corev1.Service{}, builder.WithPredicates(workloadPred)).
		WithOptions(opts).
		Complete(r)
}

// mapImpulseTemplateToImpulses converts ImpulseTemplate watch events into reconcile
// requests for all Impulses referencing the template.
//
// Behavior:
//   - Uses enqueueImpulsesByField with spec.templateRef.name index.
//   - Returns requests for all matching Impulses across namespaces.
//
// Arguments:
//   - ctx context.Context: propagated to field listing.
//   - obj client.Object: the ImpulseTemplate triggering the watch.
//
// Returns:
//   - []reconcile.Request: requests for Impulses referencing the template.
func (r *ImpulseReconciler) mapImpulseTemplateToImpulses(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.enqueueImpulsesByField(ctx, "spec.templateRef.name", obj.GetName(), nil, "impulse-template")
}

// mapStoryToImpulses converts Story watch events into reconcile requests for
// Impulses targeting that Story.
//
// Behavior:
//   - Uses enqueueImpulsesByField with spec.storyRef.key index.
//   - Matches Impulses by the Story's namespaced key.
//
// Arguments:
//   - ctx context.Context: propagated to field listing.
//   - obj client.Object: the Story triggering the watch.
//
// Returns:
//   - []reconcile.Request: requests for Impulses referencing the Story.
func (r *ImpulseReconciler) mapStoryToImpulses(ctx context.Context, obj client.Object) []reconcile.Request {
	storyNamespace := obj.GetNamespace()
	storyKey := refs.NamespacedKey(storyNamespace, obj.GetName())
	return r.enqueueImpulsesByField(
		ctx,
		contracts.IndexImpulseStoryRef,
		storyKey,
		nil,
		"story-watch",
	)
}

// mapStoryRunToImpulse converts StoryRun watch events into a reconcile request
// for the Impulse that triggered the run.
//
// Behavior:
//   - Extracts the Impulse reference from StoryRun.Spec.ImpulseRef.
//   - Returns nil when ImpulseRef is absent or has no name.
//   - Resolves namespace from ImpulseRef.Namespace or defaults to StoryRun's namespace.
//
// Arguments:
//   - _ context.Context: unused.
//   - obj client.Object: the StoryRun triggering the watch.
//
// Returns:
//   - []reconcile.Request: single request for the referenced Impulse, or nil.
func (r *ImpulseReconciler) mapStoryRunToImpulse(_ context.Context, obj client.Object) []reconcile.Request {
	run, ok := obj.(*runsv1alpha1.StoryRun)
	if !ok || run.Spec.ImpulseRef == nil || run.Spec.ImpulseRef.Name == "" {
		return nil
	}
	impulseNamespace := run.Namespace
	if run.Spec.ImpulseRef.Namespace != nil && *run.Spec.ImpulseRef.Namespace != "" {
		impulseNamespace = *run.Spec.ImpulseRef.Namespace
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Name: run.Spec.ImpulseRef.Name, Namespace: impulseNamespace},
	}}
}

// enqueueImpulsesByField lists Impulses keyed by a field index, applies an
// optional filter, and returns reconcile requests for each match.
//
// Behavior:
//   - Attempts indexed lookup using client.MatchingFields.
//   - On index failure, falls back to full namespace scan with manual filtering.
//   - Applies optional filter function to exclude non-matching Impulses.
//   - Logs errors and fallback actions at appropriate levels.
//
// Arguments:
//   - ctx context.Context: propagated to List calls.
//   - fieldKey string: the field index key (e.g., "spec.templateRef.name", "spec.storyRef.key").
//   - fieldValue string: the value to match against.
//   - filter func(*v1alpha1.Impulse) bool: optional additional filter, may be nil.
//   - watchName string: identifier for logging context.
//
// Returns:
//   - []reconcile.Request: reconcile requests for matching Impulses.
func (r *ImpulseReconciler) enqueueImpulsesByField(
	ctx context.Context,
	fieldKey, fieldValue string,
	filter func(*v1alpha1.Impulse) bool,
	watchName string,
) []reconcile.Request {
	logger := logging.NewControllerLogger(ctx, "impulse-watch").
		WithValues("watch", watchName, "field", fieldKey, "value", fieldValue)
	var impulses v1alpha1.ImpulseList
	if err := r.List(ctx, &impulses, client.MatchingFields{fieldKey: fieldValue}); err != nil {
		logger.Error(err, "Failed to list Impulses for watch fan-out; attempting fallback scan")
		var fallback v1alpha1.ImpulseList
		if listErr := r.List(ctx, &fallback); listErr != nil {
			logger.Error(listErr, "Fallback Impulse scan failed; dropping watch event")
			return nil
		}
		logger.V(1).Info("Falling back to full Impulse scan for fan-out", "candidateCount", len(fallback.Items))
		impulses.Items = filterImpulsesByField(fallback.Items, fieldKey, fieldValue)
	}

	requests := make([]reconcile.Request, 0, len(impulses.Items))
	for i := range impulses.Items {
		impulse := &impulses.Items[i]
		if filter != nil && !filter(impulse) {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: impulse.GetName(), Namespace: impulse.GetNamespace()},
		})
	}
	return requests
}

// deploymentSpecChanged reports whether the Deployment spec has drifted from desired.
//
// Behavior:
//   - Delegates to workload.PodTemplateChanged for pod template comparison.
//   - Used by reconcileDeployment to decide whether an update is needed.
//
// Arguments:
//   - current *appsv1.Deployment: the existing Deployment from the cluster.
//   - desired *appsv1.Deployment: the desired Deployment spec.
//
// Returns:
//   - bool: true if the pod template has changed.
func deploymentSpecChanged(current, desired *appsv1.Deployment) bool {
	return workload.PodTemplateChanged(&current.Spec.Template, &desired.Spec.Template)
}

// statefulSetSpecChanged reports whether the StatefulSet spec has drifted from desired.
//
// Behavior:
//   - Compares PodManagementPolicy in addition to pod template.
//   - Delegates pod template comparison to workload.PodTemplateChanged.
//   - Used by reconcileStatefulSetWorkload to decide whether an update is needed.
//
// Arguments:
//   - current *appsv1.StatefulSet: the existing StatefulSet from the cluster.
//   - desired *appsv1.StatefulSet: the desired StatefulSet spec.
//
// Returns:
//   - bool: true if PodManagementPolicy or pod template has changed.
func statefulSetSpecChanged(current, desired *appsv1.StatefulSet) bool {
	if current.Spec.PodManagementPolicy != desired.Spec.PodManagementPolicy {
		return true
	}
	return workload.PodTemplateChanged(&current.Spec.Template, &desired.Spec.Template)
}

// filterImpulsesByField filters Impulses by matching a field value.
//
// Behavior:
//   - Returns nil when fieldValue is empty to avoid false positives.
//   - Iterates through list and includes Impulses where impulseMatchesField returns true.
//   - Used as fallback when indexed lookups fail in enqueueImpulsesByField.
//
// Arguments:
//   - list []v1alpha1.Impulse: the full list of Impulses to filter.
//   - fieldKey string: the field path to match (e.g., "spec.templateRef.name").
//   - fieldValue string: the value to match against.
//
// Returns:
//   - []v1alpha1.Impulse: filtered list of matching Impulses.
func filterImpulsesByField(list []v1alpha1.Impulse, fieldKey, fieldValue string) []v1alpha1.Impulse {
	if fieldValue == "" {
		return nil
	}
	matched := make([]v1alpha1.Impulse, 0, len(list))
	for i := range list {
		if impulseMatchesField(&list[i], fieldKey, fieldValue) {
			matched = append(matched, list[i])
		}
	}
	return matched
}

// impulseMatchesField checks if an Impulse's field matches the expected value.
//
// Behavior:
//   - Supports "spec.templateRef.name", "spec.storyRef.name", and "spec.storyRef.key" field keys.
//   - Returns false for unsupported field keys.
//   - Used by filterImpulsesByField for fallback filtering.
//
// Arguments:
//   - impulse *v1alpha1.Impulse: the Impulse to check.
//   - fieldKey string: the field path to match.
//   - fieldValue string: the expected value.
//
// Returns:
//   - bool: true if the field matches the value.
func impulseMatchesField(impulse *v1alpha1.Impulse, fieldKey, fieldValue string) bool {
	switch fieldKey {
	case "spec.templateRef.name":
		return impulse.Spec.TemplateRef.Name == fieldValue
	case "spec.storyRef.name":
		return impulse.Spec.StoryRef.Name == fieldValue
	case "spec.storyRef.key":
		targetNamespace := refs.ResolveNamespace(impulse, &impulse.Spec.StoryRef.ObjectReference)
		return refs.NamespacedKey(targetNamespace, impulse.Spec.StoryRef.Name) == fieldValue
	default:
		return false
	}
}
