package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/internal/controller/secretutil"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/patch"
)

// maybeConfigureTLSEnvAndMounts attempts to mount a TLS secret and set SDK TLS env vars.
// If the named secret exists, it mounts it to /var/run/tls and sets server/client envs.
// containerIndex selects which container to mutate (0 = engram container).
func (r *RealtimeEngramReconciler) maybeConfigureTLSEnvAndMounts(ctx context.Context, namespace, secretName string, podSpec *corev1.PodTemplateSpec, containerIndex int) error {
	if podSpec == nil || len(podSpec.Spec.Containers) <= containerIndex {
		return fmt.Errorf("podSpec is nil or container index is out of range")
	}
	// Probe secret existence
	var sec corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secretName}, &sec); err != nil {
		return err
	}
	// Add volume if not present
	volumeName := "engram-tls"
	foundVol := false
	for _, v := range podSpec.Spec.Volumes {
		if v.Name == volumeName {
			foundVol = true
			break
		}
	}
	if !foundVol {
		podSpec.Spec.Volumes = append(podSpec.Spec.Volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: secretName},
			},
		})
	}
	// Add volumeMount if not present
	mountPath := "/var/run/tls"
	c := &podSpec.Spec.Containers[containerIndex]
	foundMount := false
	for _, m := range c.VolumeMounts {
		if m.Name == volumeName {
			foundMount = true
			break
		}
	}
	if !foundMount {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: volumeName, MountPath: mountPath, ReadOnly: true})
	}
	// Set server TLS env (SDK server)
	c.Env = append(c.Env,
		corev1.EnvVar{Name: "BUBU_GRPC_TLS_CERT_FILE", Value: mountPath + "/tls.crt"},
		corev1.EnvVar{Name: "BUBU_GRPC_TLS_KEY_FILE", Value: mountPath + "/tls.key"},
	)
	// Optionally set client CA and client certs if present
	// We set envs unconditionally; SDK tolerates missing files by warnings/fallback where applicable.
	c.Env = append(c.Env,
		corev1.EnvVar{Name: "BUBU_GRPC_CA_FILE", Value: mountPath + "/ca.crt"},
		corev1.EnvVar{Name: "BUBU_GRPC_CLIENT_CERT_FILE", Value: mountPath + "/tls.crt"},
		corev1.EnvVar{Name: "BUBU_GRPC_CLIENT_KEY_FILE", Value: mountPath + "/tls.key"},
		corev1.EnvVar{Name: "BUBU_GRPC_REQUIRE_TLS", Value: "true"},
	)

	return nil
}

// getTLSSecretName reads TLS secret name from annotations in priority order:
// 1) engram.bubustack.io/tls-secret
// 2) bubustack.io/tls-secret
// If neither is set, returns empty string.
func getTLSSecretName(obj *metav1.ObjectMeta) string {
	if obj == nil {
		return ""
	}
	if v, ok := obj.Annotations["engram.bubustack.io/tls-secret"]; ok && v != "" {
		return v
	}
	if v, ok := obj.Annotations["bubustack.io/tls-secret"]; ok && v != "" {
		return v
	}
	return ""
}

const (
	// RealtimeEngramFinalizer is the finalizer for Engrams in deployment/statefulset mode.
	RealtimeEngramFinalizer = "engram.bubustack.io/realtime-finalizer"
)

// RealtimeEngramReconciler reconciles an Engram object for real-time workloads
type RealtimeEngramReconciler struct {
	config.ControllerDependencies
}

// +kubebuilder:rbac:groups=bubustack.io,resources=engrams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete

func (r *RealtimeEngramReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	log := logging.NewReconcileLogger(ctx, "realtime-engram").WithValues("engram", req.NamespacedName)
	startTime := time.Now()
	defer func() {
		metrics.RecordControllerReconcile("realtime-engram", time.Since(startTime), err)
	}()

	ctx, cancel := r.withReconcileTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	var engram v1alpha1.Engram
	if err := r.Get(ctx, req.NamespacedName, &engram); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// If not a realtime mode, ensure finalizer is removed and stop.
	if handled, err := r.handleNonRealtimeFinalizerIfNeeded(ctx, &engram, log); handled || err != nil {
		return ctrl.Result{}, err
	}

	// Handle deletion
	if handled, err := r.handleDeletionIfNeeded(ctx, &engram); handled || err != nil {
		return ctrl.Result{}, err
	}

	// Ensure finalizer exists
	if err := r.ensureRealtimeFinalizer(ctx, &engram, log); err != nil {
		return ctrl.Result{}, err
	}

	return r.reconcileRealtime(ctx, &engram, log)
}

// withReconcileTimeout applies a controller-configured timeout to the given context.
func (r *RealtimeEngramReconciler) withReconcileTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	if timeout <= 0 {
		return ctx, nil
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	return ctxWithTimeout, cancel
}

// handleNonRealtimeFinalizerIfNeeded removes the realtime finalizer for non-realtime modes.
func (r *RealtimeEngramReconciler) handleNonRealtimeFinalizerIfNeeded(ctx context.Context, engram *v1alpha1.Engram, log *logging.ControllerLogger) (bool, error) {
	if engram.Spec.Mode == enums.WorkloadModeDeployment || engram.Spec.Mode == enums.WorkloadModeStatefulSet {
		return false, nil
	}
	if controllerutil.ContainsFinalizer(engram, RealtimeEngramFinalizer) {
		controllerutil.RemoveFinalizer(engram, RealtimeEngramFinalizer)
		if err := r.Update(ctx, engram); err != nil {
			log.Error(err, "Failed to remove realtime finalizer from non-realtime Engram")
			return true, err
		}
	}
	return true, nil
}

// handleDeletionIfNeeded handles deletion flow when DeletionTimestamp is set.
func (r *RealtimeEngramReconciler) handleDeletionIfNeeded(ctx context.Context, engram *v1alpha1.Engram) (bool, error) {
	if engram.DeletionTimestamp.IsZero() {
		return false, nil
	}
	if err := r.reconcileDelete(ctx, engram); err != nil {
		return true, err
	}
	return true, nil
}

// ensureRealtimeFinalizer ensures the reconciler finalizer is present.
func (r *RealtimeEngramReconciler) ensureRealtimeFinalizer(ctx context.Context, engram *v1alpha1.Engram, log *logging.ControllerLogger) error {
	if controllerutil.ContainsFinalizer(engram, RealtimeEngramFinalizer) {
		return nil
	}
	controllerutil.AddFinalizer(engram, RealtimeEngramFinalizer)
	if err := r.Update(ctx, engram); err != nil {
		log.Error(err, "Failed to add realtime finalizer")
		return err
	}
	return nil
}

// markTemplateNotFound patches status to Blocked when the template is missing.
func (r *RealtimeEngramReconciler) markTemplateNotFound(ctx context.Context, engram *v1alpha1.Engram, cause error) (ctrl.Result, error) {
	updateErr := patch.RetryableStatusPatch(ctx, r.Client, engram, func(obj client.Object) {
		e := obj.(*v1alpha1.Engram)
		cm := conditions.NewConditionManager(e.Generation)
		cm.SetCondition(&e.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonTemplateNotFound, cause.Error())
		e.Status.Phase = enums.PhaseBlocked
	})
	return ctrl.Result{}, updateErr
}

// markTemplateResolvedReady marks Engram as Ready after template resolution.
func (r *RealtimeEngramReconciler) markTemplateResolvedReady(ctx context.Context, engram *v1alpha1.Engram) error {
	return patch.RetryableStatusPatch(ctx, r.Client, engram, func(obj client.Object) {
		e := obj.(*v1alpha1.Engram)
		cm := conditions.NewConditionManager(e.Generation)
		cm.SetCondition(&e.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonTemplateResolved, "Engram template was found and is available.")
		if e.Status.Phase == enums.PhaseBlocked {
			e.Status.Phase = enums.PhasePending
		}
	})
}

// ensureRunnerServiceAccount ensures a non-default ServiceAccount is used.
func (r *RealtimeEngramReconciler) ensureRunnerServiceAccount(ctx context.Context, engram *v1alpha1.Engram, resolved *config.ResolvedExecutionConfig, log *logging.ControllerLogger) error {
	if resolved.ServiceAccountName != "" && resolved.ServiceAccountName != "default" {
		return nil
	}
	saName := fmt.Sprintf("%s-engram-runner", engram.Name)
	if err := r.ensureEngramServiceAccount(ctx, engram, saName); err != nil {
		log.Error(err, "Failed to ensure ServiceAccount for realtime engram")
		return err
	}
	resolved.ServiceAccountName = saName
	return nil
}

// reconcileWorkload dispatches to the proper workload reconciler based on mode.
func (r *RealtimeEngramReconciler) reconcileWorkload(ctx context.Context, engram *v1alpha1.Engram, template *catalogv1alpha1.EngramTemplate, resolved *config.ResolvedExecutionConfig) error {
	mode := engram.Spec.Mode
	if mode == "" {
		mode = enums.WorkloadModeJob
	}
	if mode == enums.WorkloadModeJob {
		return nil
	}

	switch mode {
	case enums.WorkloadModeDeployment:
		return r.reconcileDeployment(ctx, engram, template, resolved)
	case enums.WorkloadModeStatefulSet:
		return r.reconcileStatefulSet(ctx, engram, resolved)
	default:
		return nil
	}
}

// reconcileRealtime performs the core realtime reconciliation steps after guards.
func (r *RealtimeEngramReconciler) reconcileRealtime(ctx context.Context, engram *v1alpha1.Engram, log *logging.ControllerLogger) (ctrl.Result, error) {
	// Fetch template
	template, err := r.getEngramTemplate(ctx, engram)
	if err != nil {
		if errors.IsNotFound(err) {
			return r.markTemplateNotFound(ctx, engram, err)
		}
		log.Error(err, "Failed to get EngramTemplate")
		return ctrl.Result{}, err
	}

	// Mark template resolved
	if err := r.markTemplateResolvedReady(ctx, engram); err != nil {
		log.Error(err, "Failed to update Engram status to Ready")
		return ctrl.Result{}, err
	}

	// Resolve execution config
	resolvedConfig, err := r.ConfigResolver.ResolveExecutionConfig(ctx, nil, nil, engram, template, nil)
	if err != nil {
		log.Error(err, "Failed to resolve execution config")
		return ctrl.Result{}, err
	}

	// Ensure ServiceAccount
	if err := r.ensureRunnerServiceAccount(ctx, engram, resolvedConfig, log); err != nil {
		return ctrl.Result{}, err
	}

	// Reconcile workload and service
	if err := r.reconcileWorkload(ctx, engram, template, resolvedConfig); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileService(ctx, engram, resolvedConfig); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// getEngramTemplate fetches the EngramTemplate for a given Engram.
// It first looks in the Engram's namespace, and if not found, falls back
// to the cluster-wide template namespace.
func (r *RealtimeEngramReconciler) getEngramTemplate(ctx context.Context, engram *v1alpha1.Engram) (*catalogv1alpha1.EngramTemplate, error) {
	template := &catalogv1alpha1.EngramTemplate{}
	// For cluster-scoped resources, the key has a name but no namespace.
	templateKey := client.ObjectKey{Name: engram.Spec.TemplateRef.Name}

	if err := r.Get(ctx, templateKey, template); err != nil {
		return nil, err // Return the original error
	}

	return template, nil
}

func (r *RealtimeEngramReconciler) reconcileDelete(ctx context.Context, engram *v1alpha1.Engram) error {
	log := logging.NewReconcileLogger(ctx, "realtime-engram").WithValues("engram", engram.Name)
	log.Info("Reconciling deletion for realtime Engram")

	// Delete owned Deployment
	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, deployment)
	if err == nil {
		if err := r.Delete(ctx, deployment); err != nil && !errors.IsNotFound(err) {
			log.Error(err, "Failed to delete owned Deployment")
			return err
		}
		log.Info("Deleted owned Deployment")
	} else if !errors.IsNotFound(err) {
		log.Error(err, "Failed to get owned Deployment for deletion")
		return err
	}

	// Delete owned StatefulSet
	sts := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, sts)
	if err == nil {
		if err := r.Delete(ctx, sts); err != nil && !errors.IsNotFound(err) {
			log.Error(err, "Failed to delete owned StatefulSet")
			return err
		}
		log.Info("Deleted owned StatefulSet")
	} else if !errors.IsNotFound(err) {
		log.Error(err, "Failed to get owned StatefulSet for deletion")
		return err
	}

	// Delete owned Service
	service := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, service)
	if err == nil {
		if err := r.Delete(ctx, service); err != nil && !errors.IsNotFound(err) {
			log.Error(err, "Failed to delete owned Service")
			return err
		}
		log.Info("Deleted owned Service")
	} else if !errors.IsNotFound(err) {
		log.Error(err, "Failed to get owned Service for deletion")
		return err
	}

	// All owned resources are deleted, remove the finalizer
	if controllerutil.ContainsFinalizer(engram, RealtimeEngramFinalizer) {
		controllerutil.RemoveFinalizer(engram, RealtimeEngramFinalizer)
		if err := r.Update(ctx, engram); err != nil {
			log.Error(err, "Failed to remove finalizer")
			return err
		}
		log.Info("Removed finalizer")
	}

	return nil
}

func (r *RealtimeEngramReconciler) reconcileDeployment(ctx context.Context, engram *v1alpha1.Engram, template *catalogv1alpha1.EngramTemplate, execCfg *config.ResolvedExecutionConfig) error {
	log := logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name)
	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, deployment)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating a new Deployment", "Deployment.Namespace", engram.Namespace, "Deployment.Name", engram.Name)
		newDep := r.deploymentForEngram(ctx, engram, template, execCfg)
		if err := r.Create(ctx, newDep); err != nil {
			log.Error(err, "Failed to create new Deployment")
			return err
		}
		return nil
	} else if err != nil {
		return err
	}

	// Update logic
	desiredDep := r.deploymentForEngram(ctx, engram, template, execCfg)
	if !reflect.DeepEqual(deployment.Spec.Template, desiredDep.Spec.Template) || *deployment.Spec.Replicas != *desiredDep.Spec.Replicas {
		log.Info("Deployment spec differs, updating deployment")
		// Retry on conflict using a fresh GET + merge to avoid clobbering cluster-managed fields
		key := types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}
		if err := r.Get(ctx, key, deployment); err != nil {
			return err
		}
		original := deployment.DeepCopy()
		deployment.Spec = desiredDep.Spec
		if err := r.Patch(ctx, deployment, client.MergeFrom(original)); err != nil {
			log.Error(err, "Failed to patch Deployment")
			return err
		}
	}

	return nil
}

func (r *RealtimeEngramReconciler) reconcileStatefulSet(ctx context.Context, engram *v1alpha1.Engram, execConfig *config.ResolvedExecutionConfig) error {
	log := logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name)
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, sts)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating a new StatefulSet", "StatefulSet.Namespace", engram.Namespace, "StatefulSet.Name", engram.Name)
		newSTS := r.statefulSetForEngram(ctx, engram, execConfig)
		if err := r.Create(ctx, newSTS); err != nil {
			log.Error(err, "Failed to create new StatefulSet")
			return err
		}
		return nil
	} else if err != nil {
		return err
	}

	desired := r.statefulSetForEngram(ctx, engram, execConfig)
	// Preserve fields set by the cluster
	desired.Spec.Replicas = sts.Spec.Replicas
	if !reflect.DeepEqual(sts.Spec.Template, desired.Spec.Template) || sts.Spec.ServiceName != desired.Spec.ServiceName {
		log.Info("StatefulSet spec differs, updating statefulset")
		key := types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}
		if err := r.Get(ctx, key, sts); err != nil {
			return err
		}
		original := sts.DeepCopy()
		sts.Spec = desired.Spec
		if err := r.Patch(ctx, sts, client.MergeFrom(original)); err != nil {
			log.Error(err, "Failed to patch StatefulSet")
			return err
		}
	}
	return nil
}

func (r *RealtimeEngramReconciler) statefulSetForEngram(ctx context.Context, engram *v1alpha1.Engram, execConfig *config.ResolvedExecutionConfig) *appsv1.StatefulSet {
	labels := map[string]string{"app": engram.Name, "bubustack.io/engram": engram.Name}

	terminationGracePeriod := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultTerminationGracePeriodSeconds
	engramConfig := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig

	podSpec := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:            execConfig.ServiceAccountName,
			TerminationGracePeriodSeconds: &terminationGracePeriod,
			SecurityContext:               execConfig.ToPodSecurityContext(),
			Containers: []corev1.Container{{
				Image:           execConfig.Image,
				Name:            "engram",
				ImagePullPolicy: execConfig.ImagePullPolicy,
				SecurityContext: execConfig.ToContainerSecurityContext(),
				Ports: []corev1.ContainerPort{{
					ContainerPort: int32(r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort),
					Name:          "grpc",
				}},
				Env: []corev1.EnvVar{
					{Name: "BUBU_MODE", Value: "statefulset"},
					{Name: "BUBU_EXECUTION_MODE", Value: "streaming"},
					{Name: "BUBU_GRPC_PORT", Value: fmt.Sprintf("%d", r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort)},
					{Name: "BUBU_POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
					{Name: "BUBU_POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
					{Name: "BUBU_MAX_INLINE_SIZE", Value: fmt.Sprintf("%d", engramConfig.DefaultMaxInlineSize)},
					{Name: "BUBU_STORAGE_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultStorageTimeoutSeconds)},
					{Name: "BUBU_GRPC_GRACEFUL_SHUTDOWN_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultGracefulShutdownTimeoutSeconds)},
					// Newly added SDK tuning parameters
					{Name: "BUBU_GRPC_MAX_RECV_BYTES", Value: fmt.Sprintf("%d", engramConfig.DefaultMaxRecvMsgBytes)},
					{Name: "BUBU_GRPC_MAX_SEND_BYTES", Value: fmt.Sprintf("%d", engramConfig.DefaultMaxSendMsgBytes)},
					{Name: "BUBU_GRPC_CLIENT_MAX_RECV_BYTES", Value: fmt.Sprintf("%d", engramConfig.DefaultMaxRecvMsgBytes)},
					{Name: "BUBU_GRPC_CLIENT_MAX_SEND_BYTES", Value: fmt.Sprintf("%d", engramConfig.DefaultMaxSendMsgBytes)},
					{Name: "BUBU_GRPC_DIAL_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultDialTimeoutSeconds)},
					{Name: "BUBU_GRPC_CHANNEL_BUFFER_SIZE", Value: fmt.Sprintf("%d", engramConfig.DefaultChannelBufferSize)},
					{Name: "BUBU_GRPC_RECONNECT_MAX_RETRIES", Value: fmt.Sprintf("%d", engramConfig.DefaultReconnectMaxRetries)},
					{Name: "BUBU_GRPC_RECONNECT_BASE_BACKOFF", Value: fmt.Sprintf("%dms", engramConfig.DefaultReconnectBaseBackoffMillis)},
					{Name: "BUBU_GRPC_RECONNECT_MAX_BACKOFF", Value: fmt.Sprintf("%ds", engramConfig.DefaultReconnectMaxBackoffSeconds)},
					{Name: "BUBU_GRPC_HANG_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultHangTimeoutSeconds)},
					{Name: "BUBU_GRPC_MESSAGE_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultMessageTimeoutSeconds)},
					{Name: "BUBU_GRPC_CHANNEL_SEND_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultMessageTimeoutSeconds)}, // Re-use message timeout
				},
				LivenessProbe:  execConfig.LivenessProbe,
				ReadinessProbe: execConfig.ReadinessProbe,
				StartupProbe:   execConfig.StartupProbe,
			}},
		},
	}

	// Pass engram With as config if present
	if engram.Spec.With != nil && len(engram.Spec.With.Raw) > 0 {
		podSpec.Spec.Containers[0].Env = append(podSpec.Spec.Containers[0].Env, corev1.EnvVar{
			Name:  "BUBU_CONFIG",
			Value: string(engram.Spec.With.Raw),
		})
	}

	// Handle storage env if configured (match deployment path)
	if storagePolicy := execConfig.Storage; storagePolicy != nil && storagePolicy.S3 != nil {
		s3Config := storagePolicy.S3
		podSpec.Spec.Containers[0].Env = append(podSpec.Spec.Containers[0].Env,
			corev1.EnvVar{Name: "BUBU_STORAGE_PROVIDER", Value: "s3"},
			corev1.EnvVar{Name: "BUBU_STORAGE_S3_BUCKET", Value: s3Config.Bucket},
		)
		if s3Config.Region != "" {
			podSpec.Spec.Containers[0].Env = append(podSpec.Spec.Containers[0].Env, corev1.EnvVar{Name: "BUBU_STORAGE_S3_REGION", Value: s3Config.Region})
		}
		if s3Config.Endpoint != "" {
			podSpec.Spec.Containers[0].Env = append(podSpec.Spec.Containers[0].Env, corev1.EnvVar{Name: "BUBU_STORAGE_S3_ENDPOINT", Value: s3Config.Endpoint})
		}
		if auth := &s3Config.Authentication; auth.SecretRef != nil {
			podSpec.Spec.Containers[0].EnvFrom = append(podSpec.Spec.Containers[0].EnvFrom, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: auth.SecretRef.Name}},
			})
		}
	}

	// Configure TLS via user-provided secret if annotated
	if sec := getTLSSecretName(&engram.ObjectMeta); sec != "" {
		if err := r.maybeConfigureTLSEnvAndMounts(ctx, engram.Namespace, sec, &podSpec, 0); err != nil {
			// Log and continue without TLS rather than returning a nil StatefulSet
			logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name).Error(err, "Failed to configure TLS mounts/envs for StatefulSet")
		}
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      engram.Name,
			Namespace: engram.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: engram.Name,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: podSpec,
		},
	}
	if err := ctrl.SetControllerReference(engram, sts, r.Scheme); err != nil {
		logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name).Error(err, "failed to set controller reference on StatefulSet")
	}
	return sts
}

func (r *RealtimeEngramReconciler) ensureEngramServiceAccount(ctx context.Context, engram *v1alpha1.Engram, saName string) error {
	sa := &corev1.ServiceAccount{}
	key := types.NamespacedName{Name: saName, Namespace: engram.Namespace}
	if err := r.Get(ctx, key, sa); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		sa = &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: engram.Namespace}}
		if err := ctrl.SetControllerReference(engram, sa, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, sa)
	}
	return nil
}

func (r *RealtimeEngramReconciler) reconcileService(ctx context.Context, engram *v1alpha1.Engram, execConfig *config.ResolvedExecutionConfig) error {
	log := logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name)
	service := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, service)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating a new Service", "Service.Namespace", engram.Namespace, "Service.Name", engram.Name)
		newSvc := r.serviceForEngram(ctx, engram, execConfig)
		if err := r.Create(ctx, newSvc); err != nil {
			log.Error(err, "Failed to create new Service")
			return err
		}
		return nil
	} else if err != nil {
		return err
	}

	// Update logic
	desiredSvc := r.serviceForEngram(ctx, engram, execConfig)
	// Preserve the ClusterIP
	desiredSvc.Spec.ClusterIP = service.Spec.ClusterIP
	// Preserve the ResourceVersion to avoid update conflicts
	desiredSvc.ResourceVersion = service.ResourceVersion

	if !reflect.DeepEqual(service.Spec, desiredSvc.Spec) {
		log.Info("Service spec differs, updating service")
		key := types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}
		if err := r.Get(ctx, key, service); err != nil {
			return err
		}
		original := service.DeepCopy()
		service.Spec = desiredSvc.Spec
		if err := r.Patch(ctx, service, client.MergeFrom(original)); err != nil {
			log.Error(err, "Failed to patch Service")
			return err
		}
	}

	return nil
}

func (r *RealtimeEngramReconciler) deploymentForEngram(ctx context.Context, engram *v1alpha1.Engram, template *catalogv1alpha1.EngramTemplate, execCfg *config.ResolvedExecutionConfig) *appsv1.Deployment {
	labels := map[string]string{"app": engram.Name, "bubustack.io/engram": engram.Name}
	replicas := int32(1)
	terminationGracePeriod := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultTerminationGracePeriodSeconds
	engramConfig := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig

	envVars := buildRealtimeBaseEnv(engramConfig)
	envVars = append(envVars, buildConfigEnvVars(engram)...)

	podSpec := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels},
		Spec: corev1.PodSpec{
			ServiceAccountName:            execCfg.ServiceAccountName,
			TerminationGracePeriodSeconds: &terminationGracePeriod,
			SecurityContext:               execCfg.ToPodSecurityContext(),
			Containers: []corev1.Container{{
				Image:           execCfg.Image,
				Name:            "engram",
				ImagePullPolicy: execCfg.ImagePullPolicy,
				SecurityContext: execCfg.ToContainerSecurityContext(),
				Ports: []corev1.ContainerPort{{
					ContainerPort: int32(engramConfig.DefaultGRPCPort),
					Name:          "grpc",
				}},
				Env:            envVars,
				LivenessProbe:  execCfg.LivenessProbe,
				ReadinessProbe: execCfg.ReadinessProbe,
				StartupProbe:   execCfg.StartupProbe,
			}},
		},
	}

	r.applySecretArtifactsToRealtimePod(template, execCfg, &podSpec)
	r.configureRealtimeStorage(ctx, engram, execCfg, &podSpec)
	r.configureRealtimeTLS(ctx, engram, &podSpec)

	return r.newRealtimeDeployment(ctx, engram, labels, replicas, podSpec)
}

func buildConfigEnvVars(engram *v1alpha1.Engram) []corev1.EnvVar {
	if engram.Spec.With == nil || len(engram.Spec.With.Raw) == 0 {
		return nil
	}
	return []corev1.EnvVar{
		{Name: "BUBU_CONFIG", Value: string(engram.Spec.With.Raw)},
	}
}

func buildRealtimeBaseEnv(cfg config.EngramControllerConfig) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "BUBU_MODE", Value: "deployment"},
		{Name: "BUBU_EXECUTION_MODE", Value: "streaming"},
		{Name: "BUBU_GRPC_PORT", Value: fmt.Sprintf("%d", cfg.DefaultGRPCPort)},
		{Name: "BUBU_MAX_RECURSION_DEPTH", Value: "64"},
		downwardEnvVar("BUBU_POD_NAME", "metadata.name"),
		downwardEnvVar("BUBU_POD_NAMESPACE", "metadata.namespace"),
		downwardEnvVar("POD_NAME", "metadata.name"),
		downwardEnvVar("POD_NAMESPACE", "metadata.namespace"),
		downwardEnvVar("SERVICE_ACCOUNT_NAME", "spec.serviceAccountName"),
		{Name: "BUBU_MAX_INLINE_SIZE", Value: fmt.Sprintf("%d", cfg.DefaultMaxInlineSize)},
		{Name: "BUBU_STORAGE_TIMEOUT", Value: fmt.Sprintf("%ds", cfg.DefaultStorageTimeoutSeconds)},
		{Name: "BUBU_GRPC_GRACEFUL_SHUTDOWN_TIMEOUT", Value: fmt.Sprintf("%ds", cfg.DefaultGracefulShutdownTimeoutSeconds)},
		{Name: "BUBU_GRPC_MAX_RECV_BYTES", Value: fmt.Sprintf("%d", cfg.DefaultMaxRecvMsgBytes)},
		{Name: "BUBU_GRPC_MAX_SEND_BYTES", Value: fmt.Sprintf("%d", cfg.DefaultMaxSendMsgBytes)},
		{Name: "BUBU_GRPC_CLIENT_MAX_RECV_BYTES", Value: fmt.Sprintf("%d", cfg.DefaultMaxRecvMsgBytes)},
		{Name: "BUBU_GRPC_CLIENT_MAX_SEND_BYTES", Value: fmt.Sprintf("%d", cfg.DefaultMaxSendMsgBytes)},
		{Name: "BUBU_GRPC_DIAL_TIMEOUT", Value: fmt.Sprintf("%ds", cfg.DefaultDialTimeoutSeconds)},
		{Name: "BUBU_GRPC_CHANNEL_BUFFER_SIZE", Value: fmt.Sprintf("%d", cfg.DefaultChannelBufferSize)},
		{Name: "BUBU_GRPC_RECONNECT_MAX_RETRIES", Value: fmt.Sprintf("%d", cfg.DefaultReconnectMaxRetries)},
		{Name: "BUBU_GRPC_RECONNECT_BASE_BACKOFF", Value: fmt.Sprintf("%dms", cfg.DefaultReconnectBaseBackoffMillis)},
		{Name: "BUBU_GRPC_RECONNECT_MAX_BACKOFF", Value: fmt.Sprintf("%ds", cfg.DefaultReconnectMaxBackoffSeconds)},
		{Name: "BUBU_GRPC_HANG_TIMEOUT", Value: fmt.Sprintf("%ds", cfg.DefaultHangTimeoutSeconds)},
		{Name: "BUBU_GRPC_MESSAGE_TIMEOUT", Value: fmt.Sprintf("%ds", cfg.DefaultMessageTimeoutSeconds)},
		{Name: "BUBU_GRPC_CHANNEL_SEND_TIMEOUT", Value: fmt.Sprintf("%ds", cfg.DefaultMessageTimeoutSeconds)},
	}
}

func downwardEnvVar(name, fieldPath string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath},
		},
	}
}

func (r *RealtimeEngramReconciler) applySecretArtifactsToRealtimePod(template *catalogv1alpha1.EngramTemplate, execCfg *config.ResolvedExecutionConfig, podSpec *corev1.PodTemplateSpec) {
	if podSpec == nil || len(podSpec.Spec.Containers) == 0 || execCfg.Secrets == nil || template == nil || template.Spec.SecretSchema == nil {
		return
	}
	artifacts := secretutil.BuildArtifacts(template.Spec.SecretSchema, execCfg.Secrets)
	podSpec.Spec.Volumes = append(podSpec.Spec.Volumes, artifacts.Volumes...)

	container := &podSpec.Spec.Containers[0]
	container.Env = append(container.Env, artifacts.EnvVars...)
	container.EnvFrom = append(container.EnvFrom, artifacts.EnvFrom...)
	container.VolumeMounts = append(container.VolumeMounts, artifacts.VolumeMounts...)
}

func (r *RealtimeEngramReconciler) configureRealtimeStorage(ctx context.Context, engram *v1alpha1.Engram, execCfg *config.ResolvedExecutionConfig, podSpec *corev1.PodTemplateSpec) {
	if podSpec == nil || len(podSpec.Spec.Containers) == 0 || execCfg.Storage == nil || execCfg.Storage.S3 == nil {
		return
	}

	s3Config := execCfg.Storage.S3
	logger := logging.NewControllerLogger(ctx, "realtime_engram").WithValues("engram", engram.Name)
	logger.Info("Configuring pod for S3 object storage access", "bucket", s3Config.Bucket)

	container := &podSpec.Spec.Containers[0]
	container.Env = append(container.Env,
		corev1.EnvVar{Name: "BUBU_STORAGE_PROVIDER", Value: "s3"},
		corev1.EnvVar{Name: "BUBU_STORAGE_S3_BUCKET", Value: s3Config.Bucket},
	)
	if s3Config.Region != "" {
		container.Env = append(container.Env, corev1.EnvVar{Name: "BUBU_STORAGE_S3_REGION", Value: s3Config.Region})
	}
	if s3Config.Endpoint != "" {
		container.Env = append(container.Env, corev1.EnvVar{Name: "BUBU_STORAGE_S3_ENDPOINT", Value: s3Config.Endpoint})
	}

	if s3Config.Authentication.SecretRef != nil {
		secretName := s3Config.Authentication.SecretRef.Name
		logger.Info("Using S3 secret reference for authentication", "secretName", secretName)
		container.EnvFrom = append(container.EnvFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
			},
		})
	}
}

func (r *RealtimeEngramReconciler) configureRealtimeTLS(ctx context.Context, engram *v1alpha1.Engram, podSpec *corev1.PodTemplateSpec) {
	if podSpec == nil {
		return
	}
	if sec := getTLSSecretName(&engram.ObjectMeta); sec != "" {
		if err := r.maybeConfigureTLSEnvAndMounts(ctx, engram.Namespace, sec, podSpec, 0); err != nil {
			logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name).Error(err, "Failed to configure TLS mounts/envs for Deployment")
		}
	}
}

func (r *RealtimeEngramReconciler) newRealtimeDeployment(ctx context.Context, engram *v1alpha1.Engram, labels map[string]string, replicas int32, podSpec corev1.PodTemplateSpec) *appsv1.Deployment {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      engram.Name,
			Namespace: engram.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: podSpec,
		},
	}
	if err := ctrl.SetControllerReference(engram, dep, r.Scheme); err != nil {
		logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name).Error(err, "failed to set controller reference on Deployment")
	}
	return dep
}

func (r *RealtimeEngramReconciler) serviceForEngram(ctx context.Context, engram *v1alpha1.Engram, _ *config.ResolvedExecutionConfig) *corev1.Service {
	labels := map[string]string{"app": engram.Name, "bubustack.io/engram": engram.Name}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      engram.Name,
			Namespace: engram.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Protocol:   corev1.ProtocolTCP,
				Port:       int32(r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort),
				TargetPort: intstr.FromString("grpc"),
			}},
			Type: corev1.ServiceTypeClusterIP,
		},
	}
	// Use headless Service for StatefulSet mode to provide stable network IDs
	if engram.Spec.Mode == enums.WorkloadModeStatefulSet {
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		// Optional: make endpoints available before readiness for DNS
		svc.Spec.PublishNotReadyAddresses = true
	}
	if err := ctrl.SetControllerReference(engram, svc, r.Scheme); err != nil {
		logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name).Error(err, "failed to set controller reference on Service")
	}
	return svc
}

// SetupWithManager sets up the controller with the Manager.
func (r *RealtimeEngramReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("realtime-engram").
		For(&v1alpha1.Engram{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Watches(
			&catalogv1alpha1.EngramTemplate{},
			handler.EnqueueRequestsFromMapFunc(r.mapEngramTemplateToEngrams),
		).
		WithOptions(opts).
		Complete(r)
}

// mapEngramTemplateToEngrams finds all Engrams that reference a given EngramTemplate.
func (r *RealtimeEngramReconciler) mapEngramTemplateToEngrams(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logging.NewReconcileLogger(ctx, "engram-mapper").WithValues("engram-template", obj.GetName())
	var engrams v1alpha1.EngramList
	if err := r.List(ctx, &engrams, client.MatchingFields{"spec.templateRef.name": obj.GetName()}); err != nil {
		log.Error(err, "failed to list engrams for engramtemplate")
		return nil
	}

	requests := make([]reconcile.Request, len(engrams.Items))
	for i, item := range engrams.Items {
		requests[i] = reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      item.GetName(),
				Namespace: item.GetNamespace(),
			},
		}
	}
	return requests
}
