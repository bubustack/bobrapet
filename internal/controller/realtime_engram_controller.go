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

	// Bound reconcile duration
	timeout := r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var engram v1alpha1.Engram
	if err := r.Get(ctx, req.NamespacedName, &engram); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// This reconciler only handles non-job modes.
	// If the engram is not in a realtime mode, we should ensure our finalizer is removed and then stop.
	if engram.Spec.Mode != enums.WorkloadModeDeployment && engram.Spec.Mode != enums.WorkloadModeStatefulSet {
		if controllerutil.ContainsFinalizer(&engram, RealtimeEngramFinalizer) {
			controllerutil.RemoveFinalizer(&engram, RealtimeEngramFinalizer)
			if err := r.Update(ctx, &engram); err != nil {
				log.Error(err, "Failed to remove realtime finalizer from job-mode Engram")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Handle deletion
	if !engram.DeletionTimestamp.IsZero() {
		if err := r.reconcileDelete(ctx, &engram); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if it doesn't exist
	if !controllerutil.ContainsFinalizer(&engram, RealtimeEngramFinalizer) {
		controllerutil.AddFinalizer(&engram, RealtimeEngramFinalizer)
		if err := r.Update(ctx, &engram); err != nil {
			log.Error(err, "Failed to add realtime finalizer")
			return ctrl.Result{}, err
		}
	}

	// Get the referenced EngramTemplate
	template, err := r.getEngramTemplate(ctx, &engram)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("EngramTemplate not found for Engram, setting status to Blocked")
			updateErr := patch.RetryableStatusPatch(ctx, r.Client, &engram, func(obj client.Object) {
				e := obj.(*v1alpha1.Engram)
				cm := conditions.NewConditionManager(e.Generation)
				cm.SetCondition(&e.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonTemplateNotFound, err.Error())
				e.Status.Phase = enums.PhaseBlocked
			})
			// We return the error from the patch operation, but if it's nil, we stop reconciling.
			return ctrl.Result{}, updateErr
		}
		// For other errors, log as an error and requeue.
		log.Error(err, "Failed to get EngramTemplate")
		return ctrl.Result{}, err
	}

	// If we are here, the template was found. Mark the engram as ready.
	if err := patch.RetryableStatusPatch(ctx, r.Client, &engram, func(obj client.Object) {
		e := obj.(*v1alpha1.Engram)
		cm := conditions.NewConditionManager(e.Generation)
		cm.SetCondition(&e.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonTemplateResolved, "Engram template was found and is available.")
		// We only set phase for terminal/blocked states at this level.
		// Running state is managed by the workload reconciliation.
		if e.Status.Phase == enums.PhaseBlocked {
			e.Status.Phase = enums.PhasePending
		}
	}); err != nil {
		log.Error(err, "Failed to update Engram status to Ready")
		return ctrl.Result{}, err
	}

	// Determine execution mode - Job, Deployment, or StatefulSet
	// This logic decides which type of workload to create for the Engram
	mode := engram.Spec.Mode
	if mode == "" {
		// Default to job if not specified
		mode = enums.WorkloadModeJob
	}

	// This reconciler only handles non-job modes.
	if mode == enums.WorkloadModeJob {
		// This case is handled by the check at the beginning of the function.
		return ctrl.Result{}, nil
	}

	// Resolve the full configuration
	resolvedConfig, err := r.ConfigResolver.ResolveExecutionConfig(ctx, nil, nil, &engram, template)
	if err != nil {
		log.Error(err, "Failed to resolve execution config")
		// TODO: Update status with an error condition
		return ctrl.Result{}, err
	}

	// Ensure a non-default ServiceAccount for realtime engrams if none was specified
	if resolvedConfig.ServiceAccountName == "" || resolvedConfig.ServiceAccountName == "default" {
		saName := fmt.Sprintf("%s-engram-runner", engram.Name)
		if err := r.ensureEngramServiceAccount(ctx, &engram, saName); err != nil {
			log.Error(err, "Failed to ensure ServiceAccount for realtime engram")
			return ctrl.Result{}, err
		}
		resolvedConfig.ServiceAccountName = saName
	}

	// At this point, we reconcile the necessary resources for service-like engrams.
	switch mode {
	case enums.WorkloadModeDeployment:
		if err := r.reconcileDeployment(ctx, &engram, resolvedConfig); err != nil {
			return ctrl.Result{}, err
		}
	case enums.WorkloadModeStatefulSet:
		if err := r.reconcileStatefulSet(ctx, &engram, resolvedConfig); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Reconcile the associated Service
	if err := r.reconcileService(ctx, &engram, resolvedConfig); err != nil {
		return ctrl.Result{}, err
	}

	// For now, the reconciler's main job is to keep the status in sync with the managed resources.
	// A more advanced implementation would handle updates, scaling, etc.
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

func (r *RealtimeEngramReconciler) reconcileDeployment(ctx context.Context, engram *v1alpha1.Engram, execCfg *config.ResolvedExecutionConfig) error {
	log := logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name)
	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, deployment)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating a new Deployment", "Deployment.Namespace", engram.Namespace, "Deployment.Name", engram.Name)
		newDep := r.deploymentForEngram(ctx, engram, execCfg)
		if err := r.Create(ctx, newDep); err != nil {
			log.Error(err, "Failed to create new Deployment")
			return err
		}
		return nil
	} else if err != nil {
		return err
	}

	// Update logic
	desiredDep := r.deploymentForEngram(ctx, engram, execCfg)
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

func (r *RealtimeEngramReconciler) deploymentForEngram(ctx context.Context, engram *v1alpha1.Engram, execCfg *config.ResolvedExecutionConfig) *appsv1.Deployment {
	labels := map[string]string{"app": engram.Name, "bubustack.io/engram": engram.Name}
	replicas := int32(1)

	// Serialize the engram's 'with' configuration to JSON for the SDK
	var configEnvVars []corev1.EnvVar
	if engram.Spec.With != nil && len(engram.Spec.With.Raw) > 0 {
		configEnvVars = append(configEnvVars, corev1.EnvVar{
			Name:  "BUBU_CONFIG",
			Value: string(engram.Spec.With.Raw),
		})
	}

	// Set terminationGracePeriodSeconds to coordinate with SDK graceful shutdown timeout
	terminationGracePeriod := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultTerminationGracePeriodSeconds
	engramConfig := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig

	podSpec := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
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
					ContainerPort: int32(r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort),
					Name:          "grpc",
				}},
				Env: append([]corev1.EnvVar{
					{Name: "BUBU_MODE", Value: "deployment"},
					// Ensure SDK auto-detects streaming execution mode
					{Name: "BUBU_EXECUTION_MODE", Value: "streaming"},
					// Expose port for SDK gRPC server; aligns with Service port
					{Name: "BUBU_GRPC_PORT", Value: fmt.Sprintf("%d", r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort)},
					{Name: "BUBU_POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
					{Name: "BUBU_POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
					// Set inline size limit for storage offloading (consistency with batch StepRun jobs)
					{Name: "BUBU_MAX_INLINE_SIZE", Value: fmt.Sprintf("%d", engramConfig.DefaultMaxInlineSize)},
					// Set storage operation timeout for large file uploads/downloads
					{Name: "BUBU_STORAGE_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultStorageTimeoutSeconds)},
					// Set graceful shutdown timeout to coordinate with terminationGracePeriodSeconds
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
				}, configEnvVars...),
				LivenessProbe:  execCfg.LivenessProbe,
				ReadinessProbe: execCfg.ReadinessProbe,
				StartupProbe:   execCfg.StartupProbe,
			}},
		},
	}

	// Handle object storage configuration
	if storagePolicy := execCfg.Storage; storagePolicy != nil && storagePolicy.S3 != nil {
		s3Config := storagePolicy.S3
		// log with reconcile context
		logging.NewControllerLogger(ctx, "realtime_engram").WithValues("engram", engram.Name).Info("Configuring pod for S3 object storage access", "bucket", s3Config.Bucket)

		// Add environment variables for the SDK
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

		// Handle secret-based authentication
		if auth := &s3Config.Authentication; auth.SecretRef != nil {
			secretName := auth.SecretRef.Name
			logging.NewControllerLogger(ctx, "realtime_engram").WithValues("engram", engram.Name).Info("Using S3 secret reference for authentication", "secretName", secretName)
			podSpec.Spec.Containers[0].EnvFrom = append(podSpec.Spec.Containers[0].EnvFrom, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				},
			})
		}
	}

	// Configure TLS via user-provided secret if annotated
	if sec := getTLSSecretName(&engram.ObjectMeta); sec != "" {
		if err := r.maybeConfigureTLSEnvAndMounts(ctx, engram.Namespace, sec, &podSpec, 0); err != nil {
			logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name).Error(err, "Failed to configure TLS mounts/envs for Deployment")
		}
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      engram.Name,
			Namespace: engram.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
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
