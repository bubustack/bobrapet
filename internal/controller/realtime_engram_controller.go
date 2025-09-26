package controller

import (
	"context"
	"reflect"

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
	"github.com/bubustack/bobrapet/pkg/patch"
)

const (
	// RealtimeEngramFinalizer is the finalizer for Engrams in deployment/statefulset mode.
	RealtimeEngramFinalizer = "engram.bubu.sh/realtime-finalizer"
)

// RealtimeEngramReconciler reconciles an Engram object for real-time workloads
type RealtimeEngramReconciler struct {
	config.ControllerDependencies
}

//+kubebuilder:rbac:groups=bubu.sh,resources=engrams,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=bubu.sh,resources=engrams/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete

func (r *RealtimeEngramReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "realtime-engram").WithValues("engram", req.NamespacedName)

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
		return r.reconcileDelete(ctx, &engram)
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
				cm.SetCondition(&e.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, "TemplateNotFound", err.Error())
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
		cm.SetCondition(&e.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, "TemplateFound", "Engram template was found and is available.")
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

	// At this point, we reconcile the necessary resources for service-like engrams.
	switch mode {
	case enums.WorkloadModeDeployment:
		if err := r.reconcileDeployment(ctx, &engram, resolvedConfig); err != nil {
			return ctrl.Result{}, err
		}
	case enums.WorkloadModeStatefulSet:
		// TODO: Implement StatefulSet reconciliation
		log.Info("StatefulSet reconciliation is not yet implemented")
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

func (r *RealtimeEngramReconciler) reconcileDelete(ctx context.Context, engram *v1alpha1.Engram) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "realtime-engram").WithValues("engram", engram.Name)
	log.Info("Reconciling deletion for realtime Engram")

	// Delete owned Deployment
	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, deployment)
	if err == nil {
		if err := r.Delete(ctx, deployment); err != nil && !errors.IsNotFound(err) {
			log.Error(err, "Failed to delete owned Deployment")
			return ctrl.Result{}, err
		}
		log.Info("Deleted owned Deployment")
	} else if !errors.IsNotFound(err) {
		log.Error(err, "Failed to get owned Deployment for deletion")
		return ctrl.Result{}, err
	}

	// Delete owned Service
	service := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, service)
	if err == nil {
		if err := r.Delete(ctx, service); err != nil && !errors.IsNotFound(err) {
			log.Error(err, "Failed to delete owned Service")
			return ctrl.Result{}, err
		}
		log.Info("Deleted owned Service")
	} else if !errors.IsNotFound(err) {
		log.Error(err, "Failed to get owned Service for deletion")
		return ctrl.Result{}, err
	}

	// All owned resources are deleted, remove the finalizer
	if controllerutil.ContainsFinalizer(engram, RealtimeEngramFinalizer) {
		controllerutil.RemoveFinalizer(engram, RealtimeEngramFinalizer)
		if err := r.Update(ctx, engram); err != nil {
			log.Error(err, "Failed to remove finalizer")
			return ctrl.Result{}, err
		}
		log.Info("Removed finalizer")
	}

	return ctrl.Result{}, nil
}

func (r *RealtimeEngramReconciler) reconcileDeployment(ctx context.Context, engram *v1alpha1.Engram, execConfig *config.ResolvedExecutionConfig) error {
	log := logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name)
	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, deployment)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating a new Deployment", "Deployment.Namespace", engram.Namespace, "Deployment.Name", engram.Name)
		newDep := r.deploymentForEngram(engram, execConfig)
		if err := r.Create(ctx, newDep); err != nil {
			log.Error(err, "Failed to create new Deployment")
			return err
		}
		return nil
	} else if err != nil {
		return err
	}

	// Update logic
	desiredDep := r.deploymentForEngram(engram, execConfig)
	if !reflect.DeepEqual(deployment.Spec.Template, desiredDep.Spec.Template) || *deployment.Spec.Replicas != *desiredDep.Spec.Replicas {
		log.Info("Deployment spec differs, updating deployment")
		deployment.Spec = desiredDep.Spec
		if err := r.Update(ctx, deployment); err != nil {
			log.Error(err, "Failed to update Deployment")
			return err
		}
	}

	return nil
}

func (r *RealtimeEngramReconciler) reconcileService(ctx context.Context, engram *v1alpha1.Engram, execConfig *config.ResolvedExecutionConfig) error {
	log := logging.NewReconcileLogger(ctx, "realtime_engram").WithValues("engram", engram.Name)
	service := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: engram.Name, Namespace: engram.Namespace}, service)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating a new Service", "Service.Namespace", engram.Namespace, "Service.Name", engram.Name)
		newSvc := r.serviceForEngram(engram, execConfig)
		if err := r.Create(ctx, newSvc); err != nil {
			log.Error(err, "Failed to create new Service")
			return err
		}
		return nil
	} else if err != nil {
		return err
	}

	// Update logic
	desiredSvc := r.serviceForEngram(engram, execConfig)
	// Preserve the ClusterIP
	desiredSvc.Spec.ClusterIP = service.Spec.ClusterIP
	// Preserve the ResourceVersion to avoid update conflicts
	desiredSvc.ResourceVersion = service.ResourceVersion

	if !reflect.DeepEqual(service.Spec, desiredSvc.Spec) {
		log.Info("Service spec differs, updating service")
		service.Spec = desiredSvc.Spec
		if err := r.Update(ctx, service); err != nil {
			log.Error(err, "Failed to update Service")
			return err
		}
	}

	return nil
}

func (r *RealtimeEngramReconciler) deploymentForEngram(engram *v1alpha1.Engram, config *config.ResolvedExecutionConfig) *appsv1.Deployment {
	labels := map[string]string{"app": engram.Name, "bubu.sh/engram": engram.Name}
	replicas := int32(1)
	podSpec := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: config.ServiceAccountName,
			Containers: []corev1.Container{{
				Image:           config.Image,
				Name:            "engram",
				ImagePullPolicy: config.ImagePullPolicy,
				Ports: []corev1.ContainerPort{{
					ContainerPort: int32(r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort),
					Name:          "grpc",
				}},
				Env: []corev1.EnvVar{
					{Name: "BUBU_MODE", Value: "deployment"},
					{Name: "BUBU_POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
					{Name: "BUBU_POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
				},
			}},
		},
	}

	// Handle object storage configuration
	if storagePolicy := config.Storage; storagePolicy != nil && storagePolicy.S3 != nil {
		s3Config := storagePolicy.S3
		log := logging.NewReconcileLogger(context.TODO(), "realtime_engram").WithValues("engram", engram.Name)
		log.Info("Configuring pod for S3 object storage access", "bucket", s3Config.Bucket)

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
			log.Info("Using S3 secret reference for authentication", "secretName", secretName)
			podSpec.Spec.Containers[0].EnvFrom = append(podSpec.Spec.Containers[0].EnvFrom, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				},
			})
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
	ctrl.SetControllerReference(engram, dep, r.Scheme)
	return dep
}

func (r *RealtimeEngramReconciler) serviceForEngram(engram *v1alpha1.Engram, config *config.ResolvedExecutionConfig) *corev1.Service {
	labels := map[string]string{"app": engram.Name, "bubu.sh/engram": engram.Name}
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
	ctrl.SetControllerReference(engram, svc, r.Scheme)
	return svc
}

// SetupWithManager sets up the controller with the Manager.
func (r *RealtimeEngramReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Engram{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.Deployment{}).
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
	if err := r.List(ctx, &engrams, client.MatchingFields{"spec.templateRef": obj.GetName()}); err != nil {
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
