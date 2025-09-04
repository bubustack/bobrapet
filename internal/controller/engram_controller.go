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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"

	"github.com/bubustack/bobrapet/internal/logging"
	"github.com/bubustack/bobrapet/internal/metrics"
	"github.com/bubustack/bobrapet/internal/storage"
)

// EngramReconciler reconciles a Engram object
type EngramReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	conditionManager *ConditionManager
	finalizerManager *FinalizerManager
	templateResolver *TemplateResolver
	StorageManager   storage.StorageManager
}

//+kubebuilder:rbac:groups=bubu.sh,resources=engrams,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=bubu.sh,resources=engrams/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=bubu.sh,resources=engrams/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile validates Engram configuration and manages lifecycle
func (r *EngramReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Initialize structured logging and metrics
	rl := logging.NewReconcileLogger(ctx, "engram")
	startTime := time.Now()

	defer func() {
		duration := time.Since(startTime)
		metrics.RecordControllerReconcile("engram", duration, nil)
	}()

	var engram bubushv1alpha1.Engram
	if err := r.Get(ctx, req.NamespacedName, &engram); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Add Engram context to logger
	engramLogger := rl.WithEngram(&engram)
	rl.ReconcileStart("Processing Engram")

	// Initialize managers if needed
	if r.conditionManager == nil {
		r.conditionManager = NewConditionManager(engram.Generation)
	}
	if r.finalizerManager == nil {
		r.finalizerManager = NewFinalizerManager(r.Client)
	}
	if r.templateResolver == nil {
		r.templateResolver = NewTemplateResolver(r.Client)
	}
	// ReconcilePattern removed - using direct reconcile logic

	// Basic validation - engine configuration is required
	if engram.Spec.Engine == nil {
		conditions := &engram.Status.Conditions
		r.conditionManager.SetReadyCondition(conditions, false, ReasonValidationFailed, "engine configuration is required")
		r.conditionManager.SetDegradedCondition(conditions, true, ReasonValidationFailed, "Engine configuration missing")
		rl.ReconcileError(fmt.Errorf("engine configuration missing"), "Engine configuration is required")
		if err := r.Status().Update(ctx, &engram); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	// Validate templateRef is required
	if engram.Spec.Engine.TemplateRef == "" {
		conditions := &engram.Status.Conditions
		r.conditionManager.SetReadyCondition(conditions, false, ReasonValidationFailed, "engine.templateRef is required")
		r.conditionManager.SetDegradedCondition(conditions, true, ReasonValidationFailed, "Engine templateRef missing")
		rl.ReconcileError(fmt.Errorf("templateRef missing"), "Engine templateRef is required")
		if err := r.Status().Update(ctx, &engram); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	// Use TemplateResolver to validate and resolve configuration
	resolvedEngram, err := r.templateResolver.ResolveEngram(ctx, &engram)
	if err != nil {
		conditions := &engram.Status.Conditions
		r.conditionManager.SetReadyCondition(conditions, false, ReasonTemplateResolutionFailed, fmt.Sprintf("Template resolution failed: %v", err))
		r.conditionManager.SetCondition(conditions, ConditionTemplateResolved, metav1.ConditionFalse, ReasonTemplateResolutionFailed, err.Error())
		rl.ReconcileError(err, "Template resolution failed")
		if updateErr := r.Status().Update(ctx, &engram); updateErr != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, updateErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Update template resolved condition
	conditions := &engram.Status.Conditions
	r.conditionManager.SetCondition(conditions, ConditionTemplateResolved, metav1.ConditionTrue, ReasonTemplateResolved, fmt.Sprintf("Template resolved from %s", resolvedEngram.Source))

	// Validate engine mode (after template resolution)
	if engram.Spec.Engine.Mode == "" {
		engram.Spec.Engine.Mode = "job" // Default
	}

	validModes := []string{"job", "deployment", "statefulset"}
	validMode := false
	for _, vm := range validModes {
		if engram.Spec.Engine.Mode == vm {
			validMode = true
			break
		}
	}
	if !validMode {
		r.conditionManager.SetReadyCondition(conditions, false, ReasonValidationFailed, fmt.Sprintf("invalid engine mode '%s'", engram.Spec.Engine.Mode))
		rl.ReconcileError(fmt.Errorf("invalid engine mode: %s", engram.Spec.Engine.Mode), "Invalid engine mode")
		if err := r.Status().Update(ctx, &engram); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	engramLogger.Info("Configuring Engram", "mode", engram.Spec.Engine.Mode, "template", engram.Spec.Engine.TemplateRef)

	// Handle execution mode setup
	switch engram.Spec.Engine.Mode {
	case "job":
		// Job mode: Nothing to pre-create, Jobs are created per StepRun
		engramLogger.Info("Engram configured for job mode", "templateRef", engram.Spec.Engine.TemplateRef)
	case "deployment":
		// Deployment mode: Create/manage Deployment and Service
		if err := r.handleDeploymentMode(ctx, &engram); err != nil {
			conditions := &engram.Status.Conditions
			r.conditionManager.SetReadyCondition(conditions, false, ReasonExecutionFailed, fmt.Sprintf("failed to setup deployment mode: %v", err))
			rl.ReconcileError(err, "Failed to setup deployment mode")
			if updateErr := r.Status().Update(ctx, &engram); updateErr != nil {
				engramLogger.Error(updateErr, "failed to update status after deployment error")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}
	case "statefulset":
		// StatefulSet mode: Create/manage StatefulSet and Service
		if err := r.handleStatefulSetMode(ctx, &engram); err != nil {
			conditions := &engram.Status.Conditions
			r.conditionManager.SetReadyCondition(conditions, false, ReasonExecutionFailed, fmt.Sprintf("failed to setup statefulset mode: %v", err))
			rl.ReconcileError(err, "Failed to setup statefulset mode")
			if updateErr := r.Status().Update(ctx, &engram); updateErr != nil {
				engramLogger.Error(updateErr, "failed to update status after statefulset error")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}
	}

	// Update status to ready using ConditionManager
	conditions = &engram.Status.Conditions
	r.conditionManager.SetReadyCondition(conditions, true, ReasonValidationPassed, "Engram is valid and ready for execution")
	r.conditionManager.SetCondition(conditions, ConditionValidated, metav1.ConditionTrue, ReasonValidationPassed, "Engram configuration validated")
	if err := r.Status().Update(ctx, &engram); err != nil {
		rl.ReconcileError(err, "Failed to update engram status")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	rl.ReconcileSuccess("Engram successfully configured", "mode", engram.Spec.Engine.Mode)
	return ctrl.Result{}, nil
}

// handleDeploymentMode creates and manages a Deployment and Service for deployment mode engrams
func (r *EngramReconciler) handleDeploymentMode(ctx context.Context, engram *bubushv1alpha1.Engram) error {
	engramLogger := logging.NewControllerLogger(ctx, "engram").WithEngram(engram)

	image := r.resolveEngramImage(ctx, engram)
	replicas := int32(1)
	if engram.Spec.Engine.Workload != nil && engram.Spec.Engine.Workload.Replicas != nil {
		replicas = *engram.Spec.Engine.Workload.Replicas
	}

	labels := map[string]string{
		"app.kubernetes.io/name":      "bobrapet",
		"app.kubernetes.io/component": "engram",
		"bubu.sh/engram":              engram.Name,
	}

	if err := r.ensureService(ctx, engram, labels, 8080); err != nil {
		return fmt.Errorf("ensure service: %w", err)
	}

	// Get shared storage for namespace
	volumeMounts, volumes, err := r.getSharedStorageForNamespace(ctx, engram.Namespace)
	if err != nil {
		return fmt.Errorf("failed to get shared storage: %w", err)
	}

	depName := fmt.Sprintf("engram-%s", engram.Name)
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Name: depName, Namespace: engram.Namespace}, &dep); err != nil {
		dep = appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      depName,
				Namespace: engram.Namespace,
				Labels:    labels,
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:         "engram",
							Image:        image,
							Ports:        []corev1.ContainerPort{{ContainerPort: 8080}},
							VolumeMounts: volumeMounts,
							SecurityContext: &corev1.SecurityContext{
								RunAsNonRoot:             &[]bool{true}[0],
								AllowPrivilegeEscalation: &[]bool{false}[0],
								ReadOnlyRootFilesystem:   &[]bool{true}[0],
								Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
							},
							Resources: r.buildResourceRequirements(engram.Spec.Resources),
						}},
						Volumes: volumes,
					},
				},
			},
		}
		if err := ctrl.SetControllerReference(engram, &dep, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, &dep); err != nil {
			return err
		}
		engramLogger.Info("created Deployment", "name", depName)
		return nil
	}

	updated := false
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != replicas {
		dep.Spec.Replicas = &replicas
		updated = true
	}
	if len(dep.Spec.Template.Spec.Containers) == 0 || dep.Spec.Template.Spec.Containers[0].Image != image {
		if len(dep.Spec.Template.Spec.Containers) == 0 {
			dep.Spec.Template.Spec.Containers = []corev1.Container{{Name: "engram"}}
		}
		c := &dep.Spec.Template.Spec.Containers[0]
		c.Image = image
		c.VolumeMounts = volumeMounts
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{
				RunAsNonRoot:             &[]bool{true}[0],
				AllowPrivilegeEscalation: &[]bool{false}[0],
				ReadOnlyRootFilesystem:   &[]bool{true}[0],
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			}
		}
		c.Resources = r.buildResourceRequirements(engram.Spec.Resources)
		updated = true
	}
	// Update volumes
	if !volumesEqual(dep.Spec.Template.Spec.Volumes, volumes) {
		dep.Spec.Template.Spec.Volumes = volumes
		updated = true
	}
	if updated {
		if err := r.Update(ctx, &dep); err != nil {
			return err
		}
		engramLogger.Info("updated Deployment", "name", depName)
	}
	return nil
}

// handleStatefulSetMode creates and manages a StatefulSet and Service for statefulset mode engrams
func (r *EngramReconciler) handleStatefulSetMode(ctx context.Context, engram *bubushv1alpha1.Engram) error {
	engramLogger := logging.NewControllerLogger(ctx, "engram").WithEngram(engram)

	image := r.resolveEngramImage(ctx, engram)
	replicas := int32(1)
	if engram.Spec.Engine.Workload != nil && engram.Spec.Engine.Workload.Replicas != nil {
		replicas = *engram.Spec.Engine.Workload.Replicas
	}

	labels := map[string]string{
		"app.kubernetes.io/name":      "bobrapet",
		"app.kubernetes.io/component": "engram",
		"bubu.sh/engram":              engram.Name,
	}

	if err := r.ensureService(ctx, engram, labels, 8080); err != nil {
		return fmt.Errorf("ensure service: %w", err)
	}

	// Get shared storage for namespace
	volumeMounts, volumes, err := r.getSharedStorageForNamespace(ctx, engram.Namespace)
	if err != nil {
		return fmt.Errorf("failed to get shared storage: %w", err)
	}

	ssName := fmt.Sprintf("engram-%s", engram.Name)
	var ss appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Name: ssName, Namespace: engram.Namespace}, &ss); err != nil {
		serviceName := fmt.Sprintf("engram-%s", engram.Name)
		ss = appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ssName,
				Namespace: engram.Namespace,
				Labels:    labels,
			},
			Spec: appsv1.StatefulSetSpec{
				ServiceName: serviceName,
				Replicas:    &replicas,
				Selector:    &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:         "engram",
							Image:        image,
							Ports:        []corev1.ContainerPort{{ContainerPort: 8080}},
							VolumeMounts: volumeMounts,
							SecurityContext: &corev1.SecurityContext{
								RunAsNonRoot:             &[]bool{true}[0],
								AllowPrivilegeEscalation: &[]bool{false}[0],
								ReadOnlyRootFilesystem:   &[]bool{true}[0],
								Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
							},
							Resources: r.buildResourceRequirements(engram.Spec.Resources),
						}},
						Volumes: volumes,
					},
				},
			},
		}
		if err := ctrl.SetControllerReference(engram, &ss, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, &ss); err != nil {
			return err
		}
		engramLogger.Info("created StatefulSet", "name", ssName)
		return nil
	}

	updated := false
	if ss.Spec.Replicas == nil || *ss.Spec.Replicas != replicas {
		ss.Spec.Replicas = &replicas
		updated = true
	}
	if len(ss.Spec.Template.Spec.Containers) == 0 || ss.Spec.Template.Spec.Containers[0].Image != image {
		if len(ss.Spec.Template.Spec.Containers) == 0 {
			ss.Spec.Template.Spec.Containers = []corev1.Container{{Name: "engram"}}
		}
		c := &ss.Spec.Template.Spec.Containers[0]
		c.Image = image
		c.VolumeMounts = volumeMounts
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{
				RunAsNonRoot:             &[]bool{true}[0],
				AllowPrivilegeEscalation: &[]bool{false}[0],
				ReadOnlyRootFilesystem:   &[]bool{true}[0],
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			}
		}
		c.Resources = r.buildResourceRequirements(engram.Spec.Resources)
		updated = true
	}
	// Update volumes
	if !volumesEqual(ss.Spec.Template.Spec.Volumes, volumes) {
		ss.Spec.Template.Spec.Volumes = volumes
		updated = true
	}
	if updated {
		if err := r.Update(ctx, &ss); err != nil {
			return err
		}
		engramLogger.Info("updated StatefulSet", "name", ssName)
	}
	return nil
}

// getSharedStorageForNamespace ensures shared storage exists for the namespace
// This is different from StoryRun-specific storage - it's a namespace-wide shared space
func (r *EngramReconciler) getSharedStorageForNamespace(ctx context.Context, namespace string) ([]corev1.VolumeMount, []corev1.Volume, error) {
	// Create a namespace-scoped shared storage
	pvcName, err := r.StorageManager.EnsureStoryRunStorage(ctx, namespace, "namespace-shared", namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to ensure namespace shared storage: %w", err)
	}

	// Get volume spec for shared storage
	volumeMounts, volumes, _ := r.StorageManager.GetVolumeSpec(pvcName)

	return volumeMounts, volumes, nil
}

// volumesEqual compares two volume slices for equality
func volumesEqual(a, b []corev1.Volume) bool {
	if len(a) != len(b) {
		return false
	}
	for i, vol := range a {
		if vol.Name != b[i].Name {
			return false
		}
		// For PVC volumes, compare claim names
		if vol.PersistentVolumeClaim != nil && b[i].PersistentVolumeClaim != nil {
			if vol.PersistentVolumeClaim.ClaimName != b[i].PersistentVolumeClaim.ClaimName {
				return false
			}
		}
	}
	return true
}

// Helper functions

// Status management is now handled by ConditionManager and ReconcilePattern

// resolveEngramImage resolves container image using TemplateResolver
func (r *EngramReconciler) resolveEngramImage(ctx context.Context, engram *bubushv1alpha1.Engram) string {
	if resolved, err := r.templateResolver.ResolveEngram(ctx, engram); err == nil {
		return resolved.Image
	}
	// Fallback to convention-based naming
	if engram.Spec.Engine == nil || engram.Spec.Engine.TemplateRef == "" {
		return "ghcr.io/bubustack/engram-default:latest"
	}
	return "ghcr.io/bubustack/" + engram.Spec.Engine.TemplateRef + ":latest"
}

// ensureService ensures a ClusterIP Service exists for the engram
func (r *EngramReconciler) ensureService(ctx context.Context, engram *bubushv1alpha1.Engram, selector map[string]string, targetPort int32) error {
	svcName := fmt.Sprintf("engram-%s", engram.Name)
	var svc corev1.Service
	if err := r.Get(ctx, types.NamespacedName{Name: svcName, Namespace: engram.Namespace}, &svc); err != nil {
		svc = corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      svcName,
				Namespace: engram.Namespace,
				Labels:    selector,
			},
			Spec: corev1.ServiceSpec{
				Selector: selector,
				Ports: []corev1.ServicePort{{
					Name:       "http",
					Port:       80,
					TargetPort: intstr.FromInt(int(targetPort)),
				}},
			},
		}
		if err := ctrl.SetControllerReference(engram, &svc, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, &svc)
	}
	// Update minimal fields if drifted
	desired := []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt(int(targetPort))}}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 80 || svc.Spec.Ports[0].TargetPort.IntValue() != int(targetPort) {
		svc.Spec.Ports = desired
		return r.Update(ctx, &svc)
	}
	return nil
}

// buildResourceRequirements maps WorkloadResources to corev1.ResourceRequirements
func (r *EngramReconciler) buildResourceRequirements(resources *bubushv1alpha1.WorkloadResources) corev1.ResourceRequirements {
	reqs := corev1.ResourceRequirements{Requests: corev1.ResourceList{}, Limits: corev1.ResourceList{}}
	if resources == nil {
		return reqs
	}
	if resources.Requests != nil {
		if resources.Requests.CPU != "" {
			if q, err := resource.ParseQuantity(resources.Requests.CPU); err == nil {
				reqs.Requests[corev1.ResourceCPU] = q
			}
		}
		if resources.Requests.Memory != "" {
			if q, err := resource.ParseQuantity(resources.Requests.Memory); err == nil {
				reqs.Requests[corev1.ResourceMemory] = q
			}
		}
	}
	if resources.Limits != nil {
		if resources.Limits.CPU != "" {
			if q, err := resource.ParseQuantity(resources.Limits.CPU); err == nil {
				reqs.Limits[corev1.ResourceCPU] = q
			}
		}
		if resources.Limits.Memory != "" {
			if q, err := resource.ParseQuantity(resources.Limits.Memory); err == nil {
				reqs.Limits[corev1.ResourceMemory] = q
			}
		}
	}
	return reqs
}

// SetupWithManager sets up the controller with the Manager.
func (r *EngramReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.SetupWithManagerAndConfig(mgr, controller.Options{MaxConcurrentReconciles: 5})
}

// SetupWithManagerAndConfig sets up the controller with the Manager using configurable options.
func (r *EngramReconciler) SetupWithManagerAndConfig(mgr ctrl.Manager, options controller.Options) error {
	return r.setupControllerWithOptions(mgr, options)
}

// setupControllerWithOptions configures the controller with specific options
func (r *EngramReconciler) setupControllerWithOptions(mgr ctrl.Manager, options controller.Options) error {
	ctx := context.Background()

	// Index EngramTemplates by name for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &catalogv1alpha1.EngramTemplate{}, "metadata.name",
		func(obj client.Object) []string {
			return []string{obj.GetName()}
		}); err != nil {
		return err
	}

	// Index Engrams by template reference for efficient template impact analysis
	if err := mgr.GetFieldIndexer().IndexField(ctx, &bubushv1alpha1.Engram{}, "spec.engine.templateRef",
		func(obj client.Object) []string {
			engram := obj.(*bubushv1alpha1.Engram)
			if engram.Spec.Engine != nil && engram.Spec.Engine.TemplateRef != "" {
				return []string{engram.Spec.Engine.TemplateRef}
			}
			return []string{}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&bubushv1alpha1.Engram{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		WithOptions(options).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("engram").
		Complete(r)
}
