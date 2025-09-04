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
)

// ImpulseReconciler reconciles a Impulse object
type ImpulseReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	conditionManager *ConditionManager
	finalizerManager *FinalizerManager
	templateResolver *TemplateResolver
	// reconcilePattern removed
	referenceValidator *ReferenceValidator
}

// +kubebuilder:rbac:groups=bubu.sh,resources=impulses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bubu.sh,resources=impulses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bubu.sh,resources=impulses/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=bubu.sh,resources=stories,verbs=get;list;watch

// Reconcile validates the Impulse, ensures referenced Story exists, and creates
// or updates the underlying Deployment/StatefulSet and Service.
func (r *ImpulseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Initialize structured logging and metrics
	rl := logging.NewReconcileLogger(ctx, "impulse")
	startTime := time.Now()

	defer func() {
		duration := time.Since(startTime)
		metrics.RecordControllerReconcile("impulse", duration, nil)
	}()

	var imp bubushv1alpha1.Impulse
	if err := r.Get(ctx, req.NamespacedName, &imp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Add Impulse context to logger
	impulseLogger := rl.WithImpulse(&imp)
	rl.ReconcileStart("Processing Impulse")

	// Initialize managers if needed
	if r.conditionManager == nil {
		r.conditionManager = NewConditionManager(imp.Generation)
	}
	if r.finalizerManager == nil {
		r.finalizerManager = NewFinalizerManager(r.Client)
	}
	if r.templateResolver == nil {
		r.templateResolver = NewTemplateResolver(r.Client)
	}
	// ReconcilePattern removed - using direct reconcile logic
	if r.referenceValidator == nil {
		r.referenceValidator = NewReferenceValidator(r.Client)
	}

	// Add finalizer for cross-resource references
	if !r.finalizerManager.HasFinalizer(&imp, ImpulseFinalizerName) {
		r.finalizerManager.AddFinalizer(&imp, ImpulseFinalizerName)
		if err := r.Update(ctx, &imp); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Handle deletion
	if imp.DeletionTimestamp != nil {
		conditions := &imp.Status.Conditions
		r.conditionManager.SetTerminatingCondition(conditions, true, ReasonDeletionRequested, "Impulse is being deleted")
		// Cleanup logic would go here
		r.finalizerManager.RemoveFinalizer(&imp, ImpulseFinalizerName)
		if err := r.Update(ctx, &imp); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Basic validation
	if imp.Spec.StoryRef == "" {
		conditions := &imp.Status.Conditions
		r.conditionManager.SetReadyCondition(conditions, false, ReasonValidationFailed, "storyRef is required")
		r.conditionManager.SetDegradedCondition(conditions, true, ReasonValidationFailed, "StoryRef missing")
		rl.ReconcileError(fmt.Errorf("storyRef missing"), "StoryRef is required")
		if err := r.Status().Update(ctx, &imp); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	// Use TemplateResolver to validate and resolve configuration
	resolvedImpulse, err := r.templateResolver.ResolveImpulse(ctx, &imp)
	if err != nil {
		conditions := &imp.Status.Conditions
		r.conditionManager.SetReadyCondition(conditions, false, ReasonTemplateResolutionFailed, fmt.Sprintf("Template resolution failed: %v", err))
		r.conditionManager.SetCondition(conditions, ConditionTemplateResolved, metav1.ConditionFalse, ReasonTemplateResolutionFailed, err.Error())
		rl.ReconcileError(err, "Template resolution failed")
		if updateErr := r.Status().Update(ctx, &imp); updateErr != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, updateErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Update template resolved condition
	conditions := &imp.Status.Conditions
	r.conditionManager.SetCondition(conditions, ConditionTemplateResolved, metav1.ConditionTrue, ReasonTemplateResolved, fmt.Sprintf("Template resolved from %s", resolvedImpulse.Source))

	// Set default engine mode
	if imp.Spec.Engine.Mode == "" {
		imp.Spec.Engine.Mode = "deployment"
	}

	// Validate engine mode (impulses are always-running, so deployment or statefulset)
	validModes := []string{"deployment", "statefulset"}
	validMode := false
	for _, vm := range validModes {
		if imp.Spec.Engine.Mode == vm {
			validMode = true
			break
		}
	}
	if !validMode {
		conditions := &imp.Status.Conditions
		r.conditionManager.SetReadyCondition(conditions, false, ReasonValidationFailed, fmt.Sprintf("invalid engine mode '%s' (impulses support: deployment, statefulset)", imp.Spec.Engine.Mode))
		rl.ReconcileError(fmt.Errorf("invalid engine mode: %s", imp.Spec.Engine.Mode), "Invalid engine mode for impulse")
		if err := r.Status().Update(ctx, &imp); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	impulseLogger.Info("Configuring Impulse", "mode", imp.Spec.Engine.Mode, "template", imp.Spec.Engine.TemplateRef, "story", imp.Spec.StoryRef)

	// Mode-specific validation (minimal since image comes from template)
	switch imp.Spec.Engine.Mode {
	case "deployment":
		// Deployment mode: will create a deployment that listens for events
		impulseLogger.Info("Impulse configured for deployment mode", "templateRef", imp.Spec.Engine.TemplateRef)
	case "statefulset":
		// StatefulSet mode: for stateful triggers like Kafka consumers
		impulseLogger.Info("Impulse configured for statefulset mode", "templateRef", imp.Spec.Engine.TemplateRef)
	}

	// Validate story reference using ReferenceValidator
	if err := r.referenceValidator.ValidateStoryReference(ctx, imp.Spec.StoryRef, imp.Namespace); err != nil {
		conditions := &imp.Status.Conditions
		r.conditionManager.SetReadyCondition(conditions, false, ReasonStoryReferenceInvalid, fmt.Sprintf("Referenced story '%s' not found: %v", imp.Spec.StoryRef, err))
		r.conditionManager.SetCondition(conditions, ConditionStoryResolved, metav1.ConditionFalse, ReasonStoryReferenceInvalid, err.Error())
		rl.ReconcileError(err, "Referenced story not found", "story", imp.Spec.StoryRef)
		if updateErr := r.Status().Update(ctx, &imp); updateErr != nil {
			impulseLogger.Error(updateErr, "failed to update status after story reference error")
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Update story resolved condition
	r.conditionManager.SetCondition(conditions, ConditionStoryResolved, metav1.ConditionTrue, ReasonStoryResolved, fmt.Sprintf("Story reference '%s' resolved", imp.Spec.StoryRef))

	impulseLogger.Info("Story reference validated", "story", imp.Spec.StoryRef)

	// Update status to ready using ConditionManager
	conditions = &imp.Status.Conditions
	r.conditionManager.SetReadyCondition(conditions, true, ReasonValidationPassed, "Impulse is ready and deployed")
	r.conditionManager.SetCondition(conditions, ConditionValidated, metav1.ConditionTrue, ReasonValidationPassed, "Impulse configuration validated")
	r.conditionManager.SetCondition(conditions, ConditionListening, metav1.ConditionTrue, ReasonDeploymentReady, "Impulse is listening for triggers")
	if err := r.Status().Update(ctx, &imp); err != nil {
		rl.ReconcileError(err, "Failed to update impulse status")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	// Handle different engine modes
	switch imp.Spec.Engine.Mode {
	case "deployment":
		result, err := r.handleDeploymentMode(ctx, &imp)
		if err != nil {
			rl.ReconcileError(err, "Failed to handle deployment mode")
		} else {
			rl.ReconcileSuccess("Impulse deployment configured successfully", "mode", "deployment")
		}
		return result, err
	case "statefulset":
		result, err := r.handleStatefulSetMode(ctx, &imp)
		if err != nil {
			rl.ReconcileError(err, "Failed to handle statefulset mode")
		} else {
			rl.ReconcileSuccess("Impulse statefulset configured successfully", "mode", "statefulset")
		}
		return result, err
	default:
		conditions := &imp.Status.Conditions
		r.conditionManager.SetReadyCondition(conditions, false, ReasonValidationFailed, fmt.Sprintf("unsupported engine mode: %s", imp.Spec.Engine.Mode))
		rl.ReconcileError(fmt.Errorf("unsupported engine mode: %s", imp.Spec.Engine.Mode), "Unsupported engine mode")
		if err := r.Status().Update(ctx, &imp); err != nil {
			impulseLogger.Error(err, "failed to update status after unsupported mode error")
		}
		return ctrl.Result{}, nil
	}
}

// handleDeploymentMode creates a Deployment for deployment-based impulses
func (r *ImpulseReconciler) handleDeploymentMode(ctx context.Context, imp *bubushv1alpha1.Impulse) (ctrl.Result, error) {
	impulseLogger := logging.NewControllerLogger(ctx, "impulse").WithImpulse(imp)

	image := r.resolveImpulseImage(ctx, imp)
	replicas := int32(1)
	if imp.Spec.Engine.Workload != nil && imp.Spec.Engine.Workload.Replicas != nil {
		replicas = *imp.Spec.Engine.Workload.Replicas
	}

	labels := map[string]string{
		"app.kubernetes.io/name":      "bobrapet",
		"app.kubernetes.io/component": "impulse",
		"bubu.sh/impulse":             imp.Name,
		"bubu.sh/story":               imp.Spec.StoryRef,
	}

	if err := r.ensureImpulseService(ctx, imp, labels, 8080); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure service: %w", err)
	}

	depName := fmt.Sprintf("impulse-%s", imp.Name)
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Name: depName, Namespace: imp.Namespace}, &dep); err != nil {
		dep = appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: depName, Namespace: imp.Namespace, Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name:  "impulse",
						Image: image,
						Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
						SecurityContext: &corev1.SecurityContext{
							RunAsNonRoot:             &[]bool{true}[0],
							AllowPrivilegeEscalation: &[]bool{false}[0],
							ReadOnlyRootFilesystem:   &[]bool{true}[0],
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						Resources: r.buildResourceRequirements(imp.Spec.Resources),
					}}},
				},
			},
		}
		if err := ctrl.SetControllerReference(imp, &dep, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, &dep); err != nil {
			return ctrl.Result{}, err
		}
		impulseLogger.Info("created Deployment", "name", depName)
		return ctrl.Result{}, nil
	}

	updated := false
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != replicas {
		dep.Spec.Replicas = &replicas
		updated = true
	}
	if len(dep.Spec.Template.Spec.Containers) == 0 || dep.Spec.Template.Spec.Containers[0].Image != image {
		if len(dep.Spec.Template.Spec.Containers) == 0 {
			dep.Spec.Template.Spec.Containers = []corev1.Container{{Name: "impulse"}}
		}
		c := &dep.Spec.Template.Spec.Containers[0]
		c.Image = image
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{
				RunAsNonRoot:             &[]bool{true}[0],
				AllowPrivilegeEscalation: &[]bool{false}[0],
				ReadOnlyRootFilesystem:   &[]bool{true}[0],
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			}
		}
		c.Resources = r.buildResourceRequirements(imp.Spec.Resources)
		updated = true
	}
	if updated {
		if err := r.Update(ctx, &dep); err != nil {
			return ctrl.Result{}, err
		}
		impulseLogger.Info("updated Deployment", "name", depName)
	}
	return ctrl.Result{}, nil
}

// handleStatefulSetMode creates a StatefulSet for statefulset-based impulses
func (r *ImpulseReconciler) handleStatefulSetMode(ctx context.Context, imp *bubushv1alpha1.Impulse) (ctrl.Result, error) {
	impulseLogger := logging.NewControllerLogger(ctx, "impulse").WithImpulse(imp)

	image := r.resolveImpulseImage(ctx, imp)
	replicas := int32(1)
	if imp.Spec.Engine.Workload != nil && imp.Spec.Engine.Workload.Replicas != nil {
		replicas = *imp.Spec.Engine.Workload.Replicas
	}

	labels := map[string]string{
		"app.kubernetes.io/name":      "bobrapet",
		"app.kubernetes.io/component": "impulse",
		"bubu.sh/impulse":             imp.Name,
		"bubu.sh/story":               imp.Spec.StoryRef,
	}

	if err := r.ensureImpulseService(ctx, imp, labels, 8080); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure service: %w", err)
	}

	ssName := fmt.Sprintf("impulse-%s", imp.Name)
	var ss appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Name: ssName, Namespace: imp.Namespace}, &ss); err != nil {
		serviceName := fmt.Sprintf("impulse-%s", imp.Name)
		ss = appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: ssName, Namespace: imp.Namespace, Labels: labels},
			Spec: appsv1.StatefulSetSpec{
				ServiceName: serviceName,
				Replicas:    &replicas,
				Selector:    &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name:  "impulse",
						Image: image,
						Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
						SecurityContext: &corev1.SecurityContext{
							RunAsNonRoot:             &[]bool{true}[0],
							AllowPrivilegeEscalation: &[]bool{false}[0],
							ReadOnlyRootFilesystem:   &[]bool{true}[0],
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						Resources: r.buildResourceRequirements(imp.Spec.Resources),
					}}},
				},
			},
		}
		if err := ctrl.SetControllerReference(imp, &ss, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, &ss); err != nil {
			return ctrl.Result{}, err
		}
		impulseLogger.Info("created StatefulSet", "name", ssName)
		return ctrl.Result{}, nil
	}

	updated := false
	if ss.Spec.Replicas == nil || *ss.Spec.Replicas != replicas {
		ss.Spec.Replicas = &replicas
		updated = true
	}
	if len(ss.Spec.Template.Spec.Containers) == 0 || ss.Spec.Template.Spec.Containers[0].Image != image {
		if len(ss.Spec.Template.Spec.Containers) == 0 {
			ss.Spec.Template.Spec.Containers = []corev1.Container{{Name: "impulse"}}
		}
		c := &ss.Spec.Template.Spec.Containers[0]
		c.Image = image
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{
				RunAsNonRoot:             &[]bool{true}[0],
				AllowPrivilegeEscalation: &[]bool{false}[0],
				ReadOnlyRootFilesystem:   &[]bool{true}[0],
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			}
		}
		c.Resources = r.buildResourceRequirements(imp.Spec.Resources)
		updated = true
	}
	if updated {
		if err := r.Update(ctx, &ss); err != nil {
			return ctrl.Result{}, err
		}
		impulseLogger.Info("updated StatefulSet", "name", ssName)
	}
	return ctrl.Result{}, nil
}

// buildResourceRequirements converts WorkloadResources to Kubernetes ResourceRequirements
func (r *ImpulseReconciler) buildResourceRequirements(resources *bubushv1alpha1.WorkloadResources) corev1.ResourceRequirements {
	reqs := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	if resources == nil {
		return reqs
	}

	// Handle resource requests
	if resources.Requests != nil {
		if resources.Requests.CPU != "" {
			if quantity, err := resource.ParseQuantity(resources.Requests.CPU); err == nil {
				reqs.Requests[corev1.ResourceCPU] = quantity
			}
		}
		if resources.Requests.Memory != "" {
			if quantity, err := resource.ParseQuantity(resources.Requests.Memory); err == nil {
				reqs.Requests[corev1.ResourceMemory] = quantity
			}
		}
	}

	// Handle resource limits
	if resources.Limits != nil {
		if resources.Limits.CPU != "" {
			if quantity, err := resource.ParseQuantity(resources.Limits.CPU); err == nil {
				reqs.Limits[corev1.ResourceCPU] = quantity
			}
		}
		if resources.Limits.Memory != "" {
			if quantity, err := resource.ParseQuantity(resources.Limits.Memory); err == nil {
				reqs.Limits[corev1.ResourceMemory] = quantity
			}
		}
	}

	return reqs
}

// Helper functions

// Status management is now handled by ConditionManager and ReconcilePattern

// SetupWithManager sets up the controller with the Manager.
func (r *ImpulseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.SetupWithManagerAndConfig(mgr, controller.Options{MaxConcurrentReconciles: 5})
}

// SetupWithManagerAndConfig sets up the controller with the Manager using configurable options.
func (r *ImpulseReconciler) SetupWithManagerAndConfig(mgr ctrl.Manager, options controller.Options) error {
	return r.setupControllerWithOptions(mgr, options)
}

// setupControllerWithOptions configures the controller with specific options
func (r *ImpulseReconciler) setupControllerWithOptions(mgr ctrl.Manager, options controller.Options) error {
	ctx := context.Background()

	// Index Impulses by Story reference for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &bubushv1alpha1.Impulse{}, "spec.storyRef",
		func(obj client.Object) []string {
			return []string{obj.(*bubushv1alpha1.Impulse).Spec.StoryRef}
		}); err != nil {
		return err
	}

	// Index ImpulseTemplates by name for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &catalogv1alpha1.ImpulseTemplate{}, "metadata.name",
		func(obj client.Object) []string {
			return []string{obj.GetName()}
		}); err != nil {
		return err
	}

	// Index Impulses by template reference for efficient template impact analysis
	if err := mgr.GetFieldIndexer().IndexField(ctx, &bubushv1alpha1.Impulse{}, "spec.engine.templateRef",
		func(obj client.Object) []string {
			impulse := obj.(*bubushv1alpha1.Impulse)
			if impulse.Spec.Engine != nil && impulse.Spec.Engine.TemplateRef != "" {
				return []string{impulse.Spec.Engine.TemplateRef}
			}
			return []string{}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&bubushv1alpha1.Impulse{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		WithOptions(options).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("bubushv1alpha1.impulse").
		Complete(r)
}

// helpers

// resolveImpulseImage resolves image using TemplateResolver
func (r *ImpulseReconciler) resolveImpulseImage(ctx context.Context, imp *bubushv1alpha1.Impulse) string {
	if resolved, err := r.templateResolver.ResolveImpulse(ctx, imp); err == nil {
		return resolved.Image
	}
	// Fallback to convention-based naming
	if imp.Spec.Engine == nil || imp.Spec.Engine.TemplateRef == "" {
		return "ghcr.io/bubustack/engram-default:latest"
	}
	return "ghcr.io/bubustack/" + imp.Spec.Engine.TemplateRef + ":latest"
}

// ensureImpulseService ensures Service exists for impulse
func (r *ImpulseReconciler) ensureImpulseService(ctx context.Context, imp *bubushv1alpha1.Impulse, selector map[string]string, targetPort int32) error {
	svcName := fmt.Sprintf("impulse-%s", imp.Name)
	var svc corev1.Service
	if err := r.Get(ctx, types.NamespacedName{Name: svcName, Namespace: imp.Namespace}, &svc); err != nil {
		svc = corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: imp.Namespace, Labels: selector},
			Spec: corev1.ServiceSpec{
				Selector: selector,
				Ports: []corev1.ServicePort{{
					Name:       "http",
					Port:       80,
					TargetPort: intstr.FromInt(int(targetPort)),
				}},
			},
		}
		if err := ctrl.SetControllerReference(imp, &svc, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, &svc)
	}
	desired := []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt(int(targetPort))}}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 80 || svc.Spec.Ports[0].TargetPort.IntValue() != int(targetPort) {
		svc.Spec.Ports = desired
		return r.Update(ctx, &svc)
	}
	return nil
}
