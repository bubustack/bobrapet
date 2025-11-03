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
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	config "github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/internal/controller/naming"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/patch"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

// ImpulseReconciler reconciles a Impulse object
type ImpulseReconciler struct {
	config.ControllerDependencies
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=bubustack.io,resources=impulses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bubustack.io,resources=impulses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bubustack.io,resources=impulses/finalizers,verbs=update
// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=impulsetemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.2/pkg/reconcile
func (r *ImpulseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "impulse").WithValues("impulse", req.NamespacedName)

	ctx, cancel := r.withReconcileTimeout(ctx)
	defer cancel()

	impulse, err := r.fetchImpulse(ctx, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if handled := r.handleImpulseShortCircuit(impulse); handled {
		return ctrl.Result{}, nil
	}

	template, stop, err := r.loadImpulseTemplate(ctx, impulse, log)
	if err != nil {
		return ctrl.Result{}, err
	}
	if stop {
		return ctrl.Result{}, nil
	}

	resolvedConfig, err := r.resolveImpulseConfig(ctx, impulse, template, log)
	if err != nil {
		return ctrl.Result{}, err
	}

	deployment, reconcileErr := r.reconcileDeployment(ctx, impulse, template, resolvedConfig)
	if reconcileErr != nil {
		return ctrl.Result{}, r.handleDeploymentError(ctx, impulse, reconcileErr)
	}

	// Update status based on the Deployment's state
	if err := r.updateImpulseStatus(ctx, impulse, deployment); err != nil {
		return ctrl.Result{}, err
	}

	// Reconcile the Service whenever the template exposes ports.
	if _, serviceErr := r.reconcileService(ctx, impulse, resolvedConfig); serviceErr != nil {
		return ctrl.Result{}, r.handleServiceError(ctx, impulse, serviceErr)
	}
	return ctrl.Result{}, nil
}

func (r *ImpulseReconciler) reconcileDeployment(ctx context.Context, impulse *v1alpha1.Impulse, template *catalogv1alpha1.ImpulseTemplate, execCfg *config.ResolvedExecutionConfig) (*appsv1.Deployment, error) {
	log := logging.NewReconcileLogger(ctx, "impulse-deployment").WithValues("impulse", impulse.Name)
	deployment := &appsv1.Deployment{}
	deploymentName := naming.Compose(impulse.Name, impulse.Spec.TemplateRef.Name, "impulse")
	deploymentKey := types.NamespacedName{Name: deploymentName, Namespace: impulse.Namespace}
	engramTemplate := convertImpulseTemplate(template)

	err := r.Get(ctx, deploymentKey, deployment)
	if err != nil {
		if errors.IsNotFound(err) {
			// Create it
			newDeployment := r.buildDeploymentForImpulse(impulse, engramTemplate, execCfg)
			if err := controllerutil.SetControllerReference(impulse, newDeployment, r.Scheme); err != nil {
				return nil, fmt.Errorf("failed to set owner reference on Deployment: %w", err)
			}
			if err := r.Create(ctx, newDeployment); err != nil {
				return nil, fmt.Errorf("failed to create Deployment: %w", err)
			}
			log.Info("Created new Deployment for Impulse")
			return newDeployment, nil
		}
		return nil, fmt.Errorf("failed to get Deployment: %w", err)
	}

	// It exists, so check for updates.
	desiredDeployment := r.buildDeploymentForImpulse(impulse, engramTemplate, execCfg)
	// A simple way to check for differences is to compare the pod templates.
	// For more complex scenarios, a 3-way merge or more specific field comparisons might be needed.
	if !reflect.DeepEqual(deployment.Spec.Template, desiredDeployment.Spec.Template) {
		log.Info("Deployment spec has changed, updating.")
		original := deployment.DeepCopy()
		deployment.Spec.Template = desiredDeployment.Spec.Template
		if err := r.Patch(ctx, deployment, client.MergeFrom(original)); err != nil {
			return nil, fmt.Errorf("failed to update Deployment: %w", err)
		}
	}

	return deployment, nil
}

func (r *ImpulseReconciler) withReconcileTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (r *ImpulseReconciler) fetchImpulse(ctx context.Context, key types.NamespacedName) (*v1alpha1.Impulse, error) {
	var impulse v1alpha1.Impulse
	if err := r.Get(ctx, key, &impulse); err != nil {
		return nil, err
	}
	return &impulse, nil
}

func (r *ImpulseReconciler) handleImpulseShortCircuit(impulse *v1alpha1.Impulse) bool {
	if !impulse.DeletionTimestamp.IsZero() {
		return true
	}
	if impulse.Status.Phase.IsTerminal() && impulse.Status.ObservedGeneration == impulse.Generation {
		return true
	}
	return false
}

func (r *ImpulseReconciler) loadImpulseTemplate(ctx context.Context, impulse *v1alpha1.Impulse, log *logging.ControllerLogger) (*catalogv1alpha1.ImpulseTemplate, bool, error) {
	var template catalogv1alpha1.ImpulseTemplate
	err := r.Get(ctx, types.NamespacedName{Name: impulse.Spec.TemplateRef.Name}, &template)
	if err == nil {
		return &template, false, nil
	}
	if errors.IsNotFound(err) {
		log.Error(err, "ImpulseTemplate not found")
		if r.Recorder != nil {
			r.Recorder.Event(impulse, corev1.EventTypeWarning, conditions.ReasonTemplateNotFound, fmt.Sprintf("ImpulseTemplate '%s' not found", impulse.Spec.TemplateRef.Name))
		}
		if patchErr := r.setImpulsePhase(ctx, impulse, enums.PhaseBlocked, fmt.Sprintf("ImpulseTemplate '%s' not found", impulse.Spec.TemplateRef.Name)); patchErr != nil {
			return nil, false, patchErr
		}
		return nil, true, nil
	}
	return nil, false, fmt.Errorf("failed to get ImpulseTemplate: %w", err)
}

func (r *ImpulseReconciler) resolveImpulseConfig(ctx context.Context, impulse *v1alpha1.Impulse, template *catalogv1alpha1.ImpulseTemplate, log *logging.ControllerLogger) (*config.ResolvedExecutionConfig, error) {
	engramTemplate := convertImpulseTemplate(template)
	resolvedConfig, resolveErr := r.ConfigResolver.ResolveExecutionConfig(ctx, nil, nil, nil, engramTemplate, nil)
	if resolveErr != nil {
		if r.Recorder != nil {
			r.Recorder.Event(impulse, corev1.EventTypeWarning, conditions.ReasonInvalidConfiguration, fmt.Sprintf("Failed to resolve execution config: %s", resolveErr))
		}
		if patchErr := r.setImpulsePhase(ctx, impulse, enums.PhaseFailed, fmt.Sprintf("Failed to resolve configuration: %s", resolveErr)); patchErr != nil {
			log.Error(patchErr, "Failed to update impulse status")
			return nil, patchErr
		}
		return nil, fmt.Errorf("failed to resolve execution config for impulse '%s': %w", impulse.Name, resolveErr)
	}

	if len(impulse.Spec.Secrets) > 0 {
		if resolvedConfig.Secrets == nil {
			resolvedConfig.Secrets = make(map[string]string, len(impulse.Spec.Secrets))
		}
		for key, value := range impulse.Spec.Secrets {
			resolvedConfig.Secrets[key] = value
		}
	}

	r.ConfigResolver.ApplyExecutionOverrides(impulse.Spec.Execution, resolvedConfig)
	return resolvedConfig, nil
}

func (r *ImpulseReconciler) handleDeploymentError(ctx context.Context, impulse *v1alpha1.Impulse, reconcileErr error) error {
	if patchErr := r.setImpulsePhase(ctx, impulse, enums.PhaseFailed, fmt.Sprintf("Failed to reconcile Deployment: %s", reconcileErr)); patchErr != nil {
		return patchErr
	}
	return fmt.Errorf("failed to reconcile Deployment for impulse '%s': %w", impulse.Name, reconcileErr)
}

func (r *ImpulseReconciler) handleServiceError(ctx context.Context, impulse *v1alpha1.Impulse, serviceErr error) error {
	if patchErr := r.setImpulsePhase(ctx, impulse, enums.PhaseFailed, fmt.Sprintf("Failed to reconcile Service: %s", serviceErr)); patchErr != nil {
		return patchErr
	}
	return fmt.Errorf("failed to reconcile Service for impulse '%s': %w", impulse.Name, serviceErr)
}

func (r *ImpulseReconciler) updateImpulseStatus(ctx context.Context, impulse *v1alpha1.Impulse, deployment *appsv1.Deployment) error {
	var newPhase enums.Phase
	var message string

	// Determine the phase based on the Deployment status.
	if deployment.Status.ReadyReplicas == 0 {
		newPhase = enums.PhasePending
		message = "Waiting for Deployment pods to become ready."
	} else if deployment.Status.ReadyReplicas < *deployment.Spec.Replicas {
		newPhase = enums.PhaseRunning
		message = "Deployment is scaling up."
	} else {
		newPhase = enums.PhaseRunning // Running is the terminal state for a healthy Impulse
		message = "Impulse is active and listening for events."
	}

	if impulse.Status.Phase != newPhase || impulse.Status.ReadyReplicas != deployment.Status.ReadyReplicas {
		err := patch.RetryableStatusPatch(ctx, r.Client, impulse, func(obj client.Object) {
			i := obj.(*v1alpha1.Impulse)
			i.Status.Phase = newPhase
			i.Status.ObservedGeneration = i.Generation
			i.Status.Replicas = deployment.Status.Replicas
			i.Status.ReadyReplicas = deployment.Status.ReadyReplicas

			cm := conditions.NewConditionManager(i.Generation)
			if newPhase == enums.PhaseRunning && message == "Impulse is active and listening for events." {
				cm.SetCondition(&i.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonDeploymentReady, message)
			} else {
				cm.SetCondition(&i.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonReconciling, message)
			}
		})
		if err != nil {
			return fmt.Errorf("failed to update impulse status: %w", err)
		}
	}

	return nil
}

func (r *ImpulseReconciler) setImpulsePhase(ctx context.Context, impulse *v1alpha1.Impulse, phase enums.Phase, message string) error {
	return patch.RetryableStatusPatch(ctx, r.Client, impulse, func(obj client.Object) {
		i := obj.(*v1alpha1.Impulse)
		i.Status.Phase = phase
		i.Status.ObservedGeneration = i.Generation

		cm := conditions.NewConditionManager(i.Generation)

		// Use standard reasons from pkg/conditions to avoid hard-coded literals
		var (
			status = metav1.ConditionFalse
			reason string
		)
		switch phase {
		case enums.PhaseRunning:
			// Running implies underlying deployment is healthy
			status = metav1.ConditionTrue
			reason = conditions.ReasonDeploymentReady
		case enums.PhaseBlocked:
			// Blocked generally maps to scheduling/validation issues
			reason = conditions.ReasonSchedulingFailed
		case enums.PhaseFailed:
			reason = conditions.ReasonExecutionFailed
		default:
			// Keep default Reconciling for transitional phases
			reason = conditions.ReasonReconciling
		}

		cm.SetCondition(&i.Status.Conditions, conditions.ConditionReady, status, reason, message)
	})
}

// buildDeploymentForImpulse creates a new Deployment object for an Impulse
func (r *ImpulseReconciler) buildDeploymentForImpulse(impulse *v1alpha1.Impulse, engramTemplate *catalogv1alpha1.EngramTemplate, execCfg *config.ResolvedExecutionConfig) *appsv1.Deployment {
	deploymentName := naming.Compose(impulse.Name, impulse.Spec.TemplateRef.Name, "impulse")
	labels := map[string]string{
		"app.kubernetes.io/name":       "bobrapet-impulse",
		"app.kubernetes.io/instance":   impulse.Name,
		"app.kubernetes.io/managed-by": "bobrapet-operator",
	}
	replicas := int32(1)

	// Resolve the story reference to pass to the impulse pod.
	storyKey := impulse.Spec.StoryRef.ToNamespacedName(impulse)

	// Prepare environment variables for the impulse pod
	envVars := []corev1.EnvVar{
		{Name: "BUBU_IMPULSE_NAME", Value: impulse.Name},
		{Name: "BUBU_IMPULSE_NAMESPACE", Value: impulse.Namespace},
		{Name: "BUBU_TARGET_STORY_NAME", Value: storyKey.Name},
		{Name: "BUBU_TARGET_STORY_NAMESPACE", Value: storyKey.Namespace},
	}
	if impulse.Spec.With != nil && impulse.Spec.With.Raw != nil {
		envVars = append(envVars, corev1.EnvVar{Name: "BUBU_IMPULSE_WITH", Value: string(impulse.Spec.With.Raw)})
	}

	podSpec := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:           execCfg.ServiceAccountName,
			AutomountServiceAccountToken: ptr.To(execCfg.AutomountServiceAccountToken),
			SecurityContext:              execCfg.ToPodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "impulse",
				Image:           execCfg.Image,
				ImagePullPolicy: execCfg.ImagePullPolicy,
				LivenessProbe:   execCfg.LivenessProbe,
				ReadinessProbe:  execCfg.ReadinessProbe,
				StartupProbe:    execCfg.StartupProbe,
				SecurityContext: execCfg.ToContainerSecurityContext(),
				Resources:       execCfg.Resources,
				Env:             envVars,
			}},
		},
	}
	applyStorageEnv(execCfg, &podSpec.Spec.Containers[0])
	applySecretArtifacts(engramTemplate, execCfg, &podSpec)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: impulse.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: podSpec,
		},
	}

	return deployment
}

func (r *ImpulseReconciler) reconcileService(ctx context.Context, impulse *v1alpha1.Impulse, execCfg *config.ResolvedExecutionConfig) (*corev1.Service, error) {
	if execCfg == nil || len(execCfg.ServicePorts) == 0 {
		return nil, nil
	}

	log := logging.NewReconcileLogger(ctx, "impulse-service").WithValues("impulse", impulse.Name)
	serviceName := naming.Compose(impulse.Name, impulse.Spec.TemplateRef.Name, "impulse")
	serviceKey := types.NamespacedName{Name: serviceName, Namespace: impulse.Namespace}
	service := &corev1.Service{}

	if err := r.Get(ctx, serviceKey, service); err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get Service: %w", err)
		}
		newSvc := r.buildServiceForImpulse(impulse, execCfg, serviceName)
		if err := controllerutil.SetControllerReference(impulse, newSvc, r.Scheme); err != nil {
			return nil, fmt.Errorf("failed to set owner reference on Service: %w", err)
		}
		if err := r.Create(ctx, newSvc); err != nil {
			return nil, fmt.Errorf("failed to create Service: %w", err)
		}
		log.Info("Created new Service for Impulse")
		return newSvc, nil
	}

	desired := r.buildServiceForImpulse(impulse, execCfg, serviceName)
	desired.Spec.ClusterIP = service.Spec.ClusterIP
	desired.Spec.ClusterIPs = append([]string(nil), service.Spec.ClusterIPs...)
	desired.Spec.IPFamilies = append([]corev1.IPFamily(nil), service.Spec.IPFamilies...)
	desired.Spec.IPFamilyPolicy = service.Spec.IPFamilyPolicy
	desired.Spec.HealthCheckNodePort = service.Spec.HealthCheckNodePort
	desired.ResourceVersion = service.ResourceVersion

	if !reflect.DeepEqual(service.Spec, desired.Spec) ||
		!reflect.DeepEqual(service.Labels, desired.Labels) ||
		!reflect.DeepEqual(service.Annotations, desired.Annotations) {
		original := service.DeepCopy()
		service.Spec = desired.Spec
		service.Labels = desired.Labels
		service.Annotations = desired.Annotations
		if err := r.Patch(ctx, service, client.MergeFrom(original)); err != nil {
			return nil, fmt.Errorf("failed to update Service: %w", err)
		}
		log.Info("Updated Service for Impulse")
	}

	return service, nil
}

func (r *ImpulseReconciler) buildServiceForImpulse(impulse *v1alpha1.Impulse, execCfg *config.ResolvedExecutionConfig, serviceName string) *corev1.Service {
	selectorLabels := map[string]string{
		"app.kubernetes.io/name":       "bobrapet-impulse",
		"app.kubernetes.io/instance":   impulse.Name,
		"app.kubernetes.io/managed-by": "bobrapet-operator",
	}

	serviceLabels := make(map[string]string, len(selectorLabels)+len(execCfg.ServiceLabels))
	for k, v := range selectorLabels {
		serviceLabels[k] = v
	}
	for k, v := range execCfg.ServiceLabels {
		serviceLabels[k] = v
	}
	if impulse.Spec.Service != nil && len(impulse.Spec.Service.Labels) > 0 {
		for k, v := range impulse.Spec.Service.Labels {
			serviceLabels[k] = v
		}
	}

	annotations := make(map[string]string, len(execCfg.ServiceAnnotations))
	for k, v := range execCfg.ServiceAnnotations {
		annotations[k] = v
	}
	if impulse.Spec.Service != nil && len(impulse.Spec.Service.Annotations) > 0 {
		for k, v := range impulse.Spec.Service.Annotations {
			annotations[k] = v
		}
	}

	ports := make([]corev1.ServicePort, len(execCfg.ServicePorts))
	copy(ports, execCfg.ServicePorts)

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

// SetupWithManager sets up the controller with the Manager.
func (r *ImpulseReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	r.Recorder = mgr.GetEventRecorderFor("impulse-controller")
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Impulse{}).
		Watches(
			&catalogv1alpha1.ImpulseTemplate{},
			handler.EnqueueRequestsFromMapFunc(r.mapImpulseTemplateToImpulses),
		).
		WithOptions(opts).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}

func (r *ImpulseReconciler) mapImpulseTemplateToImpulses(ctx context.Context, obj client.Object) []reconcile.Request {
	var impulses v1alpha1.ImpulseList
	if err := r.List(ctx, &impulses, client.MatchingFields{"spec.templateRef.name": obj.GetName()}); err != nil {
		// Handle error
		return nil
	}

	requests := make([]reconcile.Request, len(impulses.Items))
	for i, item := range impulses.Items {
		requests[i] = reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      item.GetName(),
				Namespace: item.GetNamespace(),
			},
		}
	}
	return requests
}
