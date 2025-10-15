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
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/patch"
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

	timeout := r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var impulse v1alpha1.Impulse
	if err := r.Get(ctx, req.NamespacedName, &impulse); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !impulse.DeletionTimestamp.IsZero() {
		// Handle deletion if finalizers are added.
		return ctrl.Result{}, nil
	}

	if impulse.Status.Phase.IsTerminal() && impulse.Status.ObservedGeneration == impulse.Generation {
		return ctrl.Result{}, nil
	}

	// Fetch the template
	var template catalogv1alpha1.ImpulseTemplate
	if err := r.Get(ctx, types.NamespacedName{Name: impulse.Spec.TemplateRef.Name}, &template); err != nil {
		if errors.IsNotFound(err) {
			log.Error(err, "ImpulseTemplate not found")
			// Emit event for user visibility (guard recorder for tests)
			if r.Recorder != nil {
				r.Recorder.Event(&impulse, corev1.EventTypeWarning, conditions.ReasonTemplateNotFound, fmt.Sprintf("ImpulseTemplate '%s' not found", impulse.Spec.TemplateRef.Name))
			}
			err := r.setImpulsePhase(ctx, &impulse, enums.PhaseBlocked, fmt.Sprintf("ImpulseTemplate '%s' not found", impulse.Spec.TemplateRef.Name))
			return ctrl.Result{}, err // Stop reconciliation, wait for watch to trigger.
		}
		return ctrl.Result{}, fmt.Errorf("failed to get ImpulseTemplate: %w", err)
	}

	// Resolve the full configuration
	// The resolver expects an EngramTemplate, so we construct one from the ImpulseTemplate's spec.
	engramTemplate := &catalogv1alpha1.EngramTemplate{
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
	resolvedConfig, err := r.ConfigResolver.ResolveExecutionConfig(ctx, nil, nil, nil, engramTemplate)
	if err != nil {
		err := r.setImpulsePhase(ctx, &impulse, enums.PhaseFailed, fmt.Sprintf("Failed to resolve configuration: %s", err))
		if err != nil {
			log.Error(err, "Failed to update impulse status")
		}
		return ctrl.Result{}, fmt.Errorf("failed to resolve execution config for impulse '%s': %w", impulse.Name, err)
	}

	// Reconcile the deployment
	deployment, err := r.reconcileDeployment(ctx, &impulse, &template, resolvedConfig)
	if err != nil {
		err := r.setImpulsePhase(ctx, &impulse, enums.PhaseFailed, fmt.Sprintf("Failed to reconcile Deployment: %s", err))
		return ctrl.Result{}, err
	}

	// Update status based on the Deployment's state
	if err := r.updateImpulseStatus(ctx, &impulse, deployment); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *ImpulseReconciler) reconcileDeployment(ctx context.Context, impulse *v1alpha1.Impulse, template *catalogv1alpha1.ImpulseTemplate, execCfg *config.ResolvedExecutionConfig) (*appsv1.Deployment, error) {
	log := logging.NewReconcileLogger(ctx, "impulse-deployment").WithValues("impulse", impulse.Name)
	deployment := &appsv1.Deployment{}
	deploymentName := fmt.Sprintf("%s-%s-impulse", impulse.Name, impulse.Spec.TemplateRef.Name)
	deploymentKey := types.NamespacedName{Name: deploymentName, Namespace: impulse.Namespace}

	err := r.Get(ctx, deploymentKey, deployment)
	if err != nil {
		if errors.IsNotFound(err) {
			// Create it
			newDeployment := r.buildDeploymentForImpulse(impulse, template, execCfg)
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
	desiredDeployment := r.buildDeploymentForImpulse(impulse, template, execCfg)
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
func (r *ImpulseReconciler) buildDeploymentForImpulse(impulse *v1alpha1.Impulse, _ *catalogv1alpha1.ImpulseTemplate, execCfg *config.ResolvedExecutionConfig) *appsv1.Deployment {
	deploymentName := fmt.Sprintf("%s-%s-impulse", impulse.Name, impulse.Spec.TemplateRef.Name)
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
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: execCfg.ServiceAccountName,
					Containers: []corev1.Container{{
						Name:  "impulse",
						Image: execCfg.Image,
						Env:   envVars,
					}},
				},
			},
		},
	}

	return deployment
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
