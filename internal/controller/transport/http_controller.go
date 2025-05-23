/*
Copyright 2025.

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

package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	networkingv1alpha3 "istio.io/api/networking/v1alpha3"
	istiov1beta1 "istio.io/client-go/pkg/apis/networking/v1beta1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
)

// HTTPReconciler reconciles a HTTP object
type HTTPReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=transport.bubu.sh,resources=https,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=transport.bubu.sh,resources=https/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=transport.bubu.sh,resources=https/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=virtualservices,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles HTTP transport reconciliation
func (r *HTTPReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the HTTP instance
	http := &transportv1alpha1.HTTP{}
	err := r.Get(ctx, req.NamespacedName, http)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			log.Info("HTTP resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get HTTP")
		return ctrl.Result{}, err
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(http, "http.transport.bubu.sh/finalizer") {
		controllerutil.AddFinalizer(http, "http.transport.bubu.sh/finalizer")
		if err := r.Update(ctx, http); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if the object is being deleted
	if !http.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is being deleted
		if controllerutil.ContainsFinalizer(http, "http.transport.bubu.sh/finalizer") {
			// Our finalizer is present, so let's handle our external dependency
			log.Info("Deleting HTTP transport resources", "name", http.Name)

			// Remove our finalizer from the list and update it
			controllerutil.RemoveFinalizer(http, "http.transport.bubu.sh/finalizer")
			if err := r.Update(ctx, http); err != nil {
				return ctrl.Result{}, err
			}
		}
		// Stop reconciliation as the item is being deleted
		return ctrl.Result{}, nil
	}

	// Default image if not specified
	image := "bubustack/http-transport:latest"
	if http.Spec.Image != "" {
		image = http.Spec.Image
	}

	imagePullPolicy := "Never"
	if http.Spec.ImagePullPolicy != "" {
		imagePullPolicy = http.Spec.ImagePullPolicy
	}

	// Default replicas if not specified
	replicas := int32(1)
	if http.Spec.Replicas > 0 {
		replicas = http.Spec.Replicas
	}

	// Create ConfigMap for HTTP transport configuration
	configMap, err := r.reconcileConfigMap(ctx, http)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Create Deployment for HTTP transport
	deployment, err := r.reconcileDeployment(ctx, http, image, imagePullPolicy, replicas, configMap.Name)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Create Service for HTTP transport
	service, err := r.reconcileService(ctx, http)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Create VirtualService for HTTP transport
	_, err = r.reconcileVirtualService(ctx, http, service.Name)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Update status with retry
	if err := r.updateStatus(ctx, http, deployment); err != nil {
		log.Error(err, "Failed to update HTTP status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// reconcileConfigMap creates or updates a ConfigMap for the HTTP transport
func (r *HTTPReconciler) reconcileConfigMap(ctx context.Context, http *transportv1alpha1.HTTP) (*corev1.ConfigMap, error) {
	log := log.FromContext(ctx)

	// Serialize HTTP configuration to JSON
	config := map[string]interface{}{
		"path":         http.Spec.Path,
		"schema":       http.Spec.Schema,
		"destinations": http.Spec.Destinations,
	}

	configJSON, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}

	// Create ConfigMap
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      http.Name + "-config",
			Namespace: http.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     http.Name,
				"app.kubernetes.io/instance": http.Name,
				"app.kubernetes.io/part-of":  "http-transport",
			},
		},
		Data: map[string]string{
			"config.json": string(configJSON),
		},
	}

	// Set HTTP instance as the owner and controller
	if err := ctrl.SetControllerReference(http, configMap, r.Scheme); err != nil {
		return nil, err
	}

	// Create or update the ConfigMap
	foundConfigMap := &corev1.ConfigMap{}
	err = r.Get(ctx, types.NamespacedName{Name: configMap.Name, Namespace: configMap.Namespace}, foundConfigMap)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating a new ConfigMap", "ConfigMap.Namespace", configMap.Namespace, "ConfigMap.Name", configMap.Name)
		err = r.Create(ctx, configMap)
		if err != nil {
			log.Error(err, "Failed to create new ConfigMap", "ConfigMap.Namespace", configMap.Namespace, "ConfigMap.Name", configMap.Name)
			return nil, err
		}
		return configMap, nil
	} else if err != nil {
		log.Error(err, "Failed to get ConfigMap")
		return nil, err
	}

	// Update the found ConfigMap with new data
	foundConfigMap.Data = configMap.Data
	log.Info("Updating ConfigMap", "ConfigMap.Namespace", foundConfigMap.Namespace, "ConfigMap.Name", foundConfigMap.Name)
	err = r.Update(ctx, foundConfigMap)
	if err != nil {
		log.Error(err, "Failed to update ConfigMap", "ConfigMap.Namespace", foundConfigMap.Namespace, "ConfigMap.Name", foundConfigMap.Name)
		return nil, err
	}

	return foundConfigMap, nil
}

// reconcileDeployment creates or updates a Deployment for the HTTP transport
func (r *HTTPReconciler) reconcileDeployment(ctx context.Context, http *transportv1alpha1.HTTP, image string, imagePullPolicy string, replicas int32, configMapName string) (*appsv1.Deployment, error) {
	log := log.FromContext(ctx)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      http.Name,
			Namespace: http.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     http.Name,
				"app.kubernetes.io/instance": http.Name,
				"app.kubernetes.io/part-of":  "http-transport",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":     http.Name,
					"app.kubernetes.io/instance": http.Name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":     http.Name,
						"app.kubernetes.io/instance": http.Name,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "http-transport",
							Image:           image,
							ImagePullPolicy: corev1.PullPolicy(imagePullPolicy),
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 8080,
									Name:          "http",
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "config-volume",
									MountPath: "/config",
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "CONFIG_PATH",
									Value: "/config/config.json",
								},
								{
									Name:  "NATS_URL",
									Value: "nats://nats.bobrapet-system:4222",
								},
								{
									Name:  "PORT",
									Value: "8080",
								},
								{
									Name:  "NATS_MAX_RECONNECTS",
									Value: "5",
								},
								{
									Name:  "NATS_RECONNECT_WAIT_MS",
									Value: "1000",
								},
								{
									Name:  "NATS_CONNECT_TIMEOUT_MS",
									Value: "2000",
								},
								{
									Name:  "NATS_REQUEST_TIMEOUT_MS",
									Value: "30000",
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("300m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(8080),
									},
								},
								InitialDelaySeconds: 10,
								TimeoutSeconds:      5,
								PeriodSeconds:       10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(8080),
									},
								},
								InitialDelaySeconds: 5,
								TimeoutSeconds:      3,
								PeriodSeconds:       5,
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "config-volume",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: configMapName,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Set HTTP instance as the owner and controller
	if err := ctrl.SetControllerReference(http, deployment, r.Scheme); err != nil {
		return nil, err
	}

	// Create or update the Deployment
	foundDeployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}, foundDeployment)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating a new Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
		err = r.Create(ctx, deployment)
		if err != nil {
			log.Error(err, "Failed to create new Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
			return nil, err
		}
		return deployment, nil
	} else if err != nil {
		log.Error(err, "Failed to get Deployment")
		return nil, err
	}

	// Update the found Deployment with new spec
	foundDeployment.Spec = deployment.Spec
	log.Info("Updating Deployment", "Deployment.Namespace", foundDeployment.Namespace, "Deployment.Name", foundDeployment.Name)
	err = r.Update(ctx, foundDeployment)
	if err != nil {
		log.Error(err, "Failed to update Deployment", "Deployment.Namespace", foundDeployment.Namespace, "Deployment.Name", foundDeployment.Name)
		return nil, err
	}

	return foundDeployment, nil
}

// reconcileService creates or updates a Service for the HTTP transport
func (r *HTTPReconciler) reconcileService(ctx context.Context, http *transportv1alpha1.HTTP) (*corev1.Service, error) {
	log := log.FromContext(ctx)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      http.Name,
			Namespace: http.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     http.Name,
				"app.kubernetes.io/instance": http.Name,
				"app.kubernetes.io/part-of":  "http-transport",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app.kubernetes.io/name":     http.Name,
				"app.kubernetes.io/instance": http.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					TargetPort: intstr.FromInt(8080),
					Protocol:   corev1.ProtocolTCP,
					Name:       "http",
				},
			},
			Type: corev1.ServiceTypeClusterIP,
		},
	}

	// Set HTTP instance as the owner and controller
	if err := ctrl.SetControllerReference(http, service, r.Scheme); err != nil {
		return nil, err
	}

	// Create or update the Service
	foundService := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, foundService)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating a new Service", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
		err = r.Create(ctx, service)
		if err != nil {
			log.Error(err, "Failed to create new Service", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
			return nil, err
		}
		return service, nil
	} else if err != nil {
		log.Error(err, "Failed to get Service")
		return nil, err
	}

	// Update the found Service with new spec (preserving the ClusterIP)
	clusterIP := foundService.Spec.ClusterIP
	foundService.Spec = service.Spec
	foundService.Spec.ClusterIP = clusterIP

	log.Info("Updating Service", "Service.Namespace", foundService.Namespace, "Service.Name", foundService.Name)
	err = r.Update(ctx, foundService)
	if err != nil {
		log.Error(err, "Failed to update Service", "Service.Namespace", foundService.Namespace, "Service.Name", foundService.Name)
		return nil, err
	}

	return foundService, nil
}

// findProjectGateway finds the appropriate project gateway for the HTTP transport
func (r *HTTPReconciler) findProjectGateway(ctx context.Context, http *transportv1alpha1.HTTP) (string, error) {
	log := log.FromContext(ctx)

	// Look for any gateway in the namespace
	gatewayList := &istiov1beta1.GatewayList{}
	if err := r.List(ctx, gatewayList, client.InNamespace(http.Namespace)); err != nil {
		return "", err
	}

	// Get the namespace to check for project label/annotation
	namespace := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: http.Namespace}, namespace); err != nil {
		log.Error(err, "Failed to get namespace", "namespace", http.Namespace)
		// Continue without the namespace info
	} else {
		// Check if namespace has bubu.sh/project label
		if projectName, ok := namespace.Labels["bubu.sh/project"]; ok && projectName != "" {
			log.Info("Found project from namespace label", "project", projectName)

			// Look for a gateway with that project name in labels
			for _, gw := range gatewayList.Items {
				if gwProjectName, ok := gw.Labels["bubu.sh/project"]; ok && gwProjectName == projectName {
					gatewayRef := fmt.Sprintf("%s/%s", gw.Namespace, gw.Name)
					log.Info("Found gateway for namespace project label",
						"gateway", gatewayRef,
						"project", projectName)
					return gatewayRef, nil
				}
			}

			// Use project name to construct standard gateway name
			// Use hyphenated version of project name for the gateway name
			projectNameHyphenated := strings.ReplaceAll(projectName, " ", "-")
			projectNameHyphenated = strings.ToLower(projectNameHyphenated)
			gatewayRef := fmt.Sprintf("%s/%s-gateway", http.Namespace, projectNameHyphenated)
			log.Info("Using standard gateway name pattern for namespace project",
				"gateway", gatewayRef,
				"project", projectName)
			return gatewayRef, nil
		}
	}

	// First try to find a gateway with project label
	for _, gw := range gatewayList.Items {
		if projectName, ok := gw.Labels["bubu.sh/project"]; ok {
			gatewayRef := fmt.Sprintf("%s/%s", gw.Namespace, gw.Name)
			log.Info("Found gateway with bubu.sh/project label",
				"gateway", gatewayRef,
				"project", projectName)
			return gatewayRef, nil
		}
	}

	// Then try to find any gateway with standard labels
	for _, gw := range gatewayList.Items {
		if _, ok := gw.Labels["app.kubernetes.io/part-of"]; ok {
			gatewayRef := fmt.Sprintf("%s/%s", gw.Namespace, gw.Name)
			log.Info("Found gateway with app.kubernetes.io/part-of label", "gateway", gatewayRef)
			return gatewayRef, nil
		}
	}

	// As a fallback, use any gateway found in the namespace
	if len(gatewayList.Items) > 0 {
		gatewayRef := fmt.Sprintf("%s/%s", gatewayList.Items[0].Namespace, gatewayList.Items[0].Name)
		log.Info("Found gateway in namespace as fallback", "gateway", gatewayRef)
		return gatewayRef, nil
	}

	// No gateways found - log a warning
	log.Info("No gateway found in namespace, VirtualService will be created but may not work without a gateway",
		"namespace", http.Namespace,
		"transport", http.Name)

	// Return a placeholder that might work if a gateway exists with this name
	// or is created later
	return fmt.Sprintf("%s/istio-ingressgateway", http.Namespace), nil
}

// reconcileVirtualService creates or updates an Istio VirtualService for the HTTP transport
func (r *HTTPReconciler) reconcileVirtualService(ctx context.Context, http *transportv1alpha1.HTTP, serviceName string) (*istiov1beta1.VirtualService, error) {
	log := log.FromContext(ctx)

	// Find the appropriate gateway
	gatewayRef, err := r.findProjectGateway(ctx, http)
	if err != nil {
		log.Error(err, "Failed to find project gateway")
		// Fall back to a default if needed
		gatewayRef = fmt.Sprintf("%s/project-sample-gateway", http.Namespace)
	}

	// Create VirtualService to route to our HTTP transport
	vs := &istiov1beta1.VirtualService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      http.Name + "-virtualservice",
			Namespace: http.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     http.Name,
				"app.kubernetes.io/instance": http.Name,
				"app.kubernetes.io/part-of":  "http-transport",
			},
		},
		Spec: networkingv1alpha3.VirtualService{
			Hosts: []string{"*"},
			Gateways: []string{
				gatewayRef, // Use the discovered gateway
			},
			Http: []*networkingv1alpha3.HTTPRoute{
				{
					Match: []*networkingv1alpha3.HTTPMatchRequest{
						{
							Uri: &networkingv1alpha3.StringMatch{
								MatchType: &networkingv1alpha3.StringMatch_Prefix{
									Prefix: http.Spec.Path,
								},
							},
						},
					},
					Route: []*networkingv1alpha3.HTTPRouteDestination{
						{
							Destination: &networkingv1alpha3.Destination{
								Host: fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, http.Namespace),
								Port: &networkingv1alpha3.PortSelector{
									Number: 80,
								},
							},
						},
					},
				},
			},
		},
	}

	// Set HTTP instance as the owner and controller
	if err := ctrl.SetControllerReference(http, vs, r.Scheme); err != nil {
		return nil, err
	}

	// Create or update the VirtualService
	foundVS := &istiov1beta1.VirtualService{}
	err = r.Get(ctx, types.NamespacedName{Name: vs.Name, Namespace: vs.Namespace}, foundVS)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating a new VirtualService", "VirtualService.Namespace", vs.Namespace, "VirtualService.Name", vs.Name)
		err = r.Create(ctx, vs)
		if err != nil {
			log.Error(err, "Failed to create new VirtualService", "VirtualService.Namespace", vs.Namespace, "VirtualService.Name", vs.Name)
			return nil, err
		}
		return vs, nil
	} else if err != nil {
		log.Error(err, "Failed to get VirtualService")
		return nil, err
	}

	// Update the found VirtualService's fields individually to avoid issues with the protobuf implementation
	foundVS.ObjectMeta.Labels = vs.ObjectMeta.Labels
	foundVS.Spec.Hosts = vs.Spec.Hosts
	foundVS.Spec.Gateways = vs.Spec.Gateways
	foundVS.Spec.Http = vs.Spec.Http

	log.Info("Updating VirtualService", "VirtualService.Namespace", foundVS.Namespace, "VirtualService.Name", foundVS.Name)
	err = r.Update(ctx, foundVS)
	if err != nil {
		log.Error(err, "Failed to update VirtualService", "VirtualService.Namespace", foundVS.Namespace, "VirtualService.Name", foundVS.Name)
		return nil, err
	}

	return foundVS, nil
}

// updateStatus updates the HTTP resource status with retry logic to handle conflicts
func (r *HTTPReconciler) updateStatus(ctx context.Context, http *transportv1alpha1.HTTP, deployment *appsv1.Deployment) error {
	log := log.FromContext(ctx)

	// Try up to 3 times to update the status
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		// Refresh the HTTP object before trying to update status
		if i > 0 {
			current := &transportv1alpha1.HTTP{}
			if err := r.Get(ctx, types.NamespacedName{Name: http.Name, Namespace: http.Namespace}, current); err != nil {
				return err
			}
			// Only copy the status fields we care about, preserving other fields
			http = current
		}

		// Check if deployment is ready
		replicas := int32(1)
		if http.Spec.Replicas > 0 {
			replicas = http.Spec.Replicas
		}

		ready := metav1.ConditionTrue
		if deployment.Status.ReadyReplicas != replicas {
			ready = metav1.ConditionFalse
		}

		readyCondition := metav1.Condition{
			Type:               "Ready",
			Status:             ready,
			LastTransitionTime: metav1.Now(),
			Reason:             "HTTPTransportReady",
			Message:            "HTTP transport infrastructure is ready",
		}

		service := &corev1.Service{}
		err := r.Get(ctx, types.NamespacedName{Name: http.Name, Namespace: http.Namespace}, service)
		if err != nil {
			return err
		}

		endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local%s", service.Name, service.Namespace, http.Spec.Path)

		http.Status.Conditions = []metav1.Condition{readyCondition}
		http.Status.Endpoint = endpoint

		// Try to update the status
		if err := r.Status().Update(ctx, http); err != nil {
			if errors.IsConflict(err) {
				log.Info("Conflict updating HTTP status, retrying", "retry", i+1)
				continue // Retry on conflict
			}
			return err // Return any other error
		}

		return nil // Successfully updated
	}

	return fmt.Errorf("failed to update HTTP status after %d attempts", maxRetries)
}

// SetupWithManager sets up the controller with the Manager.
func (r *HTTPReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&transportv1alpha1.HTTP{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&istiov1beta1.VirtualService{}).
		Complete(r)
}
