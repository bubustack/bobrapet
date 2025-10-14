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

package main

import (
	"crypto/tls"
	"flag"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	config "github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/internal/controller"
	setup "github.com/bubustack/bobrapet/internal/setup"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/logging"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	catalogcontroller "github.com/bubustack/bobrapet/internal/controller/catalog"
	runscontroller "github.com/bubustack/bobrapet/internal/controller/runs"
	webhookrunsv1alpha1 "github.com/bubustack/bobrapet/internal/webhook/runs/v1alpha1"
	webhookv1alpha1 "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(bubushv1alpha1.AddToScheme(scheme))
	utilruntime.Must(runsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(catalogv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.1/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.1/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "d3a8b358.bubustack.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Index Fields for efficient lookups using manager lifecycle context
	managerCtx := ctrl.SetupSignalHandler()
	setup.SetupIndexers(managerCtx, mgr)

	operatorConfigManager := config.NewOperatorConfigManager(mgr.GetClient(), "bobrapet-system", "bobrapet-operator-config")
	setupLog.Info("Operator configuration manager initialized")
	if err := mgr.Add(operatorConfigManager); err != nil {
		setupLog.Error(err, "unable to add operator config manager to manager")
		os.Exit(1)
	}

	controllerConfig := operatorConfigManager.GetControllerConfig()
	setupLog.Info("Controller configuration loaded")
	configResolver := config.NewResolver(mgr.GetClient(), operatorConfigManager)
	setupLog.Info("Configuration resolver initialized")
	celLogger := logging.NewCELLogger(ctrl.Log)
	celEvaluator, err := cel.New(celLogger)
	if err != nil {
		setupLog.Error(err, "unable to create CEL evaluator")
		os.Exit(1)
	}

	deps := config.ControllerDependencies{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		ConfigResolver: configResolver,
		CELEvaluator:   *celEvaluator,
	}

	if err := (&controller.StoryReconciler{
		ControllerDependencies: deps,
	}).SetupWithManager(mgr, controllerConfig.BuildStoryControllerOptions()); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Story")
		os.Exit(1)
	}
	if err := (&controller.EngramReconciler{
		ControllerDependencies: deps,
	}).SetupWithManager(mgr, controllerConfig.BuildEngramControllerOptions()); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Engram")
		os.Exit(1)
	}
	if err := (&controller.ImpulseReconciler{
		ControllerDependencies: deps,
	}).SetupWithManager(mgr, controllerConfig.BuildImpulseControllerOptions()); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Impulse")
		os.Exit(1)
	}
	if err := (&runscontroller.StoryRunReconciler{
		ControllerDependencies: deps,
	}).SetupWithManager(mgr, controllerConfig.BuildStoryRunControllerOptions()); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "StoryRun")
		os.Exit(1)
	}
	if err := (&runscontroller.StepRunReconciler{
		ControllerDependencies: deps,
	}).SetupWithManager(mgr, controllerConfig.BuildStepRunControllerOptions()); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "StepRun")
		os.Exit(1)
	}
	if err := (&catalogcontroller.EngramTemplateReconciler{
		ControllerDependencies: deps,
	}).SetupWithManager(mgr, controllerConfig.BuildTemplateControllerOptions()); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EngramTemplate")
		os.Exit(1)
	}
	if err := (&catalogcontroller.ImpulseTemplateReconciler{
		ControllerDependencies: deps,
	}).SetupWithManager(mgr, controllerConfig.BuildTemplateControllerOptions()); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ImpulseTemplate")
		os.Exit(1)
	}

	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		setupLog.Info("setting up webhooks")
		if err := (&webhookv1alpha1.StoryWebhook{}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "Story")
			os.Exit(1)
		}
		if err := (&webhookv1alpha1.EngramWebhook{
			Config: operatorConfigManager.GetControllerConfig(),
		}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "Engram")
			os.Exit(1)
		}
		if err := (&webhookv1alpha1.ImpulseWebhook{
			Config: operatorConfigManager.GetControllerConfig(),
		}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "Impulse")
			os.Exit(1)
		}
		if err := (&webhookrunsv1alpha1.StoryRunWebhook{
			Config: operatorConfigManager.GetControllerConfig(),
		}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "StoryRun")
			os.Exit(1)
		}
		if err := (&webhookrunsv1alpha1.StepRunWebhook{}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "StepRun")
			os.Exit(1)
		}
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	// Ensure CEL evaluator background routines stop on manager shutdown
	defer celEvaluator.Close()
	if err := mgr.Start(managerCtx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
