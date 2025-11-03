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
	"context"
	"crypto/tls"
	"flag"
	"os"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"github.com/bubustack/bobrapet/pkg/observability"

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

	// Operator configuration flags
	var operatorConfigNamespace string
	var operatorConfigName string

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

	// Operator configuration flags (similar to kube-controller-manager --kubeconfig pattern)
	flag.StringVar(&operatorConfigNamespace, "config-namespace", "bobrapet-system",
		"The namespace where the operator configuration ConfigMap resides.")
	flag.StringVar(&operatorConfigName, "config-name", "bobrapet-operator-config",
		"The name of the operator configuration ConfigMap.")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Configure HTTP/2 and TLS options
	configureHTTP2(enableHTTP2, &tlsOpts)

	// Build servers' options
	webhookServer := webhook.NewServer(
		buildWebhookServerOptions(
			webhookCertPath,
			webhookCertName,
			webhookCertKey,
			tlsOpts,
		),
	)

	metricsServerOptions := buildMetricsServerOptions(
		metricsAddr,
		secureMetrics,
		metricsCertPath,
		metricsCertName,
		metricsCertKey,
		tlsOpts,
	)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "d3a8b358.bubustack.io",
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Index Fields for efficient lookups using manager lifecycle context
	managerCtx := ctrl.SetupSignalHandler()
	setup.SetupIndexers(managerCtx, mgr)

	operatorConfigManager, controllerConfig, configResolver, celEvaluator := mustInitOperatorServices(
		mgr,
		managerCtx,
		operatorConfigNamespace,
		operatorConfigName,
	)

	deps := config.ControllerDependencies{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		ConfigResolver: configResolver,
		CELEvaluator:   *celEvaluator,
	}

	mustSetupControllers(mgr, deps, controllerConfig)

	setupWebhooksIfEnabled(mgr, operatorConfigManager)
	// +kubebuilder:scaffold:builder

	mustAddHealthChecks(mgr)

	setupLog.Info("starting manager")
	defer celEvaluator.Close()
	if err := mgr.Start(managerCtx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func configureHTTP2(enableHTTP2 bool, tlsOpts *[]func(*tls.Config)) {
	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities.
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}
	if !enableHTTP2 {
		*tlsOpts = append(*tlsOpts, disableHTTP2)
	}
}

func buildWebhookServerOptions(certPath, certName, certKey string, tlsOpts []func(*tls.Config)) webhook.Options {
	opts := webhook.Options{TLSOpts: tlsOpts}
	if len(certPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", certPath, "webhook-cert-name", certName, "webhook-cert-key", certKey)
		opts.CertDir = certPath
		opts.CertName = certName
		opts.KeyName = certKey
	}
	return opts
}

func buildMetricsServerOptions(
	metricsAddr string,
	secure bool,
	certPath, certName, certKey string,
	tlsOpts []func(*tls.Config),
) metricsserver.Options {
	opts := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secure,
		TLSOpts:       tlsOpts,
	}
	if secure {
		opts.FilterProvider = filters.WithAuthenticationAndAuthorization
	}
	if len(certPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", certPath, "metrics-cert-name", certName, "metrics-cert-key", certKey)
		opts.CertDir = certPath
		opts.CertName = certName
		opts.KeyName = certKey
	}
	return opts
}

func mustInitOperatorServices(
	mgr ctrl.Manager,
	startupCtx context.Context,
	configNamespace string,
	configName string,
) (*config.OperatorConfigManager, *config.ControllerConfig, *config.Resolver, *cel.Evaluator) {
	operatorConfigManager := config.NewOperatorConfigManager(
		mgr.GetClient(),
		configNamespace,
		configName,
	)
	operatorConfigManager.SetAPIReader(mgr.GetAPIReader())

	setupLog.Info("Operator configuration manager initialized",
		"configNamespace", configNamespace,
		"configName", configName)

	// Setup the config manager as a reconciler (event-driven)
	if err := operatorConfigManager.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to setup operator config manager controller")
		os.Exit(1)
	}
	setupLog.Info("Operator config manager controller registered")

	loadCtx, cancel := context.WithTimeout(startupCtx, 15*time.Second)
	defer cancel()
	if err := operatorConfigManager.LoadInitial(loadCtx); err != nil {
		if apierrors.IsNotFound(err) {
			setupLog.Info("Operator config map not found; continuing with defaults",
				"configNamespace", configNamespace,
				"configName", configName)
		} else {
			setupLog.Error(err, "failed to load operator configuration during startup",
				"configNamespace", configNamespace,
				"configName", configName)
			os.Exit(1)
		}
	} else {
		setupLog.Info("Operator configuration loaded from ConfigMap",
			"configNamespace", configNamespace,
			"configName", configName)
	}

	controllerConfig := operatorConfigManager.GetControllerConfig()
	setupLog.Info("Controller configuration loaded")
	config.EnableTelemetry(controllerConfig.TelemetryEnabled)
	observability.EnableTracing(controllerConfig.TelemetryEnabled)
	configResolver := config.NewResolver(mgr.GetClient(), operatorConfigManager)
	setupLog.Info("Configuration resolver initialized")
	celLogger := logging.NewCELLogger(ctrl.Log)
	celEvaluator, err := cel.New(celLogger)
	if err != nil {
		setupLog.Error(err, "unable to create CEL evaluator")
		os.Exit(1)
	}
	return operatorConfigManager, controllerConfig, configResolver, celEvaluator
}

func mustSetupControllers(
	mgr ctrl.Manager,
	deps config.ControllerDependencies,
	controllerConfig *config.ControllerConfig,
) {
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
	if err := (&controller.RealtimeEngramReconciler{
		ControllerDependencies: deps,
	}).SetupWithManager(mgr, controllerConfig.BuildEngramControllerOptions()); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "RealtimeEngram")
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
}

func setupWebhooksIfEnabled(mgr ctrl.Manager, operatorConfigManager *config.OperatorConfigManager) {
	if os.Getenv("ENABLE_WEBHOOKS") == "false" {
		return
	}
	setupLog.Info("setting up webhooks")
	if err := (&webhookv1alpha1.StoryWebhook{
		Config:        operatorConfigManager.GetControllerConfig(),
		ConfigManager: operatorConfigManager,
	}).SetupWebhookWithManager(mgr); err != nil {
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
		Config:        operatorConfigManager.GetControllerConfig(),
		ConfigManager: operatorConfigManager,
	}).SetupWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "StoryRun")
		os.Exit(1)
	}
	if err := (&webhookrunsv1alpha1.StepRunWebhook{
		Config:        operatorConfigManager.GetControllerConfig(),
		ConfigManager: operatorConfigManager,
	}).SetupWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "StepRun")
		os.Exit(1)
	}
}

func mustAddHealthChecks(mgr ctrl.Manager) {
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}
}
