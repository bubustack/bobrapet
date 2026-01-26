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
	"fmt"
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

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	config "github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/internal/controller"
	catalogcontroller "github.com/bubustack/bobrapet/internal/controller/catalog"
	runscontroller "github.com/bubustack/bobrapet/internal/controller/runs"
	setup "github.com/bubustack/bobrapet/internal/setup"
	webhookrunsv1alpha1 "github.com/bubustack/bobrapet/internal/webhook/runs/v1alpha1"
	webhooktransportv1alpha1 "github.com/bubustack/bobrapet/internal/webhook/transport/v1alpha1"
	webhookv1alpha1 "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/logging"
	bootstrapruntime "github.com/bubustack/core/runtime/bootstrap"
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
	utilruntime.Must(transportv1alpha1.AddToScheme(scheme))
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
		Development: false,
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

	contractLogger := bootstrapruntime.NewContractLogger(setupLog.WithName("bootstrap"), "manager")

	operatorConfigManager, controllerConfig, configResolver, celEvaluator, err := mustInitOperatorServices(
		mgr,
		managerCtx,
		operatorConfigNamespace,
		operatorConfigName,
		contractLogger.WithComponent("operator-services"),
	)
	if err != nil {
		setupLog.Error(err, "failed to initialize operator services")
		os.Exit(1)
	}

	deps := config.ControllerDependencies{
		Client:         mgr.GetClient(),
		APIReader:      mgr.GetAPIReader(),
		Scheme:         mgr.GetScheme(),
		ConfigResolver: configResolver,
		CELEvaluator:   *celEvaluator,
	}

	if err := mustSetupControllers(
		managerCtx,
		mgr,
		deps,
		controllerConfig,
		contractLogger.WithComponent("controllers"),
	); err != nil {
		setupLog.Error(err, "failed to register controllers")
		os.Exit(1)
	}

	if err := setupWebhooksIfEnabled(mgr, operatorConfigManager, contractLogger.WithComponent("webhooks")); err != nil {
		setupLog.Error(err, "failed to register webhooks")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mustAddHealthChecks(mgr); err != nil {
		setupLog.Error(err, "failed to register health checks")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	defer celEvaluator.Close()
	if err := mgr.Start(managerCtx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// configureHTTP2 appends a TLS config modifier to disable HTTP/2 when not explicitly enabled.
//
// Behavior:
//   - When enableHTTP2 is false (the default), appends a TLS config function that
//     forces HTTP/1.1 only by setting NextProtos to ["http/1.1"].
//   - When enableHTTP2 is true, leaves tlsOpts unchanged, allowing HTTP/2.
//
// Arguments:
//   - enableHTTP2 bool: flag from --enable-http2 command line argument.
//   - tlsOpts *[]func(*tls.Config): slice of TLS config modifiers to append to.
//
// Side Effects:
//   - Logs "disabling http/2" when the disableHTTP2 modifier is invoked.
//
// Notes:
//   - HTTP/2 is disabled by default due to known vulnerabilities (CVE-2023-44487).
func configureHTTP2(enableHTTP2 bool, tlsOpts *[]func(*tls.Config)) {
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}
	if !enableHTTP2 {
		*tlsOpts = append(*tlsOpts, disableHTTP2)
	}
}

// buildWebhookServerOptions constructs webhook.Options for the controller-runtime webhook server.
//
// Behavior:
//   - Always applies the provided TLS options to the webhook server.
//   - When certPath is non-empty, configures the webhook server to watch for
//     certificate files at the specified path, enabling automatic cert rotation.
//
// Arguments:
//   - certPath string: directory containing the webhook TLS certificate and key.
//     Empty string uses controller-runtime's default cert-manager integration.
//   - certName string: filename of the TLS certificate (e.g., "tls.crt").
//   - certKey string: filename of the TLS key (e.g., "tls.key").
//   - tlsOpts []func(*tls.Config): TLS config modifiers (e.g., HTTP/2 disable).
//
// Returns:
//   - webhook.Options configured for the operator's webhook server.
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

// buildMetricsServerOptions constructs metricsserver.Options for the controller-runtime metrics server.
//
// Behavior:
//   - Configures the metrics server bind address and secure serving mode.
//   - When secure is true, enables authentication and authorization via
//     filters.WithAuthenticationAndAuthorization.
//   - When certPath is non-empty, configures the metrics server to watch for
//     certificate files at the specified path, enabling automatic cert rotation.
//
// Arguments:
//   - metricsAddr string: address to bind the metrics endpoint (e.g., ":8443").
//   - secure bool: whether to serve metrics over HTTPS with auth.
//   - certPath string: directory containing the TLS certificate and key.
//   - certName string: filename of the TLS certificate (e.g., "tls.crt").
//   - certKey string: filename of the TLS key (e.g., "tls.key").
//   - tlsOpts []func(*tls.Config): TLS config modifiers (e.g., HTTP/2 disable).
//
// Returns:
//   - metricsserver.Options configured for the operator's metrics endpoint.
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

// mustInitOperatorServices wires the OperatorConfig manager/controller,
// performs the initial ConfigMap load with a 15s timeout, and returns the
// controller config, shared Resolver, and CEL evaluator used by the rest of
// the operator (../bobrapet/cmd/main.go:239-299).
func mustInitOperatorServices(
	mgr ctrl.Manager,
	startupCtx context.Context,
	configNamespace string,
	configName string,
	contract bootstrapruntime.ContractLogger,
) (*config.OperatorConfigManager, *config.ControllerConfig, *config.Resolver, *cel.Evaluator, error) {
	contract.Start("init")
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
		contract.Failure("register", fmt.Errorf("setup operator config manager controller: %w", err))
		return nil, nil, nil, nil, fmt.Errorf("setup operator config manager controller: %w", err)
	}
	setupLog.Info("Operator config manager controller registered")

	loadCtx, cancel := context.WithTimeout(startupCtx, 15*time.Second)
	defer cancel()
	loadStart := time.Now()
	configSource := "defaults"
	if err := operatorConfigManager.LoadInitial(loadCtx); err != nil {
		if apierrors.IsNotFound(err) {
			setupLog.Info("Operator config map not found; continuing with defaults",
				"configNamespace", configNamespace,
				"configName", configName,
				"loadDuration", time.Since(loadStart))
		} else {
			setupLog.Error(err, "failed to load operator configuration during startup",
				"configNamespace", configNamespace,
				"configName", configName)
			contract.Failure("load-config", err)
			return nil, nil, nil, nil, fmt.Errorf("load operator configuration: %w", err)
		}
	} else {
		configSource = "configmap"
		setupLog.Info("Operator configuration loaded from ConfigMap",
			"configNamespace", configNamespace,
			"configName", configName,
			"loadDuration", time.Since(loadStart))
	}

	controllerConfig := operatorConfigManager.GetControllerConfig()
	setupLog.Info("Controller configuration loaded", "source", configSource)
	config.ApplyRuntimeToggles(controllerConfig)
	setupLog.Info("Runtime toggles applied",
		"source", configSource,
		"telemetryEnabled", controllerConfig.TelemetryEnabled,
		"tracePropagationEnabled", controllerConfig.TracePropagationEnabled,
		"verboseLogging", controllerConfig.EnableVerboseLogging,
		"stepOutputLogging", controllerConfig.EnableStepOutputLogging,
		"metricsEnabled", controllerConfig.EnableMetrics,
	)
	configResolver := config.NewResolver(mgr.GetClient(), operatorConfigManager)
	setupLog.Info("Configuration resolver initialized")
	celLogger := logging.NewCELLogger(ctrl.Log)
	celCfg := cel.Config{
		EvaluationTimeout:   controllerConfig.CELEvaluationTimeout,
		MaxExpressionLength: controllerConfig.CELMaxExpressionLength,
		EnableMacros:        &controllerConfig.CELEnableMacros,
	}
	celEvaluator, err := cel.New(celLogger, celCfg)
	if err != nil {
		setupLog.Error(err, "unable to create CEL evaluator")
		contract.Failure("init", err)
		return nil, nil, nil, nil, fmt.Errorf("create CEL evaluator: %w", err)
	}
	if err := probeCELLoopVars(celEvaluator); err != nil {
		setupLog.Error(err, "CEL loop variable probe failed; loop item expressions may not evaluate")
	} else {
		setupLog.Info("CEL loop variable probe succeeded")
	}
	contract.Success("init",
		"configSource", configSource,
		"configNamespace", configNamespace,
		"configName", configName,
	)
	return operatorConfigManager, controllerConfig, configResolver, celEvaluator, nil
}

func probeCELLoopVars(eval *cel.Evaluator) error {
	if eval == nil {
		return fmt.Errorf("cel evaluator is nil")
	}
	with := map[string]any{"probe": "{{ item.url }}"}
	vars := map[string]any{
		"item": map[string]any{"url": "ok"},
	}
	_, err := eval.ResolveWithInputs(context.Background(), with, vars)
	return err
}

// mustSetupControllers registers every reconciling controller with the
// manager using the controller-specific options from ControllerConfig and
// returns an error if any SetupWithManager call fails.
func mustSetupControllers(
	ctx context.Context,
	mgr ctrl.Manager,
	deps config.ControllerDependencies,
	controllerConfig *config.ControllerConfig,
	contract bootstrapruntime.ContractLogger,
) error {
	register := func(name string, setupFn func() error) error {
		component := contract.WithComponent(name)
		component.Start("register")
		if err := setupFn(); err != nil {
			component.Failure("register", err)
			return err
		}
		component.Success("register")
		return nil
	}

	type item struct {
		name    string
		indexes []setup.IndexRegistration
		setup   func() error
	}

	controllers := []item{
		{
			name: "Story",
			indexes: []setup.IndexRegistration{
				setup.IndexStoryStepEngramRefs,
				setup.IndexStoryStepStoryRefs,
				setup.IndexStoryRunStoryRefKey,
				setup.IndexImpulseStoryRef,
				setup.IndexEngramTemplateRef,
			},
			setup: func() error {
				return (&controller.StoryReconciler{
					ControllerDependencies: deps,
				}).SetupWithManager(mgr, controllerConfig.BuildStoryControllerOptions())
			},
		},
		{
			name: "Engram",
			indexes: []setup.IndexRegistration{
				setup.IndexEngramTemplateRef,
				setup.IndexStoryStepEngramRefs,
				setup.IndexStepRunEngramRef,
			},
			setup: func() error {
				return (&controller.EngramReconciler{
					ControllerDependencies: deps,
				}).SetupWithManager(mgr, controllerConfig.BuildEngramControllerOptions())
			},
		},
		{
			name: "Impulse",
			indexes: []setup.IndexRegistration{
				setup.IndexStoryRunImpulseRef,
				setup.IndexImpulseTemplateRef,
				setup.IndexImpulseStoryRef,
			},
			setup: func() error {
				return (&controller.ImpulseReconciler{
					ControllerDependencies: deps,
				}).SetupWithManager(mgr, controllerConfig.BuildImpulseControllerOptions())
			},
		},
		{
			name: "StoryRun",
			setup: func() error {
				return (&runscontroller.StoryRunReconciler{
					ControllerDependencies: deps,
				}).SetupWithManager(mgr, controllerConfig.BuildStoryRunControllerOptions())
			},
		},
		{
			name: "StepRun",
			indexes: []setup.IndexRegistration{
				setup.IndexStepRunEngramRef,
				setup.IndexEngramTemplateRef,
			},
			setup: func() error {
				return (&runscontroller.StepRunReconciler{
					ControllerDependencies: deps,
				}).SetupWithManager(mgr, controllerConfig.BuildStepRunControllerOptions())
			},
		},
		{
			name: "EngramTemplate",
			indexes: []setup.IndexRegistration{
				setup.IndexEngramTemplateRef,
			},
			setup: func() error {
				return (&catalogcontroller.EngramTemplateReconciler{
					ControllerDependencies: deps,
				}).SetupWithManager(mgr, controllerConfig.BuildTemplateControllerOptions())
			},
		},
		{
			name: "ImpulseTemplate",
			indexes: []setup.IndexRegistration{
				setup.IndexEngramTemplateRef,
			},
			setup: func() error {
				return (&catalogcontroller.ImpulseTemplateReconciler{
					ControllerDependencies: deps,
				}).SetupWithManager(mgr, controllerConfig.BuildTemplateControllerOptions())
			},
		},
		{
			name: "Transport",
			indexes: []setup.IndexRegistration{
				setup.IndexStoryTransportRefs,
			},
			setup: func() error {
				return (&controller.TransportReconciler{
					ControllerDependencies: deps,
				}).SetupWithManager(mgr, controllerConfig.BuildTransportControllerOptions())
			},
		},
	}

	for _, ctr := range controllers {
		controllerEntry := ctr
		if err := register(controllerEntry.name, func() error {
			if len(controllerEntry.indexes) > 0 {
				if err := setup.EnsureIndexes(ctx, mgr, controllerEntry.indexes); err != nil {
					return err
				}
			}
			return controllerEntry.setup()
		}); err != nil {
			return err
		}
	}
	return nil
}

// setupWebhooksIfEnabled registers every admission webhook unless ENABLE_WEBHOOKS
// is explicitly set to "false".
func setupWebhooksIfEnabled(
	mgr ctrl.Manager,
	operatorConfigManager *config.OperatorConfigManager,
	contract bootstrapruntime.ContractLogger,
) error {
	enableWebhooksEnv := os.Getenv("ENABLE_WEBHOOKS")
	if enableWebhooksEnv == "false" {
		setupLog.Info("skipping webhook setup", "enableWebhooks", enableWebhooksEnv, "reason", "env-disabled")
		return nil
	}
	setupLog.Info("setting up webhooks", "enableWebhooks", enableWebhooksEnv)
	contract.Start("register")
	defer contract.Success("register")

	register := func(name string, setupFn func() error) error {
		component := contract.WithComponent(name)
		component.Start("register")
		if err := setupFn(); err != nil {
			component.Failure("register", err)
			return err
		}
		component.Success("register")
		return nil
	}

	webhooks := []struct {
		name  string
		setup func() error
	}{
		{
			name: "Story",
			setup: func() error {
				return (&webhookv1alpha1.StoryWebhook{
					Config:        operatorConfigManager.GetControllerConfig(),
					ConfigManager: operatorConfigManager,
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "Engram",
			setup: func() error {
				return (&webhookv1alpha1.EngramWebhook{
					Config: operatorConfigManager.GetControllerConfig(),
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "Impulse",
			setup: func() error {
				return (&webhookv1alpha1.ImpulseWebhook{
					Config: operatorConfigManager.GetControllerConfig(),
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "StoryRun",
			setup: func() error {
				return (&webhookrunsv1alpha1.StoryRunWebhook{
					Config:        operatorConfigManager.GetControllerConfig(),
					ConfigManager: operatorConfigManager,
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "StepRun",
			setup: func() error {
				return (&webhookrunsv1alpha1.StepRunWebhook{
					Config:        operatorConfigManager.GetControllerConfig(),
					ConfigManager: operatorConfigManager,
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "Transport",
			setup: func() error {
				return (&webhooktransportv1alpha1.TransportWebhook{}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "TransportBinding",
			setup: func() error {
				return (&webhooktransportv1alpha1.TransportBindingWebhook{}).SetupWebhookWithManager(mgr)
			},
		},
	}

	for _, wh := range webhooks {
		if err := register(wh.name, wh.setup); err != nil {
			return err
		}
	}
	return nil
}

// mustAddHealthChecks registers the liveness and readiness probes with the manager.
func mustAddHealthChecks(mgr ctrl.Manager) error {
	runner := bootstrapruntime.Runner{Log: setupLog.WithName("healthchecks")}
	return runner.Register(
		bootstrapruntime.Entry{
			Kind:           "health",
			Name:           "healthz",
			ErrMessage:     "unable to set up health check",
			SuccessMessage: "health check registered",
			Register: func() error {
				return mgr.AddHealthzCheck("healthz", healthz.Ping)
			},
		},
		bootstrapruntime.Entry{
			Kind:           "health",
			Name:           "readyz",
			ErrMessage:     "unable to set up ready check",
			SuccessMessage: "ready check registered",
			Register: func() error {
				return mgr.AddReadyzCheck("readyz", healthz.Ping)
			},
		},
	)
}
