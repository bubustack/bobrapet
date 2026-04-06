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
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
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

	bootstrapruntime "github.com/bubustack/core/runtime/bootstrap"
	"github.com/bubustack/core/templating"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	policyv1alpha1 "github.com/bubustack/bobrapet/api/policy/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/internal/controller"
	catalogcontroller "github.com/bubustack/bobrapet/internal/controller/catalog"
	runscontroller "github.com/bubustack/bobrapet/internal/controller/runs"
	"github.com/bubustack/bobrapet/internal/setup"
	webhookrunsv1alpha1 "github.com/bubustack/bobrapet/internal/webhook/runs/v1alpha1"
	webhooktransportv1alpha1 "github.com/bubustack/bobrapet/internal/webhook/transport/v1alpha1"
	webhookv1alpha1 "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/observability"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

const (
	tracerServiceName = "bobrapet-operator"
	// tracerInitTimeout mirrors the OTLP exporter default timeout (10s).
	tracerInitTimeout     = 10 * time.Second
	tracerShutdownTimeout = 5 * time.Second
	// initialConfigLoadTimeout bounds how long the operator waits for the operator
	// ConfigMap to be readable during startup before falling back to defaults.
	initialConfigLoadTimeout = 15 * time.Second
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(bubushv1alpha1.AddToScheme(scheme))
	utilruntime.Must(runsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(catalogv1alpha1.AddToScheme(scheme))
	utilruntime.Must(transportv1alpha1.AddToScheme(scheme))
	utilruntime.Must(policyv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// operatorFlags holds all command-line flag values for the operator.
type operatorFlags struct {
	metricsAddr             string
	metricsCertPath         string
	metricsCertName         string
	metricsCertKey          string
	webhookCertPath         string
	webhookCertName         string
	webhookCertKey          string
	enableLeaderElection    bool
	leaderElectionID        string
	leaderElectionNamespace string
	probeAddr               string
	secureMetrics           bool
	enableHTTP2             bool
	operatorConfigNamespace string
	operatorConfigName      string
	tracingInitTimeout      time.Duration
	tracingShutdownTimeout  time.Duration
}

func main() {
	var flags operatorFlags

	flag.StringVar(&flags.metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service. "+
		"When enabling metrics, pair with --metrics-secure: use :8443 with --metrics-secure=true (default) "+
		"for authenticated HTTPS, or :8080 with --metrics-secure=false for unauthenticated HTTP.")
	flag.StringVar(&flags.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&flags.enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&flags.leaderElectionID, "leader-election-id", "d3a8b358.bubustack.io",
		"Leader election ID to prevent clashes between controllers using the same namespace.")
	flag.StringVar(&flags.leaderElectionNamespace, "leader-election-namespace", "",
		"Namespace for leader election resources. If empty, uses the in-cluster namespace "+
			"and will fail when running out of cluster.")
	flag.BoolVar(&flags.secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS with authentication and authorization. "+
			"Has no effect when --metrics-bind-address=0 (metrics disabled). "+
			"Use --metrics-secure=false together with --metrics-bind-address=:8080 for unauthenticated HTTP metrics.")
	flag.StringVar(&flags.webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&flags.webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&flags.webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&flags.metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&flags.metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.") //nolint:lll
	flag.StringVar(&flags.metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&flags.enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")

	// Operator configuration flags (similar to kube-controller-manager --kubeconfig pattern)
	flag.StringVar(&flags.operatorConfigNamespace, "config-namespace", "bobrapet-system",
		"The namespace where the operator configuration ConfigMap resides.")
	flag.StringVar(&flags.operatorConfigName, "config-name", "bobrapet-operator-config",
		"The name of the operator configuration ConfigMap.")
	flag.DurationVar(&flags.tracingInitTimeout, "tracing-init-timeout", tracerInitTimeout,
		"Timeout for initializing the OTLP tracer exporter. Use 0 to keep the default.")
	flag.DurationVar(&flags.tracingShutdownTimeout, "tracing-shutdown-timeout", tracerShutdownTimeout,
		"Timeout for flushing/shutting down the tracer provider on exit. Use 0 to keep the default.")

	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	if err := run(flags); err != nil {
		// A context.Canceled error means the OS sent SIGTERM/SIGINT while the
		// operator was still initializing (e.g. during the initial ConfigMap
		// load). This is a graceful signal-induced shutdown, not a crash — exit
		// cleanly so Kubernetes does not mark the pod as failed.
		if errors.Is(err, context.Canceled) {
			os.Exit(0)
		}
		setupLog.Error(err, "startup failed")
		os.Exit(1)
	}
}

// run executes the full operator lifecycle and returns an error on failure.
//
// Separating startup logic into run() rather than inlining it in main() ensures
// that all deferred cleanup functions (OTEL tracer shutdown, template evaluator
// close) execute on every exit path. Calling os.Exit directly from main() after
// defers are registered would bypass them, silently skipping resource shutdown on
// late-stage startup failures (e.g. controller registration, webhook registration).
func run(flags operatorFlags) error {
	tlsOpts := configureHTTP2(flags.enableHTTP2)

	enableWebhooks, enableWebhooksRaw := shouldEnableWebhooks()

	traceInitTimeout, traceShutdownTimeout := normalizeTracingTimeouts(
		flags.tracingInitTimeout,
		flags.tracingShutdownTimeout,
	)

	// Configure tracing defaults before applying runtime toggles.
	observability.ConfigureTracing(tracerServiceName, traceInitTimeout, traceShutdownTimeout)

	// Build servers' options
	var webhookServer webhook.Server
	if enableWebhooks {
		webhookServer = webhook.NewServer(
			buildWebhookServerOptions(
				flags.webhookCertPath,
				flags.webhookCertName,
				flags.webhookCertKey,
				tlsOpts,
			),
		)
	} else {
		webhookServer = newNoopWebhookServer()
	}

	metricsServerOptions := buildMetricsServerOptions(
		flags.metricsAddr,
		flags.secureMetrics,
		flags.metricsCertPath,
		flags.metricsCertName,
		flags.metricsCertKey,
		tlsOpts,
	)

	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("unable to get kubeconfig: %w", err)
	}
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsServerOptions,
		WebhookServer:           webhookServer,
		HealthProbeBindAddress:  flags.probeAddr,
		LeaderElection:          flags.enableLeaderElection,
		LeaderElectionID:        flags.leaderElectionID,
		LeaderElectionNamespace: flags.leaderElectionNamespace,
		// LeaderElectionReleaseOnCancel causes the leader to release its lease
		// immediately upon receiving a shutdown signal, rather than holding it
		// until the TTL expires (~15 s). This reduces failover delay in HA
		// deployments at the cost of a potential brief period where no leader
		// is active during a graceful restart.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("unable to start manager: %w", err)
	}

	managerCtx := ctrl.SetupSignalHandler()

	contractLogger := bootstrapruntime.NewContractLogger(setupLog.WithName("bootstrap"), "manager")

	operatorConfigManager, controllerConfig, configResolver, templateEvaluator, err := mustInitOperatorServices(
		managerCtx,
		mgr,
		flags.operatorConfigNamespace,
		flags.operatorConfigName,
		contractLogger.WithComponent("operator-services"),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize operator services: %w", err)
	}

	// Register cleanup in shutdown order (LIFO):
	//   1. OTEL tracer shutdown must run last — register its defer first (outermost).
	//      Shutdown is a no-op if tracing was never enabled.
	//   2. templateEvaluator.Close must run first — register its defer second
	//      (innermost) so any spans emitted during Close are flushed before the OTEL
	//      exporter itself shuts down.
	//
	// Note: when TelemetryEnabled is true, a third defer (cancel for initCtx,
	// registered below inside the if-block) runs before templateEvaluator.Close.
	// Cancelling initCtx at that point is a no-op: traceInitTimeout has already
	// elapsed, and the tracer provider lifecycle is independent of initCtx once
	// InitTracerProvider returns.
	//
	// Both defers are intentionally registered before the tracer provider init step.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), traceShutdownTimeout)
		defer cancel()
		if err := observability.ShutdownTracerProvider(shutdownCtx); err != nil {
			setupLog.Error(err, "failed to shutdown OTEL tracer provider")
		}
	}()
	defer templateEvaluator.Close()

	// InitTracerProvider must use context.Background() rather than managerCtx.
	// managerCtx is cancelled when a shutdown signal arrives; if it were passed
	// here, any in-flight OTLP init work could be interrupted mid-startup.
	if controllerConfig.TelemetryEnabled {
		initCtx, cancel := context.WithTimeout(context.Background(), traceInitTimeout)
		defer cancel()
		if _, err := observability.InitTracerProvider(initCtx, tracerServiceName); err != nil {
			return fmt.Errorf("failed to initialize OTEL tracer provider: %w", err)
		}
	}

	deps := config.ControllerDependencies{
		Client:            mgr.GetClient(),
		APIReader:         mgr.GetAPIReader(),
		Scheme:            mgr.GetScheme(),
		ConfigResolver:    configResolver,
		TemplateEvaluator: templateEvaluator,
	}

	// Register field indexes before controllers rely on indexed lookups.
	if err := setup.SetupIndexers(managerCtx, mgr); err != nil {
		setupLog.Error(err, "unable to register field indexes")
		os.Exit(1)
	}

	if err := mustSetupControllers(
		managerCtx,
		mgr,
		deps,
		controllerConfig,
		contractLogger.WithComponent("controllers"),
	); err != nil {
		return fmt.Errorf("failed to register controllers: %w", err)
	}

	if err := setupWebhooksIfEnabled(
		mgr,
		operatorConfigManager,
		contractLogger.WithComponent("webhooks"),
		enableWebhooks,
		enableWebhooksRaw,
	); err != nil {
		return fmt.Errorf("failed to register webhooks: %w", err)
	}
	// +kubebuilder:scaffold:builder

	var webhookChecker healthz.Checker
	if enableWebhooks {
		webhookChecker = mgr.GetWebhookServer().StartedChecker()
	}
	if err := mustAddHealthChecks(mgr, webhookChecker); err != nil {
		return fmt.Errorf("failed to register health checks: %w", err)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(managerCtx); err != nil {
		return fmt.Errorf("problem running manager: %w", err)
	}
	return nil
}

// configureHTTP2 returns a TLS config modifier slice that forces HTTP/1.1 when
// enableHTTP2 is false (the default), protecting against CVE-2023-44487
// (HTTP/2 Rapid Reset Attack). When enableHTTP2 is true, it returns nil,
// leaving the TLS config unchanged and allowing HTTP/2 negotiation.
func configureHTTP2(enableHTTP2 bool) []func(*tls.Config) {
	return []func(*tls.Config){
		func(c *tls.Config) {
			c.MinVersion = tls.VersionTLS12
			if !enableHTTP2 {
				setupLog.Info("disabling http/2")
				c.NextProtos = []string{"http/1.1"}
			}
		},
	}
}

func normalizeTracingTimeouts(initTimeout, shutdownTimeout time.Duration) (time.Duration, time.Duration) {
	if initTimeout <= 0 {
		initTimeout = tracerInitTimeout
	}
	if shutdownTimeout <= 0 {
		shutdownTimeout = tracerShutdownTimeout
	}
	return initTimeout, shutdownTimeout
}

type noopWebhookServer struct {
	mux *http.ServeMux
}

func newNoopWebhookServer() webhook.Server {
	return &noopWebhookServer{mux: http.NewServeMux()}
}

func (s *noopWebhookServer) NeedLeaderElection() bool { return false }

func (s *noopWebhookServer) Register(path string, hook http.Handler) {
	setupLog.V(1).Info("webhook server disabled; handler registration is a no-op", "path", path)
	if s.mux == nil {
		s.mux = http.NewServeMux()
	}
	s.mux.Handle(path, hook)
}

func (s *noopWebhookServer) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (s *noopWebhookServer) StartedChecker() healthz.Checker { return healthz.Ping }

func (s *noopWebhookServer) WebhookMux() *http.ServeMux {
	if s.mux == nil {
		s.mux = http.NewServeMux()
	}
	return s.mux
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
//     Empty string uses controller-runtime defaults (a temp-dir path); it does not
//     auto-integrate with cert-manager.
//   - certName string: filename of the TLS certificate (e.g., "tls.crt").
//   - certKey string: filename of the TLS key (e.g., "tls.key").
//   - tlsOpts []func(*tls.Config): TLS config modifiers (e.g., HTTP/2 disable).
//
// Returns:
//   - webhook.Options configured for the operator's webhook server.
func buildWebhookServerOptions(certPath, certName, certKey string, tlsOpts []func(*tls.Config)) webhook.Options {
	// Initial webhook TLS options
	opts := webhook.Options{TLSOpts: tlsOpts}
	if len(certPath) > 0 {
		setupLog.Info("initializing webhook certificate watcher using provided certificates",
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
	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	opts := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secure,
		TLSOpts:       tlsOpts,
	}
	if secure {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/metrics/filters#WithAuthenticationAndAuthorization
		opts.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// To use cert-manager for certificate management (recommended for production):
	// - Uncomment [METRICS-WITH-CERTS] in config/default/kustomization.yaml
	// - Uncomment [PROMETHEUS-WITH-CERTS] in config/prometheus/kustomization.yaml
	if len(certPath) > 0 {
		setupLog.Info("initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", certPath, "metrics-cert-name", certName, "metrics-cert-key", certKey)
		opts.CertDir = certPath
		opts.CertName = certName
		opts.KeyName = certKey
	}
	return opts
}

// mustInitOperatorServices wires the OperatorConfig manager/controller,
// performs the initial ConfigMap load with a 15s timeout, and returns the
// OperatorConfigManager, controller config snapshot, shared Resolver, and
// template evaluator used by the rest of the operator.
func mustInitOperatorServices(
	startupCtx context.Context,
	mgr ctrl.Manager,
	configNamespace string,
	configName string,
	contract bootstrapruntime.ContractLogger,
) (*config.OperatorConfigManager, *config.ControllerConfig, *config.Resolver, *templating.Evaluator, error) {
	contract.Start("init")
	operatorConfigManager, err := config.NewOperatorConfigManager(
		mgr.GetClient(),
		configNamespace,
		configName,
		mgr.GetAPIReader(),
	)
	if err != nil {
		contract.Failure("init", fmt.Errorf("create operator config manager: %w", err))
		return nil, nil, nil, nil, fmt.Errorf("create operator config manager: %w", err)
	}

	setupLog.Info("operator configuration manager initialized",
		"configNamespace", configNamespace,
		"configName", configName)

	// Setup the config manager as a reconciler (event-driven)
	if err := operatorConfigManager.SetupWithManager(mgr); err != nil {
		contract.Failure("init", fmt.Errorf("setup operator config manager controller: %w", err))
		return nil, nil, nil, nil, fmt.Errorf("setup operator config manager controller: %w", err)
	}
	setupLog.Info("operator config manager controller registered")

	loadCtx, cancel := context.WithTimeout(startupCtx, initialConfigLoadTimeout)
	defer cancel()
	contract.Start("load-config")
	loadStart := time.Now()
	loadedFromConfigMap := false
	if err := operatorConfigManager.LoadInitial(loadCtx); err != nil {
		switch {
		case apierrors.IsNotFound(err):
			setupLog.Info("operator config map not found; continuing with defaults",
				"configNamespace", configNamespace,
				"configName", configName,
				"loadDuration", time.Since(loadStart))
			contract.Success("load-config", "source", "defaults",
				"configNamespace", configNamespace, "configName", configName)
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			// Distinguish a signal interruption or an initialConfigLoadTimeout expiry from
			// a genuine configuration error so operators can diagnose shutdown races vs.
			// misconfigured ConfigMap coordinates.
			setupLog.Error(err, "interrupted while loading operator configuration",
				"configNamespace", configNamespace,
				"configName", configName)
			contract.Failure("load-config", err)
			contract.Failure("init", err)
			return nil, nil, nil, nil, fmt.Errorf("interrupted while loading operator configuration: %w", err)
		default:
			setupLog.Error(err, "failed to load operator configuration during startup",
				"configNamespace", configNamespace,
				"configName", configName)
			contract.Failure("load-config", err)
			contract.Failure("init", err)
			return nil, nil, nil, nil, fmt.Errorf("load operator configuration: %w", err)
		}
	} else {
		loadedFromConfigMap = true
		setupLog.Info("operator configuration loaded from ConfigMap",
			"configNamespace", configNamespace,
			"configName", configName,
			"loadDuration", time.Since(loadStart))
		contract.Success("load-config", "source", "configmap",
			"configNamespace", configNamespace, "configName", configName)
	}

	controllerConfig := operatorConfigManager.GetControllerConfig()
	configSource := "defaults"
	if loadedFromConfigMap {
		configSource = "configmap"
	}
	setupLog.Info("controller configuration loaded", "source", configSource)
	if !loadedFromConfigMap {
		// ApplyRuntimeToggles is called by the ApplyConfig hook inside LoadInitial for
		// the configmap path. For the defaults path (ConfigMap not found), the hook is
		// never triggered, so we apply toggles explicitly here.
		config.ApplyRuntimeToggles(controllerConfig)
	}
	// Always log the effective toggle state for startup observability, regardless of
	// whether config was loaded from ConfigMap or defaults.
	setupLog.Info("runtime toggles applied",
		"source", configSource,
		"telemetryEnabled", controllerConfig.TelemetryEnabled,
		"tracePropagationEnabled", controllerConfig.TracePropagationEnabled,
		"verboseLogging", controllerConfig.EnableVerboseLogging,
		"stepOutputLogging", controllerConfig.EnableStepOutputLogging,
		"metricsEnabled", controllerConfig.EnableMetrics,
	)
	configResolver := config.NewResolver(mgr.GetClient(), operatorConfigManager)
	setupLog.Info("configuration resolver initialized")
	templateCfg := templating.Config{
		EvaluationTimeout: controllerConfig.TemplateEvaluationTimeout,
		MaxOutputBytes:    controllerConfig.TemplateMaxOutputBytes,
		Deterministic:     controllerConfig.TemplateDeterministic,
	}
	templateEvaluator, err := templating.New(templateCfg)
	if err != nil {
		setupLog.Error(err, "unable to create template evaluator")
		contract.Failure("init", err)
		return nil, nil, nil, nil, fmt.Errorf("create template evaluator: %w", err)
	}
	contract.Success("init",
		"configSource", configSource,
		"configNamespace", configNamespace,
		"configName", configName,
	)
	return operatorConfigManager, controllerConfig, configResolver, templateEvaluator, nil
}

// mustSetupControllers registers every reconciling controller with the
// manager using the controller-specific options from ControllerConfig and
// returns an error if any SetupWithManager call fails.
//
// Each controller's declared indexes are registered via setup.EnsureIndexes
// before the controller itself is started. EnsureIndexes is idempotent: the
// internal sync.Map deduplicates re-registration across controllers that share
// the same index. Index registration failures are fatal; a controller must not
// start without the indexes it relies on for efficient lookups.
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
			return fmt.Errorf("controller %s: %w", name, err)
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
			indexes: []setup.IndexRegistration{
				// IndexStepRunStoryRunRef indexes StepRuns by parent StoryRun name so
				// the StoryRun controller can list all steps for a given run.
				setup.IndexStepRunStoryRunRef,
				// IndexStepRunPhase lets DAG concurrency checks count only running steps.
				setup.IndexStepRunPhase,
				// IndexStoryRunStoryRefName indexes StoryRuns by parent Story name.
				setup.IndexStoryRunStoryRefName,
			},
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
				// IndexStepRunStoryRunRef also required by StepRun for parent-run
				// lookups; idempotent if already registered by StoryRun above.
				setup.IndexStepRunStoryRunRef,
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
				// Description and Version indexes allow efficient catalog lookups
				// by metadata fields (e.g. UI template browser, version pinning).
				setup.IndexEngramTemplateDescription,
				setup.IndexEngramTemplateVersion,
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
				// IndexImpulseTemplateRef indexes Impulses by spec.templateRef.name so the
				// ImpulseTemplate controller can list dependent Impulses for usage counts.
				setup.IndexImpulseTemplateRef,
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
				setup.IndexTransportBindingTransportRef,
			},
			setup: func() error {
				return (&controller.TransportReconciler{
					ControllerDependencies: deps,
				}).SetupWithManager(mgr, controllerConfig.BuildTransportControllerOptions())
			},
		},
		{
			name: "StoryTrigger",
			setup: func() error {
				return (&runscontroller.StoryTriggerReconciler{
					Client:         mgr.GetClient(),
					APIReader:      mgr.GetAPIReader(),
					Scheme:         mgr.GetScheme(),
					ConfigResolver: deps.ConfigResolver,
				}).SetupWithManager(mgr, controllerConfig.BuildStoryTriggerControllerOptions())
			},
		},
		{
			name: "EffectClaim",
			setup: func() error {
				return (&runscontroller.EffectClaimReconciler{
					Client:    mgr.GetClient(),
					APIReader: mgr.GetAPIReader(),
					Scheme:    mgr.GetScheme(),
				}).SetupWithManager(mgr, controllerConfig.BuildEffectClaimControllerOptions())
			},
		},
	}

	// register() invokes its closure synchronously, so ctr is safe to capture
	// directly — the closure never outlives the current iteration.
	for _, ctr := range controllers {
		if err := register(ctr.name, func() error {
			if len(ctr.indexes) > 0 {
				if err := setup.EnsureIndexes(ctx, mgr, ctr.indexes); err != nil {
					return err
				}
			}
			return ctr.setup()
		}); err != nil {
			return err
		}
	}
	setup.LogRegisteredIndexes()
	return nil
}

// setupWebhooksIfEnabled registers every admission webhook unless ENABLE_WEBHOOKS
// is explicitly set to "false".
//
// Config consistency note:
//   - StoryWebhook, StoryRunWebhook, StoryTriggerWebhook, EffectClaimWebhook,
//     StepRunWebhook, EngramWebhook, and ImpulseWebhook hold both a point-in-time
//     ControllerConfig snapshot and a live ConfigManager reference, so they
//     re-fetch the effective config on each admission request.
//   - TransportWebhook and TransportBindingWebhook are schema-only validators with no
//     ControllerConfig dependency; they hold neither a snapshot nor a ConfigManager.
func setupWebhooksIfEnabled(
	mgr ctrl.Manager,
	operatorConfigManager *config.OperatorConfigManager,
	contract bootstrapruntime.ContractLogger,
	enableWebhooks bool,
	rawValue string,
) error {
	if !enableWebhooks {
		setupLog.Info("skipping webhook setup", "enableWebhooks", rawValue, "reason", "env-disabled")
		return nil
	}
	setupLog.Info("setting up webhooks", "enableWebhooks", rawValue)
	contract.Start("register")

	register := func(name string, setupFn func() error) error {
		component := contract.WithComponent(name)
		component.Start("register")
		if err := setupFn(); err != nil {
			component.Failure("register", err)
			return fmt.Errorf("webhook %s: %w", name, err)
		}
		component.Success("register")
		return nil
	}

	// Obtain a single ControllerConfig snapshot. Webhooks with a ConfigManager
	// field re-fetch the effective config on each admission request (see config
	// consistency note in the function doc).
	controllerCfg := operatorConfigManager.GetControllerConfig()

	webhooks := []struct {
		name  string
		setup func() error
	}{
		{
			name: "Story",
			setup: func() error {
				return (&webhookv1alpha1.StoryWebhook{
					Config:        controllerCfg,
					ConfigManager: operatorConfigManager,
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "Engram",
			setup: func() error {
				return (&webhookv1alpha1.EngramWebhook{
					Config:        controllerCfg,
					ConfigManager: operatorConfigManager,
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "Impulse",
			setup: func() error {
				return (&webhookv1alpha1.ImpulseWebhook{
					Config:        controllerCfg,
					ConfigManager: operatorConfigManager,
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "StoryRun",
			setup: func() error {
				return (&webhookrunsv1alpha1.StoryRunWebhook{
					Config:        controllerCfg,
					ConfigManager: operatorConfigManager,
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "StepRun",
			setup: func() error {
				return (&webhookrunsv1alpha1.StepRunWebhook{
					Config:        controllerCfg,
					ConfigManager: operatorConfigManager,
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "StoryTrigger",
			setup: func() error {
				return (&webhookrunsv1alpha1.StoryTriggerWebhook{
					Config:        controllerCfg,
					ConfigManager: operatorConfigManager,
				}).SetupWebhookWithManager(mgr)
			},
		},
		{
			name: "EffectClaim",
			setup: func() error {
				return (&webhookrunsv1alpha1.EffectClaimWebhook{
					Config:        controllerCfg,
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
			contract.Failure("register", err)
			return err
		}
	}
	// contract.Success is called only after all webhooks have registered
	// successfully. It is not deferred to avoid falsely reporting success when
	// any registration in the loop above returns an error.
	contract.Success("register")
	return nil
}

func shouldEnableWebhooks() (bool, string) {
	raw := os.Getenv("ENABLE_WEBHOOKS")
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return true, trimmed
	}
	enabled, err := strconv.ParseBool(trimmed)
	if err != nil {
		setupLog.Error(err, "invalid ENABLE_WEBHOOKS value; defaulting to enabled", "enableWebhooks", trimmed)
		return true, trimmed
	}
	return enabled, trimmed
}

// mustAddHealthChecks registers the liveness and readiness probes with the manager.
func mustAddHealthChecks(mgr ctrl.Manager, webhookChecker healthz.Checker) error {
	runner := bootstrapruntime.Runner{Log: setupLog.WithName("healthchecks")}
	entries := []bootstrapruntime.Entry{
		{
			Kind:           "health",
			Name:           "healthz",
			ErrMessage:     "unable to set up health check",
			SuccessMessage: "health check registered",
			Register: func() error {
				return mgr.AddHealthzCheck("healthz", healthz.Ping)
			},
		},
		{
			Kind:           "health",
			Name:           "readyz",
			ErrMessage:     "unable to set up ready check",
			SuccessMessage: "ready check registered",
			Register: func() error {
				return mgr.AddReadyzCheck("readyz", healthz.Ping)
			},
		},
	}
	if webhookChecker != nil {
		entries = append(entries, bootstrapruntime.Entry{
			Kind:           "health",
			Name:           "webhook-ready",
			ErrMessage:     "unable to set up webhook readiness check",
			SuccessMessage: "webhook readiness check registered",
			Register: func() error {
				return mgr.AddReadyzCheck("webhook", webhookChecker)
			},
		})
	}
	return runner.Register(entries...)
}
