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
	"strconv"
	"strings"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"go.uber.org/zap/zapcore"

	"github.com/go-logr/logr"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	bobrapetcel "github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobravoz-grpc/internal/config"
	"github.com/bubustack/bobravoz-grpc/internal/controller"
	"github.com/bubustack/bobravoz-grpc/internal/hub"
	"github.com/bubustack/bobravoz-grpc/internal/telemetry"
	podwebhook "github.com/bubustack/bobravoz-grpc/internal/webhook/pod"
	"github.com/bubustack/core/contracts"
	bootstrapruntime "github.com/bubustack/core/runtime/bootstrap"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

const (
	managerContainerName  = "manager"
	legacyControllerImage = "controller:latest"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(bubuv1alpha1.AddToScheme(scheme))
	utilruntime.Must(runsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(transportv1alpha1.AddToScheme(scheme))

	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	var hubPort int
	var connectorImage string
	var connectorImagePullPolicy string
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
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.IntVar(&hubPort, "hub-port", 9000, "The port for the gRPC hub server.")
	connectorImageDefault := os.Getenv("CONNECTOR_IMAGE")
	if connectorImageDefault == "" {
		connectorImageDefault = config.DefaultConnectorImage
	}
	flag.StringVar(
		&connectorImage,
		"connector-image",
		connectorImageDefault,
		"Container image for injected connector sidecars.",
	)
	connectorPolicyDefault := os.Getenv("CONNECTOR_IMAGE_PULL_POLICY")
	if connectorPolicyDefault == "" {
		connectorPolicyDefault = string(corev1.PullIfNotPresent)
	}
	flag.StringVar(
		&connectorImagePullPolicy,
		"connector-image-pull-policy",
		connectorPolicyDefault,
		"Image pull policy for connector sidecars.",
	)
	flag.StringVar(&operatorConfigNamespace, "config-namespace", "bobrapet-system",
		"The namespace containing the operator configuration ConfigMap.")
	flag.StringVar(&operatorConfigName, "config-name", "bobravoz-grpc-operator-config",
		"The name of the operator configuration ConfigMap.")
	opts := zap.Options{
		Development: false,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	connectorImageFlag := flag.CommandLine.Lookup("connector-image")
	connectorImageFlagOverride := connectorImageFlag != nil && connectorImageFlag.Value.String() != connectorImageFlag.DefValue
	connectorImageEnvOverride := os.Getenv("CONNECTOR_IMAGE") != ""
	connectorImageExplicit := connectorImageFlagOverride || connectorImageEnvOverride

	connectorPolicyFlag := flag.CommandLine.Lookup("connector-image-pull-policy")
	connectorPolicyFlagOverride := connectorPolicyFlag != nil && connectorPolicyFlag.Value.String() != connectorPolicyFlag.DefValue
	connectorPolicyEnvOverride := os.Getenv("CONNECTOR_IMAGE_PULL_POLICY") != ""
	connectorPolicyExplicit := connectorPolicyFlagOverride || connectorPolicyEnvOverride

	if os.Getenv(contracts.HubPortEnv) == "" {
		_ = os.Setenv(contracts.HubPortEnv, strconv.Itoa(hubPort))
	}
	if os.Getenv(contracts.HubServiceNameEnv) == "" {
		_ = os.Setenv(contracts.HubServiceNameEnv, "bobravoz-grpc-hub")
	}

	if debugLoggingEnabled() {
		opts.Development = true
		opts.Level = zapcore.DebugLevel
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog = ctrl.Log.WithName("setup")
	if debugLoggingEnabled() {
		setupLog.Info("debug logging enabled via BUBU_DEBUG")
	}

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
	// TODO(user): If you enable certManager, uncomment the following lines:
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
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "184655c9.bubustack.io",
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

	ctx := ctrl.SetupSignalHandler()

	operatorConfigManager := config.NewOperatorConfigManager(
		mgr.GetClient(),
		operatorConfigNamespace,
		operatorConfigName,
	)
	operatorConfigManager.SetAPIReader(mgr.GetAPIReader())
	if err := operatorConfigManager.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up operator config manager")
		os.Exit(1)
	}

	loadCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	if err := operatorConfigManager.LoadInitial(loadCtx); err != nil {
		if apierrors.IsNotFound(err) {
			setupLog.Info("operator config map not found; using defaults",
				"configNamespace", operatorConfigNamespace,
				"configName", operatorConfigName)
		} else {
			setupLog.Error(err, "failed to load operator configuration",
				"configNamespace", operatorConfigNamespace,
				"configName", operatorConfigName)
			cancel()
			os.Exit(1)
		}
	}
	cancel()

	cfg := operatorConfigManager.GetConfig()
	enableMacros := cfg.CEL.EnableMacros
	celCfg := bobrapetcel.Config{
		EvaluationTimeout:   cfg.CEL.EvaluationTimeout,
		MaxExpressionLength: cfg.CEL.MaxExpressionLength,
		EnableMacros:        &enableMacros,
	}

	if _, ok := os.LookupEnv(contracts.TransportSecurityModeEnv); !ok {
		mode := sanitizeSecurityMode(cfg.Hub.SecurityMode)
		if err := os.Setenv(contracts.TransportSecurityModeEnv, mode); err != nil {
			setupLog.Error(err, "failed to propagate transport security mode env from operator config")
			os.Exit(1)
		}
		setupLog.Info("hub transport security mode derived from operator config",
			"value", mode,
			"configNamespace", operatorConfigNamespace,
			"configName", operatorConfigName)
	}

	if _, ok := os.LookupEnv(contracts.TracePropagationEnv); !ok {
		if err := os.Setenv(contracts.TracePropagationEnv, strconv.FormatBool(cfg.Telemetry.TracePropagation)); err != nil {
			setupLog.Error(err, "failed to propagate trace propagation env from operator config")
			os.Exit(1)
		}
		setupLog.Info("trace propagation derived from operator config",
			"value", cfg.Telemetry.TracePropagation,
			"configNamespace", operatorConfigNamespace,
			"configName", operatorConfigName)
	}

	if err := applyHubEnvFromConfig(setupLog, cfg); err != nil {
		setupLog.Error(err, "failed to propagate hub tuning env from operator config")
		os.Exit(1)
	}

	if cfg.Connector.Image != "" && !connectorImageExplicit {
		connectorImage = cfg.Connector.Image
		setupLog.Info("connector image derived from operator config",
			"value", connectorImage,
			"configNamespace", operatorConfigNamespace,
			"configName", operatorConfigName)
	}
	if cfg.Connector.ImagePullPolicy != "" && !connectorPolicyExplicit {
		connectorImagePullPolicy = string(cfg.Connector.ImagePullPolicy)
		setupLog.Info("connector image pull policy derived from operator config",
			"value", connectorImagePullPolicy,
			"configNamespace", operatorConfigNamespace,
			"configName", operatorConfigName)
	}

	telemetry.InitFromEnv()

	allowInsecure, err := configureHubTLSFromSecret()
	if err != nil {
		setupLog.Error(err, "failed to configure hub TLS")
		os.Exit(1)
	}

	connectorPolicy := corev1.PullPolicy(connectorImagePullPolicy)
	connectorImage = strings.TrimSpace(connectorImage)
	if shouldInferConnectorImage(connectorImage) {
		if inferred, inferErr := inferConnectorImage(context.Background(), mgr.GetClient()); inferErr != nil {
			setupLog.Info("using configured connector image", "image", connectorImage, "reason", inferErr.Error())
		} else if inferred != "" {
			setupLog.Info("using manager image for connector", "image", inferred)
			connectorImage = inferred
		}
	}
	bootstrapRunner := bootstrapruntime.Runner{Log: setupLog.WithName("bootstrap")}
	if err := bootstrapRunner.Register(
		bootstrapruntime.Entry{
			Kind:           "webhook",
			Name:           "Connector",
			ErrMessage:     "unable to configure connector webhook",
			SuccessMessage: "connector webhook configured",
			Register: func() error {
				return setupConnectorWebhook(mgr, connectorImage, connectorPolicy)
			},
		},
		bootstrapruntime.Entry{
			Kind:           "controller",
			Name:           "BobravozGRPC",
			ErrMessage:     "unable to create controller",
			SuccessMessage: "controller registered",
			Fields:         []any{"controller", "BobravozGRPC"},
			Register: func() error {
				return (&controller.TransportReconciler{
					Client: mgr.GetClient(),
					Scheme: mgr.GetScheme(),
				}).SetupWithManager(mgr)
			},
		},
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
	); err != nil {
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder

	// Start the gRPC hub server
	var hubServer *hub.Server
	hubRunner := bootstrapruntime.Runner{Log: setupLog.WithName("hub")}
	if err := hubRunner.Register(
		bootstrapruntime.Entry{
			Kind:           "hub",
			Name:           "server",
			ErrMessage:     "unable to create hub server",
			SuccessMessage: "hub server initialized",
			Register: func() error {
				var serverErr error
				hubServer, serverErr = hub.NewServer(ctx, mgr.GetClient(), celCfg)
				return serverErr
			},
		},
	); err != nil {
		os.Exit(1)
	}
	defer hubServer.Close()

	setupLog.Info("=== BOBRAVOZ-GRPC IMAGE VERSION: UPDATED_WITH_AUDIOFRAME_FIX_v2 ===")
	setupLog.Info("Hub and Connector with AudioFrame passthrough enabled")

	go func() {
		if err := hubServer.Start(ctx, hubPort, allowInsecure); err != nil {
			setupLog.Error(err, "problem running hub server")
			os.Exit(1)
		}
	}()

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func configureHubTLSFromSecret() (bool, error) {
	certConfigured := ensureEnvFromFile(contracts.HubTLSCertFileEnv, hub.DefaultHubTLSCert)
	keyConfigured := ensureEnvFromFile(contracts.HubTLSKeyFileEnv, hub.DefaultHubTLSKey)
	_ = ensureEnvFromFile(contracts.HubCAFileEnv, hub.DefaultHubTLSCA)

	mode, explicitlySet, err := resolveSecurityMode()
	if err != nil {
		return false, err
	}

	tlsAssetsPresent := certConfigured && keyConfigured
	if tlsAssetsPresent && !explicitlySet {
		mode = contracts.TransportSecurityModeTLS
	}

	switch mode {
	case contracts.TransportSecurityModePlaintext:
		if tlsAssetsPresent {
			setupLog.Info("TLS assets detected but plaintext mode requested; ignoring certificates")
		} else {
			setupLog.Info(
				"Hub TLS assets not detected; running in plaintext mode (set %s=%s and mount cert/key to enforce TLS)",
				contracts.TransportSecurityModeEnv,
				contracts.TransportSecurityModeTLS,
			)
		}
		return true, nil
	case contracts.TransportSecurityModeTLS:
		if !tlsAssetsPresent {
			return false, fmt.Errorf(
				"%s=%s but %s/%s not available",
				contracts.TransportSecurityModeEnv,
				contracts.TransportSecurityModeTLS,
				contracts.HubTLSCertFileEnv,
				contracts.HubTLSKeyFileEnv,
			)
		}
		setupLog.Info(
			"Enabling hub TLS",
			"cert", os.Getenv(contracts.HubTLSCertFileEnv),
			"key", os.Getenv(contracts.HubTLSKeyFileEnv),
		)
		return false, nil
	default:
		return false, fmt.Errorf(
			"unsupported %s value %q: must be %q or %q",
			contracts.TransportSecurityModeEnv,
			mode,
			contracts.TransportSecurityModeTLS,
			contracts.TransportSecurityModePlaintext,
		)
	}
}

func ensureEnvFromFile(envKey, path string) bool {
	if envKey == "" || path == "" {
		return false
	}
	if current := os.Getenv(envKey); current != "" {
		return true
	}
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		if err := os.Setenv(envKey, path); err == nil {
			return true
		}
	}
	return false
}

func resolveSecurityMode() (string, bool, error) {
	raw, ok := os.LookupEnv(contracts.TransportSecurityModeEnv)
	if !ok {
		return contracts.TransportSecurityModeTLS, false, nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return contracts.TransportSecurityModeTLS, true, nil
	}

	switch strings.ToLower(raw) {
	case contracts.TransportSecurityModeTLS:
		return contracts.TransportSecurityModeTLS, true, nil
	case contracts.TransportSecurityModePlaintext:
		return contracts.TransportSecurityModePlaintext, true, nil
	default:
		return "", true, fmt.Errorf(
			"invalid %s value %q: must be %q or %q",
			contracts.TransportSecurityModeEnv,
			raw,
			contracts.TransportSecurityModeTLS,
			contracts.TransportSecurityModePlaintext,
		)
	}
}

func sanitizeSecurityMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case contracts.TransportSecurityModePlaintext:
		return contracts.TransportSecurityModePlaintext
	case contracts.TransportSecurityModeTLS:
		return contracts.TransportSecurityModeTLS
	default:
		return contracts.TransportSecurityModeTLS
	}
}

func applyHubEnvFromConfig(logger logr.Logger, cfg *config.OperatorConfig) error {
	if cfg == nil {
		return nil
	}
	hubCfg := cfg.Hub

	if set, err := setIntEnvIfUnset(contracts.HubBufferMaxMessagesEnv, hubCfg.BufferMaxMessages); err != nil {
		return fmt.Errorf("set %s: %w", contracts.HubBufferMaxMessagesEnv, err)
	} else if set {
		logger.Info("hub buffer max messages derived from operator config", "env", contracts.HubBufferMaxMessagesEnv, "value", hubCfg.BufferMaxMessages)
	}
	if set, err := setIntEnvIfUnset(contracts.HubBufferMaxBytesEnv, hubCfg.BufferMaxBytes); err != nil {
		return fmt.Errorf("set %s: %w", contracts.HubBufferMaxBytesEnv, err)
	} else if set {
		logger.Info("hub buffer max bytes derived from operator config", "env", contracts.HubBufferMaxBytesEnv, "value", hubCfg.BufferMaxBytes)
	}
	if set, err := setDurationEnvIfUnset(contracts.HubBufferEvictionTTLEnv, hubCfg.BufferEvictionTTL); err != nil {
		return fmt.Errorf("set %s: %w", contracts.HubBufferEvictionTTLEnv, err)
	} else if set {
		logger.Info("hub buffer TTL derived from operator config", "env", contracts.HubBufferEvictionTTLEnv, "value", hubCfg.BufferEvictionTTL)
	}
	if set, err := setDurationEnvIfUnset(contracts.HubBufferEvictionIntervalEnv, hubCfg.BufferEvictionPeriod); err != nil {
		return fmt.Errorf("set %s: %w", contracts.HubBufferEvictionIntervalEnv, err)
	} else if set {
		logger.Info("hub buffer eviction interval derived from operator config", "env", contracts.HubBufferEvictionIntervalEnv, "value", hubCfg.BufferEvictionPeriod)
	}
	if set, err := setIntEnvIfUnset(contracts.GRPCChannelBufferSizeEnv, hubCfg.ChannelBufferSize); err != nil {
		return fmt.Errorf("set %s: %w", contracts.GRPCChannelBufferSizeEnv, err)
	} else if set {
		logger.Info("hub channel buffer size derived from operator config", "env", contracts.GRPCChannelBufferSizeEnv, "value", hubCfg.ChannelBufferSize)
	}
	if set, err := setDurationEnvIfUnset(contracts.HubPerMessageTimeoutEnv, hubCfg.PerMessageTimeout); err != nil {
		return fmt.Errorf("set %s: %w", contracts.HubPerMessageTimeoutEnv, err)
	} else if set {
		logger.Info("hub per-message timeout derived from operator config", "env", contracts.HubPerMessageTimeoutEnv, "value", hubCfg.PerMessageTimeout)
	}

	return nil
}

// setIntEnvIfUnset writes env to strconv.Itoa(value) when value > 0 and the key
// is unset, returning true when it changed and surfacing os.Setenv failures so
// callers can abort startup.
func setIntEnvIfUnset(env string, value int) (bool, error) {
	if env == "" {
		setupLog.Info("skipping env override because key is empty")
		return false, nil
	}
	if value <= 0 {
		setupLog.Info("skipping env override because value is non-positive", "env", env, "value", value)
		return false, nil
	}
	if existing, exists := os.LookupEnv(env); exists {
		setupLog.Info("skipping env override because env already set", "env", env, "current", existing)
		return false, nil
	}
	if err := os.Setenv(env, strconv.Itoa(value)); err != nil {
		return false, err
	}
	return true, nil
}

// setDurationEnvIfUnset writes env to value.String() when value > 0 and the key
// is unset, returning true when it updated the environment and propagating any
// os.Setenv failure.
func setDurationEnvIfUnset(env string, value time.Duration) (bool, error) {
	if env == "" {
		setupLog.Info("skipping duration env override because key is empty")
		return false, nil
	}
	if value <= 0 {
		setupLog.Info("skipping duration env override because value is non-positive", "env", env, "value", value)
		return false, nil
	}
	if existing, exists := os.LookupEnv(env); exists {
		setupLog.Info("skipping duration env override because env already set", "env", env, "current", existing)
		return false, nil
	}
	if err := os.Setenv(env, value.String()); err != nil {
		return false, err
	}
	return true, nil
}

func setupConnectorWebhook(mgr ctrl.Manager, image string, policy corev1.PullPolicy) error {
	if os.Getenv("ENABLE_WEBHOOKS") == "false" {
		setupLog.Info("connector webhook disabled because ENABLE_WEBHOOKS=false")
		return nil
	}
	if image == "" {
		setupLog.Info("connector webhook skipped because connector-image is empty")
		return nil
	}
	webhook := podwebhook.NewConnectorWebhook(mgr.GetClient(), image, policy)
	return webhook.SetupWithManager(mgr)
}

// shouldInferConnectorImage returns true when the configured connector image is
// blank or still set to the default/legacy value, signalling that main should
// copy the manager's image instead.
func shouldInferConnectorImage(current string) bool {
	trimmed := strings.TrimSpace(current)
	if trimmed == "" {
		return true
	}
	if trimmed == config.DefaultConnectorImage {
		return true
	}
	return trimmed == legacyControllerImage
}

func inferConnectorImage(ctx context.Context, cli crclient.Client) (string, error) {
	podName := strings.TrimSpace(os.Getenv("POD_NAME"))
	podNamespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
	if podName == "" || podNamespace == "" {
		return "", fmt.Errorf("POD_NAME and POD_NAMESPACE must be set to infer connector image")
	}

	var pod corev1.Pod
	if err := cli.Get(ctx, types.NamespacedName{Name: podName, Namespace: podNamespace}, &pod); err != nil {
		return "", fmt.Errorf("fetch manager pod %s/%s: %w", podNamespace, podName, err)
	}

	for _, container := range pod.Spec.Containers {
		if container.Name == managerContainerName {
			image := strings.TrimSpace(container.Image)
			if image == "" {
				return "", fmt.Errorf("manager container image is empty on pod %s/%s", podNamespace, podName)
			}
			return image, nil
		}
	}

	return "", fmt.Errorf("manager container %q not found on pod %s/%s", managerContainerName, podNamespace, podName)
}

func debugLoggingEnabled() bool {
	raw := strings.TrimSpace(os.Getenv(contracts.DebugEnv))
	if raw == "" {
		return false
	}
	switch strings.ToLower(raw) {
	case "1", "true", "t", "yes", "y", "on", "debug":
		return true
	default:
		return false
	}
}
