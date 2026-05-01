package config

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/runtime/operatorconfig"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// DefaultConnectorImage is the canonical image reference for connector sidecars.
	DefaultConnectorImage = "ghcr.io/bubustack/bobravoz-grpc-connector:latest"
)

var configManagerLog = ctrl.Log.WithName("operator-config").WithName("manager")

// OperatorConfig represents the runtime configuration for the bobravoz-grpc controller.
type OperatorConfig struct {
	Hub        HubConfig        `json:"hub,omitempty"`
	Telemetry  TelemetryConfig  `json:"telemetry,omitempty"`
	Templating TemplatingConfig `json:"templating,omitempty"`
	Connector  ConnectorConfig  `json:"connector,omitempty"`
}

// Clone returns a deep copy of the OperatorConfig.
func (cfg *OperatorConfig) Clone() *OperatorConfig {
	if cfg == nil {
		return nil
	}
	copy := *cfg
	return &copy
}

// ConnectorConfig captures injector defaults for the transport connector.
type ConnectorConfig struct {
	Image           string            `json:"image,omitempty"`
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
}

// HubConfig captures hub-specific toggles.
type HubConfig struct {
	// SecurityMode selects the transport security model; only "tls" is supported.
	SecurityMode string `json:"securityMode,omitempty"`
	// BufferMaxMessages is the maximum number of messages retained per stream buffer (> 0).
	BufferMaxMessages int `json:"bufferMaxMessages,omitempty"`
	// BufferMaxBytes is the maximum total bytes retained per stream buffer (> 0).
	BufferMaxBytes int `json:"bufferMaxBytes,omitempty"`
	// BufferEvictionTTL is how long idle buffer entries are kept before eviction (> 0).
	BufferEvictionTTL time.Duration `json:"bufferEvictionTTL,omitempty"`
	// BufferEvictionPeriod is the interval between buffer eviction sweeps (> 0).
	BufferEvictionPeriod time.Duration `json:"bufferEvictionPeriod,omitempty"`
	// ChannelBufferSize is the capacity of per-stream send channels (> 0).
	ChannelBufferSize int `json:"channelBufferSize,omitempty"`
	// PerMessageTimeout is the maximum time allowed for sending a single message (> 0).
	PerMessageTimeout time.Duration `json:"perMessageTimeout,omitempty"`
	// MaxActiveStreams is the upper limit on concurrent hub streams (> 0).
	MaxActiveStreams int `json:"maxActiveStreams,omitempty"`
	// MaxBuffers is the maximum number of stream buffers the hub will allocate (> 0).
	MaxBuffers int `json:"maxBuffers,omitempty"`
	// MaxDownstreamsHardCap is the absolute maximum downstream subscribers per stream (1-256).
	MaxDownstreamsHardCap int `json:"maxDownstreamsHardCap,omitempty"`
}

// TelemetryConfig captures OpenTelemetry-related toggles.
type TelemetryConfig struct {
	TracePropagation bool `json:"tracePropagation,omitempty"`
}

// TemplatingConfig captures template evaluation tunables for realtime routing.
type TemplatingConfig struct {
	EvaluationTimeout time.Duration `json:"evaluationTimeout,omitempty"`
	MaxOutputBytes    int           `json:"maxOutputBytes,omitempty"`
	Deterministic     bool          `json:"deterministic,omitempty"`
	OffloadedPolicy   string        `json:"offloadedPolicy,omitempty"`
	MaterializeEngram string        `json:"materializeEngram,omitempty"`
}

// DefaultOperatorConfig returns the default configuration used when the ConfigMap is absent.
func DefaultOperatorConfig() *OperatorConfig {
	return &OperatorConfig{
		Hub: HubConfig{
			SecurityMode:          contracts.TransportSecurityModeTLS,
			BufferMaxMessages:     1000,
			BufferMaxBytes:        10 * 1024 * 1024,
			BufferEvictionTTL:     10 * time.Minute,
			BufferEvictionPeriod:  time.Minute,
			ChannelBufferSize:     100,
			PerMessageTimeout:     10 * time.Minute,
			MaxActiveStreams:      2000,
			MaxBuffers:            1000,
			MaxDownstreamsHardCap: 64,
		},
		Telemetry: TelemetryConfig{
			TracePropagation: true,
		},
		Templating: TemplatingConfig{
			EvaluationTimeout: 30 * time.Second,
			MaxOutputBytes:    64 * 1024,
			Deterministic:     false,
			OffloadedPolicy:   "error",
			MaterializeEngram: "bubu-materialize",
		},
		Connector: ConnectorConfig{
			Image:           DefaultConnectorImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
		},
	}
}

// OperatorConfigManager watches a ConfigMap and keeps an in-memory copy of the configuration.
type OperatorConfigManager struct {
	manager *operatorconfig.Manager[OperatorConfig]
}

// NewOperatorConfigManager creates a new manager instance. Returns an error if
// the underlying config manager cannot be initialised (e.g. invalid client).
func NewOperatorConfigManager(k8sClient client.Client, namespace, configMapName string) (*OperatorConfigManager, error) {
	shared, err := operatorconfig.NewManager(operatorconfig.Options[OperatorConfig]{
		Client:         configMapReader(k8sClient),
		Logger:         configManagerLog,
		ConfigMapKey:   types.NamespacedName{Name: configMapName, Namespace: namespace},
		ControllerName: "bobravoz-operator-config-manager",
		DefaultConfig:  DefaultOperatorConfig,
		ParseConfigMap: func(cm *corev1.ConfigMap) (*OperatorConfig, error) {
			return parseOperatorConfigMap(cm)
		},
		CloneConfig: func(cfg *OperatorConfig) *OperatorConfig {
			if cfg == nil {
				return nil
			}
			return cfg.Clone()
		},
		OnConfigApplied: func(reason operatorconfig.ReloadReason, cfg *OperatorConfig) {
			configManagerLog.Info("operator configuration applied",
				"reason", reason,
				"tracePropagation", cfg.Telemetry.TracePropagation,
				"hubSecurityMode", cfg.Hub.SecurityMode,
			)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create bobravoz operator config manager: %w", err)
	}

	return &OperatorConfigManager{manager: shared}, nil
}

// SetAPIReader injects a non-cached reader for startup loads before the cache is ready.
func (o *OperatorConfigManager) SetAPIReader(reader client.Reader) {
	o.manager.SetAPIReader(configMapReader(reader))
}

// GetConfig returns the current configuration snapshot.
func (o *OperatorConfigManager) GetConfig() *OperatorConfig {
	return o.manager.CurrentConfig()
}

// LoadInitial performs a synchronous load of the configuration ConfigMap.
func (o *OperatorConfigManager) LoadInitial(ctx context.Context) error {
	return o.manager.LoadInitial(ctx)
}

// SetupWithManager registers the manager as a controller so config updates are tracked.
func (o *OperatorConfigManager) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("bobravoz-operator-config-manager").
		For(&corev1.ConfigMap{}).
		WithEventFilter(o.configMapPredicate()).
		Complete(o)
}

// Reconcile reacts to ConfigMap changes and refreshes the cached configuration.
func (o *OperatorConfigManager) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	_, err := o.manager.Reconcile(ctx, operatorconfig.Request{NamespacedName: req.NamespacedName})
	return reconcile.Result{}, err
}

func (o *OperatorConfigManager) configMapPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return o.manager.MatchesConfigMap(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return o.manager.MatchesConfigMap(e.ObjectNew)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return o.manager.MatchesConfigMap(e.Object)
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

func configMapReader(reader client.Reader) operatorconfig.ConfigMapReader {
	if reader == nil {
		return nil
	}
	return operatorconfig.ConfigMapReaderFunc(func(ctx context.Context, key types.NamespacedName, into *corev1.ConfigMap) error {
		return reader.Get(ctx, key, into)
	})
}

func parseOperatorConfigMap(cm *corev1.ConfigMap) (*OperatorConfig, error) {
	cfg := *DefaultOperatorConfig()
	data := cm.Data
	if mode, err := parseSecurityModeValue(data["hub.transport-security-mode"]); err != nil {
		return nil, err
	} else if mode != "" {
		cfg.Hub.SecurityMode = mode
	}

	positiveInts := []struct {
		key    string
		target *int
	}{
		{key: "hub.buffer-max-messages", target: &cfg.Hub.BufferMaxMessages},
		{key: "hub.buffer-max-bytes", target: &cfg.Hub.BufferMaxBytes},
		{key: "hub.channel-buffer-size", target: &cfg.Hub.ChannelBufferSize},
		{key: "hub.max-active-streams", target: &cfg.Hub.MaxActiveStreams},
		{key: "hub.max-buffers", target: &cfg.Hub.MaxBuffers},
		{key: "hub.max-downstreams-hard-cap", target: &cfg.Hub.MaxDownstreamsHardCap},
	}
	for _, item := range positiveInts {
		setPositiveInt(item.target, data[item.key])
	}

	positiveDurations := []struct {
		key    string
		target *time.Duration
	}{
		{key: "hub.buffer-eviction-ttl", target: &cfg.Hub.BufferEvictionTTL},
		{key: "hub.buffer-eviction-interval", target: &cfg.Hub.BufferEvictionPeriod},
		{key: "hub.per-message-timeout", target: &cfg.Hub.PerMessageTimeout},
	}
	for _, item := range positiveDurations {
		setPositiveDuration(item.target, data[item.key])
	}

	setBool(&cfg.Telemetry.TracePropagation, data["telemetry.trace-propagation"])
	setTrimmedIfPresent(&cfg.Connector.Image, data, "connector.image")
	if val, ok := data["connector.image-pull-policy"]; ok {
		cfg.Connector.ImagePullPolicy = corev1.PullPolicy(strings.TrimSpace(val))
	}
	parseTemplatingConfig(cm, &cfg)
	return &cfg, nil
}

func setPositiveInt(target *int, raw string) {
	if target == nil {
		return
	}
	parsed, err := strconv.Atoi(raw)
	if err == nil && parsed > 0 {
		*target = parsed
	}
}

func setPositiveDuration(target *time.Duration, raw string) {
	if target == nil {
		return
	}
	parsed, err := time.ParseDuration(raw)
	if err == nil && parsed > 0 {
		*target = parsed
	}
}

func setBool(target *bool, raw string) {
	if target == nil {
		return
	}
	parsed, err := strconv.ParseBool(raw)
	if err == nil {
		*target = parsed
	}
}

func setTrimmedIfPresent(target *string, data map[string]string, key string) {
	if target == nil || data == nil {
		return
	}
	if val, ok := data[key]; ok {
		*target = strings.TrimSpace(val)
	}
}

func parseTemplatingConfig(cm *corev1.ConfigMap, cfg *OperatorConfig) {
	if val, ok := cm.Data[contracts.KeyTemplatingEvaluationTimeout]; ok {
		parsed, err := time.ParseDuration(val)
		switch {
		case err != nil:
			configManagerLog.Info("invalid templating evaluation timeout; keeping default", "raw", val, "error", err.Error())
		case parsed < time.Second:
			configManagerLog.Info("templating evaluation timeout below minimum; keeping default", "raw", val)
		case parsed > 60*time.Second:
			configManagerLog.Info("templating evaluation timeout above maximum; keeping default", "raw", val)
		default:
			cfg.Templating.EvaluationTimeout = parsed
		}
	}
	if val, ok := cm.Data[contracts.KeyTemplatingMaxOutputBytes]; ok {
		if parsed, err := strconv.Atoi(val); err == nil && parsed >= 0 {
			cfg.Templating.MaxOutputBytes = parsed
		}
	}
	if val, ok := cm.Data[contracts.KeyTemplatingDeterministic]; ok {
		if parsed, err := strconv.ParseBool(val); err == nil {
			cfg.Templating.Deterministic = parsed
		}
	}
	if val, ok := cm.Data[contracts.KeyTemplatingOffloadedPolicy]; ok {
		cfg.Templating.OffloadedPolicy = strings.TrimSpace(val)
	}
	if val, ok := cm.Data[contracts.KeyTemplatingMaterializeEngram]; ok {
		cfg.Templating.MaterializeEngram = strings.TrimSpace(val)
	}
}

// parseSecurityModeValue validates the configured transport security mode.
// Only "tls" is supported; blank values keep the existing default.
func parseSecurityModeValue(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	if strings.EqualFold(trimmed, contracts.TransportSecurityModeTLS) {
		return contracts.TransportSecurityModeTLS, nil
	}
	return "", fmt.Errorf(
		"invalid hub.transport-security-mode %q: only %q is supported",
		trimmed,
		contracts.TransportSecurityModeTLS,
	)
}
