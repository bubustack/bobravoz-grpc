package config

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/runtime/operatorconfig"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// DefaultConnectorImage is the canonical image reference for connector sidecars.
	DefaultConnectorImage = "ghcr.io/bubustack/bobravoz-connector:latest"
)

var configManagerLog = ctrl.Log.WithName("operator-config").WithName("manager")

// OperatorConfig represents the runtime configuration for the bobravoz-grpc controller.
type OperatorConfig struct {
	Hub       HubConfig       `json:"hub,omitempty"`
	Telemetry TelemetryConfig `json:"telemetry,omitempty"`
	CEL       CelConfig       `json:"cel,omitempty"`
	Connector ConnectorConfig `json:"connector,omitempty"`
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
	SecurityMode         string        `json:"securityMode,omitempty"`
	BufferMaxMessages    int           `json:"bufferMaxMessages,omitempty"`
	BufferMaxBytes       int           `json:"bufferMaxBytes,omitempty"`
	BufferEvictionTTL    time.Duration `json:"bufferEvictionTTL,omitempty"`
	BufferEvictionPeriod time.Duration `json:"bufferEvictionPeriod,omitempty"`
	ChannelBufferSize    int           `json:"channelBufferSize,omitempty"`
	PerMessageTimeout    time.Duration `json:"perMessageTimeout,omitempty"`
}

// TelemetryConfig captures OpenTelemetry-related toggles.
type TelemetryConfig struct {
	TracePropagation bool `json:"tracePropagation,omitempty"`
}

// CelConfig captures CEL evaluator tunables.
type CelConfig struct {
	EvaluationTimeout   time.Duration `json:"evaluationTimeout,omitempty"`
	MaxExpressionLength int           `json:"maxExpressionLength,omitempty"`
	EnableMacros        bool          `json:"enableMacros,omitempty"`
}

// DefaultOperatorConfig returns the default configuration used when the ConfigMap is absent.
func DefaultOperatorConfig() *OperatorConfig {
	return &OperatorConfig{
		Hub: HubConfig{
			SecurityMode:         contracts.TransportSecurityModePlaintext,
			BufferMaxMessages:    1000,
			BufferMaxBytes:       10 * 1024 * 1024,
			BufferEvictionTTL:    10 * time.Minute,
			BufferEvictionPeriod: time.Minute,
			ChannelBufferSize:    100,
			PerMessageTimeout:    10 * time.Minute,
		},
		Telemetry: TelemetryConfig{
			TracePropagation: true,
		},
		CEL: CelConfig{
			EnableMacros: true,
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

// NewOperatorConfigManager creates a new manager instance.
func NewOperatorConfigManager(k8sClient client.Client, namespace, configMapName string) *OperatorConfigManager {
	shared := operatorconfig.NewManager(operatorconfig.Options[OperatorConfig]{
		Client:         k8sClient,
		Logger:         configManagerLog,
		ConfigMapKey:   types.NamespacedName{Name: configMapName, Namespace: namespace},
		ControllerName: "bobravoz-operator-config-manager",
		DefaultConfig:  DefaultOperatorConfig,
		ParseConfigMap: func(cm *corev1.ConfigMap) (*OperatorConfig, error) {
			return parseOperatorConfigMap(cm), nil
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

	return &OperatorConfigManager{manager: shared}
}

// SetAPIReader injects a non-cached reader for startup loads before the cache is ready.
func (o *OperatorConfigManager) SetAPIReader(reader client.Reader) {
	o.manager.SetAPIReader(reader)
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
	return o.manager.SetupWithManager(mgr)
}

// Reconcile reacts to ConfigMap changes and refreshes the cached configuration.
func (o *OperatorConfigManager) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return o.manager.Reconcile(ctx, req)
}

func parseOperatorConfigMap(cm *corev1.ConfigMap) *OperatorConfig {
	cfg := *DefaultOperatorConfig()
	cfg.Hub.SecurityMode = normalizeSecurityModeValue(cfg.Hub.SecurityMode)
	if val, ok := cm.Data["hub.transport-security-mode"]; ok {
		if mode := normalizeSecurityModeValue(val); mode != "" {
			cfg.Hub.SecurityMode = mode
		}
	} else if val, ok := cm.Data["hub.allow-insecure"]; ok {
		if parsed, err := strconv.ParseBool(val); err == nil {
			if parsed {
				cfg.Hub.SecurityMode = contracts.TransportSecurityModePlaintext
			} else {
				cfg.Hub.SecurityMode = contracts.TransportSecurityModeTLS
			}
		}
	}
	if val, ok := cm.Data["hub.buffer-max-messages"]; ok {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			cfg.Hub.BufferMaxMessages = parsed
		}
	}
	if val, ok := cm.Data["hub.buffer-max-bytes"]; ok {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			cfg.Hub.BufferMaxBytes = parsed
		}
	}
	if val, ok := cm.Data["hub.buffer-eviction-ttl"]; ok {
		if parsed, err := time.ParseDuration(val); err == nil && parsed > 0 {
			cfg.Hub.BufferEvictionTTL = parsed
		}
	}
	if val, ok := cm.Data["hub.buffer-eviction-interval"]; ok {
		if parsed, err := time.ParseDuration(val); err == nil && parsed > 0 {
			cfg.Hub.BufferEvictionPeriod = parsed
		}
	}
	if val, ok := cm.Data["hub.channel-buffer-size"]; ok {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			cfg.Hub.ChannelBufferSize = parsed
		}
	}
	if val, ok := cm.Data["hub.per-message-timeout"]; ok {
		if parsed, err := time.ParseDuration(val); err == nil && parsed > 0 {
			cfg.Hub.PerMessageTimeout = parsed
		}
	}
	if val, ok := cm.Data["telemetry.trace-propagation"]; ok {
		if parsed, err := strconv.ParseBool(val); err == nil {
			cfg.Telemetry.TracePropagation = parsed
		}
	}
	if val, ok := cm.Data["connector.image"]; ok {
		cfg.Connector.Image = strings.TrimSpace(val)
	}
	if val, ok := cm.Data["connector.image-pull-policy"]; ok {
		cfg.Connector.ImagePullPolicy = corev1.PullPolicy(strings.TrimSpace(val))
	}
	parseCELConfig(cm, &cfg)
	return &cfg
}

func parseCELConfig(cm *corev1.ConfigMap, cfg *OperatorConfig) {
	if val, ok := cm.Data["cel.evaluation-timeout"]; ok {
		if parsed, err := time.ParseDuration(val); err == nil && parsed >= 0 {
			cfg.CEL.EvaluationTimeout = parsed
		}
	}
	if val, ok := cm.Data["cel.max-expression-length"]; ok {
		if parsed, err := strconv.Atoi(val); err == nil && parsed >= 0 {
			cfg.CEL.MaxExpressionLength = parsed
		}
	}
	if val, ok := cm.Data["cel.enable-macros"]; ok {
		if parsed, err := strconv.ParseBool(val); err == nil {
			cfg.CEL.EnableMacros = parsed
		}
	}
}

func normalizeSecurityModeValue(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case contracts.TransportSecurityModePlaintext:
		return contracts.TransportSecurityModePlaintext
	case contracts.TransportSecurityModeTLS:
		return contracts.TransportSecurityModeTLS
	default:
		return contracts.TransportSecurityModeTLS
	}
}
