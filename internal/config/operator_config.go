package config

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bubustack/bobrapet/pkg/contracts"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// OperatorConfig represents the runtime configuration for the bobravoz-grpc controller.
type OperatorConfig struct {
	Hub       HubConfig       `json:"hub,omitempty"`
	Telemetry TelemetryConfig `json:"telemetry,omitempty"`
	CEL       CelConfig       `json:"cel,omitempty"`
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
	}
}

// OperatorConfigManager watches a ConfigMap and keeps an in-memory copy of the configuration.
type OperatorConfigManager struct {
	client        client.Client
	apiReader     client.Reader
	namespace     string
	configMapName string
	mu            sync.RWMutex
	currentConfig *OperatorConfig
	defaultConfig *OperatorConfig
	lastSync      time.Time
}

// NewOperatorConfigManager creates a new manager instance.
func NewOperatorConfigManager(k8sClient client.Client, namespace, configMapName string) *OperatorConfigManager {
	defaults := DefaultOperatorConfig()
	return &OperatorConfigManager{
		client:        k8sClient,
		namespace:     namespace,
		configMapName: configMapName,
		currentConfig: defaults,
		defaultConfig: defaults,
	}
}

// SetAPIReader injects a non-cached reader for startup loads before the cache is ready.
func (o *OperatorConfigManager) SetAPIReader(reader client.Reader) {
	o.apiReader = reader
}

// GetConfig returns the current configuration snapshot.
func (o *OperatorConfigManager) GetConfig() *OperatorConfig {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.currentConfig
}

// LoadInitial performs a synchronous load of the configuration ConfigMap.
func (o *OperatorConfigManager) LoadInitial(ctx context.Context) error {
	config, err := o.loadAndParseConfig(ctx)
	if err != nil {
		return err
	}
	o.mu.Lock()
	o.currentConfig = config
	o.lastSync = time.Now()
	o.mu.Unlock()
	return nil
}

// SetupWithManager registers the manager as a controller so config updates are tracked.
func (o *OperatorConfigManager) SetupWithManager(mgr ctrl.Manager) error {
	preds := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object.GetNamespace() == o.namespace && e.Object.GetName() == o.configMapName
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectNew.GetNamespace() == o.namespace && e.ObjectNew.GetName() == o.configMapName
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return e.Object.GetNamespace() == o.namespace && e.Object.GetName() == o.configMapName
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.ConfigMap{}).
		WithEventFilter(preds).
		Complete(o)
}

// Reconcile reacts to ConfigMap changes and refreshes the cached configuration.
func (o *OperatorConfigManager) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx).WithName("operator-config-manager")

	if req.Namespace != o.namespace || req.Name != o.configMapName {
		return reconcile.Result{}, nil
	}

	config, err := o.loadAndParseConfig(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("operator config map missing; reverting to defaults")
			o.mu.Lock()
			o.currentConfig = o.defaultConfig
			o.lastSync = time.Now()
			o.mu.Unlock()
			return reconcile.Result{}, nil
		}
		logger.Error(err, "failed to refresh operator configuration")
		return reconcile.Result{RequeueAfter: 30 * time.Second}, err
	}

	o.mu.Lock()
	o.currentConfig = config
	o.lastSync = time.Now()
	o.mu.Unlock()

	logger.Info("operator configuration updated", "lastSync", o.lastSync.Format(time.RFC3339))
	return reconcile.Result{}, nil
}

func (o *OperatorConfigManager) loadAndParseConfig(ctx context.Context) (*OperatorConfig, error) {
	reader := o.apiReader
	if reader == nil {
		reader = o.client
	}

	var cm corev1.ConfigMap
	if err := reader.Get(ctx, types.NamespacedName{
		Namespace: o.namespace,
		Name:      o.configMapName,
	}, &cm); err != nil {
		return nil, fmt.Errorf("get operator config map: %w", err)
	}

	return o.parseConfigMap(&cm), nil
}

func (o *OperatorConfigManager) parseConfigMap(cm *corev1.ConfigMap) *OperatorConfig {
	cfg := *o.defaultConfig
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
