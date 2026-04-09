package config

import (
	"testing"
	"time"

	"github.com/bubustack/core/contracts"
	corev1 "k8s.io/api/core/v1"
)

func mustParseOperatorConfigMap(t *testing.T, cm *corev1.ConfigMap) *OperatorConfig {
	t.Helper()
	cfg, err := parseOperatorConfigMap(cm)
	if err != nil {
		t.Fatalf("parseOperatorConfigMap returned error: %v", err)
	}
	return cfg
}

func TestOperatorConfigParseHubTunables(t *testing.T) {
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"hub.buffer-max-messages":      "123",
			"hub.buffer-max-bytes":         "2048",
			"hub.buffer-eviction-ttl":      "5m",
			"hub.buffer-eviction-interval": "30s",
			"hub.channel-buffer-size":      "55",
			"hub.per-message-timeout":      "90s",
			"hub.max-active-streams":       "250",
			"hub.max-buffers":              "400",
			"hub.max-downstreams-hard-cap": "16",
		},
	}

	cfg := mustParseOperatorConfigMap(t, cm)
	if cfg.Hub.BufferMaxMessages != 123 {
		t.Fatalf("expected buffer max messages to be 123, got %d", cfg.Hub.BufferMaxMessages)
	}
	if cfg.Hub.BufferMaxBytes != 2048 {
		t.Fatalf("expected buffer max bytes to be 2048, got %d", cfg.Hub.BufferMaxBytes)
	}
	if cfg.Hub.BufferEvictionTTL != 5*time.Minute {
		t.Fatalf("expected buffer eviction TTL to be 5m, got %s", cfg.Hub.BufferEvictionTTL)
	}
	if cfg.Hub.BufferEvictionPeriod != 30*time.Second {
		t.Fatalf("expected buffer eviction interval to be 30s, got %s", cfg.Hub.BufferEvictionPeriod)
	}
	if cfg.Hub.ChannelBufferSize != 55 {
		t.Fatalf("expected channel buffer size to be 55, got %d", cfg.Hub.ChannelBufferSize)
	}
	if cfg.Hub.PerMessageTimeout != 90*time.Second {
		t.Fatalf("expected per message timeout to be 90s, got %s", cfg.Hub.PerMessageTimeout)
	}
	if cfg.Hub.MaxActiveStreams != 250 {
		t.Fatalf("expected max active streams to be 250, got %d", cfg.Hub.MaxActiveStreams)
	}
	if cfg.Hub.MaxBuffers != 400 {
		t.Fatalf("expected max buffers to be 400, got %d", cfg.Hub.MaxBuffers)
	}
	if cfg.Hub.MaxDownstreamsHardCap != 16 {
		t.Fatalf("expected max downstreams hard cap to be 16, got %d", cfg.Hub.MaxDownstreamsHardCap)
	}
}

func TestOperatorConfigSecurityModeParsing(t *testing.T) {
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"hub.transport-security-mode": "tls",
		},
	}
	cfg := mustParseOperatorConfigMap(t, cm)
	if cfg.Hub.SecurityMode != contracts.TransportSecurityModeTLS {
		t.Fatalf("expected security mode tls, got %s", cfg.Hub.SecurityMode)
	}

}

func TestOperatorConfigRejectsUnsupportedSecurityMode(t *testing.T) {
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"hub.transport-security-mode": "plaintext",
		},
	}
	if _, err := parseOperatorConfigMap(cm); err == nil {
		t.Fatal("expected unsupported security mode to return an error")
	}
}

func TestDefaultOperatorConfigSecurityModeIsTLS(t *testing.T) {
	cfg := DefaultOperatorConfig()
	if cfg.Hub.SecurityMode != contracts.TransportSecurityModeTLS {
		t.Fatalf("expected default security mode tls, got %s", cfg.Hub.SecurityMode)
	}
}

func TestOperatorConfigConnectorFields(t *testing.T) {
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"connector.image":             "ghcr.io/example/connector:v1",
			"connector.image-pull-policy": "Always",
		},
	}

	cfg := mustParseOperatorConfigMap(t, cm)
	if cfg.Connector.Image != "ghcr.io/example/connector:v1" {
		t.Fatalf("expected connector image to be configured, got %s", cfg.Connector.Image)
	}
	if cfg.Connector.ImagePullPolicy != corev1.PullAlways {
		t.Fatalf("expected connector pull policy to be Always, got %s", cfg.Connector.ImagePullPolicy)
	}
}

func TestOperatorConfigHotReload(t *testing.T) {
	// Test that parsing a new ConfigMap yields updated configuration values.
	initialCM := &corev1.ConfigMap{
		Data: map[string]string{
			"hub.buffer-max-messages": "100",
			"hub.buffer-max-bytes":    "1024",
			"hub.max-active-streams":  "500",
		},
	}

	initialCfg := mustParseOperatorConfigMap(t, initialCM)
	if initialCfg.Hub.BufferMaxMessages != 100 {
		t.Fatalf("expected initial buffer max messages to be 100, got %d", initialCfg.Hub.BufferMaxMessages)
	}
	if initialCfg.Hub.BufferMaxBytes != 1024 {
		t.Fatalf("expected initial buffer max bytes to be 1024, got %d", initialCfg.Hub.BufferMaxBytes)
	}
	if initialCfg.Hub.MaxActiveStreams != 500 {
		t.Fatalf("expected initial max active streams to be 500, got %d", initialCfg.Hub.MaxActiveStreams)
	}

	// Simulate a hot reload by parsing an updated ConfigMap.
	updatedCM := &corev1.ConfigMap{
		Data: map[string]string{
			"hub.buffer-max-messages": "200",
			"hub.buffer-max-bytes":    "2048",
			"hub.max-active-streams":  "1000",
		},
	}

	updatedCfg := mustParseOperatorConfigMap(t, updatedCM)
	if updatedCfg.Hub.BufferMaxMessages != 200 {
		t.Fatalf("expected updated buffer max messages to be 200, got %d", updatedCfg.Hub.BufferMaxMessages)
	}
	if updatedCfg.Hub.BufferMaxBytes != 2048 {
		t.Fatalf("expected updated buffer max bytes to be 2048, got %d", updatedCfg.Hub.BufferMaxBytes)
	}
	if updatedCfg.Hub.MaxActiveStreams != 1000 {
		t.Fatalf("expected updated max active streams to be 1000, got %d", updatedCfg.Hub.MaxActiveStreams)
	}

	// Verify initial config was not mutated.
	if initialCfg.Hub.BufferMaxMessages != 100 {
		t.Fatal("initial config should not have been mutated")
	}
}

func TestOperatorConfigTemplatingHotReload(t *testing.T) {
	// Test templating config changes are reflected in subsequent parses.
	cm1 := &corev1.ConfigMap{
		Data: map[string]string{
			contracts.KeyTemplatingEvaluationTimeout: "10s",
			contracts.KeyTemplatingMaxOutputBytes:    "8192",
			contracts.KeyTemplatingDeterministic:     "true",
			contracts.KeyTemplatingOffloadedPolicy:   "skip",
		},
	}

	cfg1 := mustParseOperatorConfigMap(t, cm1)
	if cfg1.Templating.EvaluationTimeout != 10*time.Second {
		t.Fatalf("expected evaluation timeout 10s, got %s", cfg1.Templating.EvaluationTimeout)
	}
	if cfg1.Templating.MaxOutputBytes != 8192 {
		t.Fatalf("expected max output bytes 8192, got %d", cfg1.Templating.MaxOutputBytes)
	}
	if !cfg1.Templating.Deterministic {
		t.Fatal("expected deterministic to be true")
	}
	if cfg1.Templating.OffloadedPolicy != "skip" {
		t.Fatalf("expected offloaded policy 'skip', got %s", cfg1.Templating.OffloadedPolicy)
	}

	// Update ConfigMap with new values.
	cm2 := &corev1.ConfigMap{
		Data: map[string]string{
			contracts.KeyTemplatingEvaluationTimeout: "30s",
			contracts.KeyTemplatingMaxOutputBytes:    "16384",
			contracts.KeyTemplatingDeterministic:     "false",
			contracts.KeyTemplatingOffloadedPolicy:   "error",
		},
	}

	cfg2 := mustParseOperatorConfigMap(t, cm2)
	if cfg2.Templating.EvaluationTimeout != 30*time.Second {
		t.Fatalf("expected updated evaluation timeout 30s, got %s", cfg2.Templating.EvaluationTimeout)
	}
	if cfg2.Templating.MaxOutputBytes != 16384 {
		t.Fatalf("expected updated max output bytes 16384, got %d", cfg2.Templating.MaxOutputBytes)
	}
	if cfg2.Templating.Deterministic {
		t.Fatal("expected deterministic to be false after update")
	}
	if cfg2.Templating.OffloadedPolicy != "error" {
		t.Fatalf("expected offloaded policy 'error', got %s", cfg2.Templating.OffloadedPolicy)
	}
}

func TestOperatorConfigRejectsInvalidEvaluationTimeout(t *testing.T) {
	defaults := DefaultOperatorConfig()

	tooLow := &corev1.ConfigMap{
		Data: map[string]string{
			contracts.KeyTemplatingEvaluationTimeout: "500ms",
		},
	}
	cfg := mustParseOperatorConfigMap(t, tooLow)
	if cfg.Templating.EvaluationTimeout != defaults.Templating.EvaluationTimeout {
		t.Fatalf("expected default evaluation timeout %s, got %s", defaults.Templating.EvaluationTimeout, cfg.Templating.EvaluationTimeout)
	}

	tooHigh := &corev1.ConfigMap{
		Data: map[string]string{
			contracts.KeyTemplatingEvaluationTimeout: "90s",
		},
	}
	cfg = mustParseOperatorConfigMap(t, tooHigh)
	if cfg.Templating.EvaluationTimeout != defaults.Templating.EvaluationTimeout {
		t.Fatalf("expected default evaluation timeout %s when too high, got %s", defaults.Templating.EvaluationTimeout, cfg.Templating.EvaluationTimeout)
	}

	valid := &corev1.ConfigMap{
		Data: map[string]string{
			contracts.KeyTemplatingEvaluationTimeout: "2s",
		},
	}
	cfg = mustParseOperatorConfigMap(t, valid)
	if cfg.Templating.EvaluationTimeout != 2*time.Second {
		t.Fatalf("expected evaluation timeout 2s, got %s", cfg.Templating.EvaluationTimeout)
	}
}

func TestOperatorConfigDefersToDefaultsOnInvalidValues(t *testing.T) {
	// Test that invalid values in ConfigMap result in defaults being used.
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"hub.buffer-max-messages":      "not-a-number",
			"hub.buffer-max-bytes":         "-100",
			"hub.buffer-eviction-ttl":      "invalid-duration",
			"telemetry.trace-propagation":  "not-a-bool",
			"hub.max-downstreams-hard-cap": "0", // should be ignored (not positive)
		},
	}

	cfg := mustParseOperatorConfigMap(t, cm)
	defaults := DefaultOperatorConfig()

	// Invalid numeric values should result in defaults.
	if cfg.Hub.BufferMaxMessages != defaults.Hub.BufferMaxMessages {
		t.Fatalf("expected default buffer max messages %d, got %d", defaults.Hub.BufferMaxMessages, cfg.Hub.BufferMaxMessages)
	}
	if cfg.Hub.BufferMaxBytes != defaults.Hub.BufferMaxBytes {
		t.Fatalf("expected default buffer max bytes %d, got %d", defaults.Hub.BufferMaxBytes, cfg.Hub.BufferMaxBytes)
	}
	if cfg.Hub.BufferEvictionTTL != defaults.Hub.BufferEvictionTTL {
		t.Fatalf("expected default eviction TTL %s, got %s", defaults.Hub.BufferEvictionTTL, cfg.Hub.BufferEvictionTTL)
	}
	// Invalid bool should result in default.
	if cfg.Telemetry.TracePropagation != defaults.Telemetry.TracePropagation {
		t.Fatalf("expected default trace propagation %v, got %v", defaults.Telemetry.TracePropagation, cfg.Telemetry.TracePropagation)
	}
	// Zero value for max downstreams should use default.
	if cfg.Hub.MaxDownstreamsHardCap != defaults.Hub.MaxDownstreamsHardCap {
		t.Fatalf("expected default max downstreams hard cap %d, got %d", defaults.Hub.MaxDownstreamsHardCap, cfg.Hub.MaxDownstreamsHardCap)
	}
}
