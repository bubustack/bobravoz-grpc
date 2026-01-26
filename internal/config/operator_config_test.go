package config

import (
	"testing"
	"time"

	"github.com/bubustack/core/contracts"
	corev1 "k8s.io/api/core/v1"
)

func TestOperatorConfigParseHubTunables(t *testing.T) {
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"hub.buffer-max-messages":      "123",
			"hub.buffer-max-bytes":         "2048",
			"hub.buffer-eviction-ttl":      "5m",
			"hub.buffer-eviction-interval": "30s",
			"hub.channel-buffer-size":      "55",
			"hub.per-message-timeout":      "90s",
		},
	}

	cfg := parseOperatorConfigMap(cm)
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
}

func TestOperatorConfigSecurityModeParsing(t *testing.T) {
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"hub.transport-security-mode": "tls",
		},
	}
	cfg := parseOperatorConfigMap(cm)
	if cfg.Hub.SecurityMode != contracts.TransportSecurityModeTLS {
		t.Fatalf("expected security mode tls, got %s", cfg.Hub.SecurityMode)
	}

	cm = &corev1.ConfigMap{
		Data: map[string]string{
			"hub.allow-insecure": "false",
		},
	}
	cfg = parseOperatorConfigMap(cm)
	if cfg.Hub.SecurityMode != contracts.TransportSecurityModeTLS {
		t.Fatalf("expected deprecated allow-insecure=false to map to tls, got %s", cfg.Hub.SecurityMode)
	}

	cm = &corev1.ConfigMap{
		Data: map[string]string{
			"hub.allow-insecure": "true",
		},
	}
	cfg = parseOperatorConfigMap(cm)
	if cfg.Hub.SecurityMode != contracts.TransportSecurityModePlaintext {
		t.Fatalf("expected deprecated allow-insecure=true to map to plaintext, got %s", cfg.Hub.SecurityMode)
	}
}

func TestOperatorConfigConnectorFields(t *testing.T) {
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"connector.image":             "ghcr.io/example/connector:v1",
			"connector.image-pull-policy": "Always",
		},
	}

	cfg := parseOperatorConfigMap(cm)
	if cfg.Connector.Image != "ghcr.io/example/connector:v1" {
		t.Fatalf("expected connector image to be configured, got %s", cfg.Connector.Image)
	}
	if cfg.Connector.ImagePullPolicy != corev1.PullAlways {
		t.Fatalf("expected connector pull policy to be Always, got %s", cfg.Connector.ImagePullPolicy)
	}
}
