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
			"hub.max-active-streams":       "250",
			"hub.max-buffers":              "400",
			"hub.max-downstreams-hard-cap": "16",
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
	cfg := parseOperatorConfigMap(cm)
	if cfg.Hub.SecurityMode != contracts.TransportSecurityModeTLS {
		t.Fatalf("expected security mode tls, got %s", cfg.Hub.SecurityMode)
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
