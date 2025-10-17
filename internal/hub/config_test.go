package hub

import (
	"testing"
	"time"

	"github.com/bubustack/bobrapet/pkg/contracts"
)

func TestGetChannelBufferSizeFromEnv(t *testing.T) {
	t.Setenv(contracts.GRPCChannelBufferSizeEnv, "512")
	if size := getChannelBufferSize(); size != 512 {
		t.Fatalf("expected channel buffer size 512, got %d", size)
	}
}

func TestGetChannelBufferSizeDefault(t *testing.T) {
	t.Setenv(contracts.GRPCChannelBufferSizeEnv, "")
	if size := getChannelBufferSize(); size != 100 {
		t.Fatalf("expected default buffer size 100, got %d", size)
	}
}

func TestGetEvictionIntervalFromEnv(t *testing.T) {
	t.Setenv(contracts.HubBufferEvictionIntervalEnv, "45s")
	if interval := getEvictionInterval(); interval != 45*time.Second {
		t.Fatalf("expected eviction interval 45s, got %s", interval)
	}
}

func TestGetEvictionIntervalDefault(t *testing.T) {
	t.Setenv(contracts.HubBufferEvictionIntervalEnv, "invalid")
	if interval := getEvictionInterval(); interval != time.Minute {
		t.Fatalf("expected default eviction interval 1m, got %s", interval)
	}
}
