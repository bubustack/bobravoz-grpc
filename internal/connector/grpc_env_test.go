package connector

import (
	"testing"

	"github.com/bubustack/bobrapet/pkg/contracts"
)

func TestResolveChannelBufferSize(t *testing.T) {
	t.Setenv(contracts.GRPCChannelBufferSizeEnv, "64")
	if size := resolveChannelBufferSize(); size != 64 {
		t.Fatalf("expected channel buffer size 64, got %d", size)
	}
}

func TestParseServerMessageSizes(t *testing.T) {
	t.Setenv(contracts.GRPCMaxRecvBytesEnv, "2048")
	t.Setenv(contracts.GRPCMaxSendBytesEnv, "4096")
	recv, send := parseServerMessageSizes()
	if recv != 2048 || send != 4096 {
		t.Fatalf("unexpected message sizes recv=%d send=%d", recv, send)
	}
}

func TestParseServerMessageSizesDefault(t *testing.T) {
	t.Setenv(contracts.GRPCMaxRecvBytesEnv, "invalid")
	t.Setenv(contracts.GRPCMaxSendBytesEnv, "")
	recv, send := parseServerMessageSizes()
	if recv != defaultMaxMessageSize || send != defaultMaxMessageSize {
		t.Fatalf("expected defaults, got recv=%d send=%d", recv, send)
	}
}
