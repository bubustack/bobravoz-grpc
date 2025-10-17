package connector

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/bobrapet/pkg/contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	defaultMaxMessageSize    = 10 * 1024 * 1024
	defaultChannelBufferSize = 16
	defaultKeepaliveTime     = 0
	defaultKeepaliveTimeout  = 0
	defaultMessageTimeout    = 30 * time.Second
)

func serverOptionsFromEnv() []grpc.ServerOption {
	recv, send := parseServerMessageSizes()
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(recv),
		grpc.MaxSendMsgSize(send),
	}
	if kaOpt, ok := serverKeepaliveOptionFromEnv(); ok {
		opts = append(opts, kaOpt)
	}
	return opts
}

func clientCallOptionsFromEnv() []grpc.CallOption {
	recv := parsePositiveIntEnv(contracts.GRPCClientMaxRecvBytesEnv, defaultMaxMessageSize)
	send := parsePositiveIntEnv(contracts.GRPCClientMaxSendBytesEnv, defaultMaxMessageSize)
	return []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(recv),
		grpc.MaxCallSendMsgSize(send),
	}
}

func parseServerMessageSizes() (int, int) {
	recv := parsePositiveIntEnv(contracts.GRPCMaxRecvBytesEnv, defaultMaxMessageSize)
	send := parsePositiveIntEnv(contracts.GRPCMaxSendBytesEnv, defaultMaxMessageSize)
	return recv, send
}

func parsePositiveIntEnv(key string, def int) int {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func parsePositiveDurationEnv(key string) time.Duration {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			return d
		}
	}
	return 0
}

func messageTimeoutFromEnv() time.Duration {
	if d := parsePositiveDurationEnv(contracts.GRPCMessageTimeoutEnv); d > 0 {
		return d
	}
	return defaultMessageTimeout
}

func channelSendTimeoutFromEnv() time.Duration {
	return parsePositiveDurationEnv(contracts.GRPCChannelSendTimeoutEnv)
}

func hangTimeoutFromEnv() time.Duration {
	return parsePositiveDurationEnv(contracts.GRPCHangTimeoutEnv)
}
func serverKeepaliveOptionFromEnv() (grpc.ServerOption, bool) {
	var params keepalive.ServerParameters
	var have bool
	if v := os.Getenv(contracts.GRPCKeepaliveTimeEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			params.Time = d
			have = true
		}
	}
	if v := os.Getenv(contracts.GRPCKeepaliveTimeoutEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			params.Timeout = d
			have = true
		}
	}
	if !have {
		return nil, false
	}
	return grpc.KeepaliveParams(params), true
}

func resolveChannelBufferSize() int {
	return parsePositiveIntEnv(contracts.GRPCChannelBufferSizeEnv, defaultChannelBufferSize)
}
