package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var (
	// Deprecated: bubu_* prefix kept for backward compatibility; prefer bobravoz_* below
	rpcDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bubu_grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests handled by the Hub (seconds)",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "code"},
	)

	// Deprecated: bubu_* prefix kept for backward compatibility; prefer bobravoz_* below
	rpcTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bubu_grpc_requests_total",
			Help: "Total number of gRPC requests handled by the Hub, labeled by code",
		},
		[]string{"method", "code"},
	)

	// Preferred metric names with bobravoz_* prefix
	rpcDurationBobravoz = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobravoz_grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests handled by the Hub (seconds)",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "code"},
	)

	rpcTotalBobravoz = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_grpc_requests_total",
			Help: "Total number of gRPC requests handled by the Hub, labeled by code",
		},
		[]string{"method", "code"},
	)
)

func init() {
	// Register with controller-runtime metrics registry
	crmetrics.Registry.MustRegister(rpcDuration, rpcTotal, rpcDurationBobravoz, rpcTotalBobravoz)
}

// UnaryServerInterceptor records duration and status code for unary RPCs.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err).String()
		elapsedSeconds := time.Since(start).Seconds()
		rpcDuration.WithLabelValues(info.FullMethod, code).
			Observe(elapsedSeconds)
		rpcTotal.WithLabelValues(info.FullMethod, code).
			Inc()
		rpcDurationBobravoz.WithLabelValues(info.FullMethod, code).Observe(elapsedSeconds)
		rpcTotalBobravoz.WithLabelValues(info.FullMethod, code).Inc()
		return resp, err
	}
}

// StreamServerInterceptor records duration and status code for streaming RPCs.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		code := status.Code(err).String()
		elapsedSeconds := time.Since(start).Seconds()
		rpcDuration.WithLabelValues(info.FullMethod, code).
			Observe(elapsedSeconds)
		rpcTotal.WithLabelValues(info.FullMethod, code).
			Inc()
		rpcDurationBobravoz.WithLabelValues(info.FullMethod, code).Observe(elapsedSeconds)
		rpcTotalBobravoz.WithLabelValues(info.FullMethod, code).Inc()
		return err
	}
}
