/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// streamingLatencyBuckets provides histogram buckets tuned for streaming RPC
// latencies, ranging from sub-millisecond to multi-minute durations.
var streamingLatencyBuckets = []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30, 60, 300}

var (
	// Preferred metric names with bobravoz_* prefix
	rpcDurationBobravoz = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobravoz_grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests handled by the Hub (seconds)",
			Buckets: streamingLatencyBuckets,
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
	crmetrics.Registry.MustRegister(rpcDurationBobravoz, rpcTotalBobravoz)
}

// UnaryServerInterceptor records duration and status code for unary RPCs.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err).String()
		elapsedSeconds := time.Since(start).Seconds()
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
		rpcDurationBobravoz.WithLabelValues(info.FullMethod, code).Observe(elapsedSeconds)
		rpcTotalBobravoz.WithLabelValues(info.FullMethod, code).Inc()
		return err
	}
}
