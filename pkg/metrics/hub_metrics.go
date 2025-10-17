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
	"os"
	"strconv"

	bobrapetmetrics "github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// hubBufferSizeGauge tracks the current number of messages buffered per downstream
	hubBufferSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobravoz_hub_buffer_current_size",
			Help: "Current number of messages buffered for downstream engrams (gauge for autoscaling)",
		},
		[]string{"storyrun", "step"},
	)

	// hubMessagesDroppedCounter tracks total messages dropped due to buffer overflow
	hubMessagesDroppedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_hub_messages_dropped_total",
			Help: "Total number of messages dropped due to buffer full (monotonic counter)",
		},
		[]string{"storyrun", "step", "reason"},
	)

	// hubBufferFlushCounter tracks total messages flushed from buffers
	hubBufferFlushCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_hub_buffer_flushed_total",
			Help: "Total number of messages flushed from buffers to downstream (monotonic counter)",
		},
		[]string{"storyrun", "step"},
	)

	// hubBufferBytesGauge tracks the current total bytes buffered per downstream
	hubBufferBytesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobravoz_hub_buffer_current_bytes",
			Help: "Current total bytes buffered for downstream engrams (gauge for capacity monitoring)",
		},
		[]string{"storyrun", "step"},
	)

	// Note: message flow (received/sent) counters are defined and registered in bobrapet/pkg/metrics
	// to avoid duplicate registration across binaries. Use wrapper functions below to record them.

	// Configuration gauges to expose buffer limits for alerting/utilization calculations
	hubBufferMaxMessagesGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "bobravoz_hub_buffer_max_messages_config",
			Help: "Configured maximum number of messages buffered per downstream engram (per pod)",
		},
	)

	hubBufferMaxBytesGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "bobravoz_hub_buffer_max_bytes_config",
			Help: "Configured maximum total bytes buffered per downstream engram (per pod)",
		},
	)
)

func init() {
	// Register custom Hub metrics with controller-runtime's registry
	crmetrics.Registry.MustRegister(
		hubBufferSizeGauge,
		hubMessagesDroppedCounter,
		hubBufferFlushCounter,
		hubBufferBytesGauge,
		hubBufferMaxMessagesGauge,
		hubBufferMaxBytesGauge,
	)
	// Initialize config gauges from environment (best-effort; defaults if unset)
	// Keep logic independent from internal packages to avoid import cycles
	if v := os.Getenv("BUBU_HUB_BUFFER_MAX_MESSAGES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			hubBufferMaxMessagesGauge.Set(float64(n))
		}
	} else {
		hubBufferMaxMessagesGauge.Set(100) // default in code
	}
	if v := os.Getenv("BUBU_HUB_BUFFER_MAX_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			hubBufferMaxBytesGauge.Set(float64(n))
		}
	} else {
		hubBufferMaxBytesGauge.Set(10 * 1024 * 1024) // 10MB default
	}
}

// RecordHubBufferSize records the current buffer size (for autoscaling)
func RecordHubBufferSize(storyRun, step string, size int) {
	hubBufferSizeGauge.WithLabelValues(storyRun, step).Set(float64(size))
}

// RecordHubBufferBytes records the current buffer size in bytes
func RecordHubBufferBytes(storyRun, step string, bytes int) {
	hubBufferBytesGauge.WithLabelValues(storyRun, step).Set(float64(bytes))
}

// RecordHubMessageDropped records a dropped message
func RecordHubMessageDropped(storyRun, step, reason string) {
	hubMessagesDroppedCounter.WithLabelValues(storyRun, step, reason).Inc()
}

// RecordHubBufferFlush records a successful buffer flush
func RecordHubBufferFlush(storyRun, step string, count int) {
	hubBufferFlushCounter.WithLabelValues(storyRun, step).Add(float64(count))
}

// RecordHubMessageReceived records a received message at the hub
func RecordHubMessageReceived(storyRun, step string) {
	// Delegate to shared controller metrics to avoid duplicate metric definitions
	bobrapetmetrics.RecordGRPCMessageReceived(storyRun, step)
}

// RecordHubMessageSent records a message successfully sent from the hub
func RecordHubMessageSent(storyRun, step string) {
	bobrapetmetrics.RecordGRPCMessageSent(storyRun, step)
}

// Example HPA configuration for cluster admins:
//
// apiVersion: autoscaling/v2
// kind: HorizontalPodAutoscaler
// metadata:
//   name: bobravoz-hub-autoscaler
//   namespace: bobrapet-system
// spec:
//   scaleTargetRef:
//     apiVersion: apps/v1
//     kind: Deployment
//     name: bobravoz-grpc-hub
//   minReplicas: 2
//   maxReplicas: 10
//   metrics:
//   - type: Pods
//     pods:
//       metric:
//         name: bobravoz_hub_buffer_current_size
//       target:
//         type: AverageValue
//         averageValue: "50"  # Scale up when avg buffer size > 50 msgs per pod
