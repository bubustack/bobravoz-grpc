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
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"time"

	bobrapetmetrics "github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/core/contracts"
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

	hubHeartbeatIntervalGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "bobravoz_hub_heartbeat_interval_seconds",
			Help: "Configured heartbeat interval in seconds for hub-to-connector keepalives.",
		},
	)

	hubHeartbeatFailureCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "bobravoz_hub_heartbeat_failures_total",
			Help: "Monotonic counter tracking SendHeartbeats errors.",
		},
	)

	hubStartupCapabilitiesObservationCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_hub_startup_capabilities_observations_total",
			Help: "Monotonic counter tracking passive observations of connector startup capability declarations.",
		},
		[]string{"mode"},
	)

	hubHeartbeatsReceivedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_hub_heartbeats_received_total",
			Help: "Total number of heartbeat packets received on hub streams.",
		},
		[]string{"storyrun", "step"},
	)

	// hubEventTimeWatermarkGauge tracks the latest observed event-time watermark per stream.
	hubEventTimeWatermarkGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobravoz_hub_event_time_watermark_seconds",
			Help: "Latest observed event-time watermark (Unix seconds) per storyrun/step.",
		},
		[]string{"storyrun", "step"},
	)

	// hubEventTimeLagHistogram tracks the lag between event time and processing time.
	hubEventTimeLagHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobravoz_hub_event_time_lag_seconds",
			Help:    "Lag between event time and hub processing time in seconds.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		},
		[]string{"storyrun", "step"},
	)

	// hubRecordingCounter tracks recording attempts and outcomes.
	hubRecordingCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_hub_stream_recordings_total",
			Help: "Total number of stream recordings (by mode and outcome).",
		},
		[]string{"storyrun", "step", "mode", "status"},
	)

	// hubReplayLastAckGauge tracks the last acknowledged sequence per partition.
	hubReplayLastAckGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobravoz_hub_replay_last_ack",
			Help: "Last acknowledged sequence for replayable streams (bucketed partition label).",
		},
		[]string{"storyrun", "step", "partition_bucket"},
	)

	// hubActiveStreams tracks the current number of active streams in the hub.
	hubActiveStreams = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "bobravoz_hub_active_streams",
			Help: "Current number of active streams in the hub.",
		},
	)

	// hubReplayPendingGauge tracks the current number of unacked messages.
	hubReplayPendingGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobravoz_hub_replay_unacked_current",
			Help: "Current number of unacked messages for replayable streams.",
		},
		[]string{"storyrun", "step"},
	)
)

const replayPartitionBucketCount = 16

func init() {
	// Register custom Hub metrics with controller-runtime's registry
	crmetrics.Registry.MustRegister(
		hubBufferSizeGauge,
		hubMessagesDroppedCounter,
		hubBufferFlushCounter,
		hubBufferBytesGauge,
		hubBufferMaxMessagesGauge,
		hubBufferMaxBytesGauge,
		hubHeartbeatIntervalGauge,
		hubHeartbeatFailureCounter,
		hubStartupCapabilitiesObservationCounter,
		hubHeartbeatsReceivedCounter,
		hubEventTimeWatermarkGauge,
		hubEventTimeLagHistogram,
		hubRecordingCounter,
		hubReplayLastAckGauge,
		hubReplayPendingGauge,
		hubActiveStreams,
	)
	// Initialize config gauges from environment (best-effort; defaults if unset)
	// Keep logic independent from internal packages to avoid import cycles
	if v := os.Getenv(contracts.HubBufferMaxMessagesEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			hubBufferMaxMessagesGauge.Set(float64(n))
		}
	} else {
		hubBufferMaxMessagesGauge.Set(100) // default in code
	}
	if v := os.Getenv(contracts.HubBufferMaxBytesEnv); v != "" {
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

// RecordHubHeartbeat records a heartbeat packet received on the stream.
func RecordHubHeartbeat(storyRun, step string) {
	hubHeartbeatsReceivedCounter.WithLabelValues(storyRun, step).Inc()
}

// RecordHubMessageSent records a message successfully sent from the hub
func RecordHubMessageSent(storyRun, step string) {
	bobrapetmetrics.RecordGRPCMessageSent(storyRun, step)
}

// RecordHubHeartbeatInterval records the currently effective heartbeat interval.
func RecordHubHeartbeatInterval(interval time.Duration) {
	hubHeartbeatIntervalGauge.Set(interval.Seconds())
}

// RecordHubHeartbeatFailure increments the failure counter when SendHeartbeats fails.
func RecordHubHeartbeatFailure() {
	hubHeartbeatFailureCounter.Inc()
}

// RecordHubStartupCapabilitiesObservation records the startup capability mode
// declared by a connector on the hub stream metadata.
func RecordHubStartupCapabilitiesObservation(mode string) {
	hubStartupCapabilitiesObservationCounter.WithLabelValues(strings.TrimSpace(mode)).Inc()
}

// RecordHubEventTime records event-time watermark and lag metrics.
func RecordHubEventTime(storyRun, step string, eventTime time.Time, lag time.Duration) {
	if !eventTime.IsZero() {
		hubEventTimeWatermarkGauge.WithLabelValues(storyRun, step).Set(float64(eventTime.Unix()))
	}
	if lag < 0 {
		lag = 0
	}
	hubEventTimeLagHistogram.WithLabelValues(storyRun, step).Observe(lag.Seconds())
}

// RecordHubRecording records stream recording outcomes.
func RecordHubRecording(storyRun, step, mode, status string) {
	hubRecordingCounter.WithLabelValues(storyRun, step, mode, status).Inc()
}

// RecordHubReplayLastAck records the last acknowledged sequence for a partition.
func RecordHubReplayLastAck(storyRun, step, partition string, ack uint64) {
	hubReplayLastAckGauge.WithLabelValues(storyRun, step, replayPartitionBucket(partition)).Set(float64(ack))
}

// RecordHubReplayPending records the current number of unacked messages.
func RecordHubReplayPending(storyRun, step string, pending int) {
	hubReplayPendingGauge.WithLabelValues(storyRun, step).Set(float64(pending))
}

// RecordHubActiveStreams sets the current number of active streams in the hub.
func RecordHubActiveStreams(count float64) {
	hubActiveStreams.Set(count)
}

// RefreshHubConfigMetrics updates the configuration gauges at runtime.
// Call this when the operator configuration changes to keep metrics accurate.
func RefreshHubConfigMetrics(maxMessages, maxBytes int) {
	hubBufferMaxMessagesGauge.Set(float64(maxMessages))
	hubBufferMaxBytesGauge.Set(float64(maxBytes))
}

func replayPartitionBucket(partition string) string {
	if partition == "" {
		return "default"
	}

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(partition))
	return fmt.Sprintf("bucket-%02d", hasher.Sum32()%replayPartitionBucketCount)
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
