package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	transportBindingPendingCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_transport_binding_pending_total",
			Help: "Monotonic counter tracking TransportReconciler waits caused by ErrBindingPending.",
		},
		[]string{"transport"},
	)
	transportBindingWaitHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobravoz_transport_binding_wait_seconds",
			Help:    "Histogram tracking how long StoryRuns wait for TransportBindings during pending and ready transitions.",
			Buckets: prometheus.ExponentialBuckets(0.25, 2, 10),
		},
		[]string{"transport", "phase"},
	)
	engramTransportReadyCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_engram_transport_readiness_changes_total",
			Help: "Monotonic counter tracking Engram transport readiness annotation changes.",
		},
		[]string{"ready"},
	)
	transportCleanupCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_transport_cleanup_engram_total",
			Help: "Monotonic counter tracking Engram annotations reset during transport cleanup.",
		},
		[]string{"namespace"},
	)
)

func init() {
	crmetrics.Registry.MustRegister(transportBindingPendingCounter, transportBindingWaitHistogram, engramTransportReadyCounter, transportCleanupCounter)
}

// RecordTransportBindingPending increments the counter when a transport needs to requeue
// waiting for the operator to seed TransportBindings.
func RecordTransportBindingPending(transportName string) {
	transportBindingPendingCounter.WithLabelValues(transportName).Inc()
}

// ObserveTransportBindingWait records how long a StoryRun has waited (or waited before
// succeeding) for TransportBindings to appear.
func ObserveTransportBindingWait(transportName, phase string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	transportBindingWaitHistogram.WithLabelValues(transportName, phase).Observe(duration.Seconds())
}

// RecordEngramTransportReadyChange increments the readiness-change counter when Engram
// annotations flip between ready states.
func RecordEngramTransportReadyChange(ready bool) {
	label := "false"
	if ready {
		label = "true"
	}
	engramTransportReadyCounter.WithLabelValues(label).Inc()
}

// RecordTransportCleanup increments the cleanup counter when EnsureCleanUp resets Engram annotations.
func RecordTransportCleanup(namespace string, count int) {
	if count <= 0 {
		return
	}
	if namespace == "" {
		namespace = "(unknown)"
	}
	transportCleanupCounter.WithLabelValues(namespace).Add(float64(count))
}
