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
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	connectorCapabilityChangeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_connector_capability_changes_total",
			Help: "Monotonic counter tracking capability updates written to TransportBinding status.",
		},
		[]string{"namespace", "binding", "field"},
	)
	connectorCapabilityFieldEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_connector_capability_field_events_total",
			Help: "Monotonic counter tracking capability observations per field and patch outcome.",
		},
		[]string{"namespace", "binding", "field", "outcome"},
	)
	connectorObservationDropCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_connector_capability_drops_total",
			Help: "Monotonic counter tracking capability observations dropped before they were applied.",
		},
		[]string{"namespace", "binding", "reason"},
	)
	connectorObservationSkipCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_connector_capability_skips_total",
			Help: "Monotonic counter tracking capability observations that were skipped because they were duplicates.",
		},
		[]string{"namespace", "binding", "reason"},
	)
	connectorCapabilityListenerGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobravoz_connector_capability_listeners",
			Help: "Gauge reporting how many listeners are currently watching capability updates for a binding.",
		},
		[]string{"namespace", "binding"},
	)
	connectorDownstreamDropCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_connector_downstream_drops_total",
			Help: "Monotonic counter tracking downstream frames dropped by Subscribe.",
		},
		[]string{"namespace", "binding", "reason"},
	)
	connectorControlDirectiveCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_connector_control_directives_total",
			Help: "Monotonic counter tracking control directives sent/received by direction and type.",
		},
		[]string{"namespace", "binding", "direction", "type"},
	)
	connectorReporterExitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_connector_reporter_exits_total",
			Help: "Monotonic counter tracking binding status reporter exits by reason.",
		},
		[]string{"namespace", "binding", "reason"},
	)
	connectorPatchFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_connector_patch_failures_total",
			Help: "Monotonic counter tracking TransportBinding status patch failures.",
		},
		[]string{"namespace", "binding"},
	)
	connectorPatchSuccessGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobravoz_connector_patch_success_total",
			Help: "Gauge tracking successful TransportBinding status patches.",
		},
		[]string{"namespace", "binding"},
	)
	connectorPatchFailureGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobravoz_connector_patch_failure_total",
			Help: "Gauge tracking failed TransportBinding status patches.",
		},
		[]string{"namespace", "binding"},
	)
)

func init() {
	crmetrics.Registry.MustRegister(
		connectorCapabilityChangeCounter,
		connectorCapabilityFieldEventCounter,
		connectorObservationDropCounter,
		connectorObservationSkipCounter,
		connectorCapabilityListenerGauge,
		connectorDownstreamDropCounter,
		connectorControlDirectiveCounter,
		connectorReporterExitCounter,
		connectorPatchFailureCounter,
		connectorPatchSuccessGauge,
		connectorPatchFailureGauge,
	)
}

// RecordConnectorCapabilityChange increments the counter for the provided binding/field.
func RecordConnectorCapabilityChange(namespace, binding, field string) {
	connectorCapabilityChangeCounter.WithLabelValues(namespace, binding, field).Inc()
}

// RecordConnectorCapabilityFieldEvent records capability field observations keyed by outcome.
func RecordConnectorCapabilityFieldEvent(namespace, binding, field, outcome string) {
	connectorCapabilityFieldEventCounter.WithLabelValues(namespace, binding, field, outcome).Inc()
}

// RecordConnectorObservationDrop increments the counter when an observation is dropped.
func RecordConnectorObservationDrop(namespace, binding, reason string) {
	connectorObservationDropCounter.WithLabelValues(namespace, binding, reason).Inc()
}

// RecordConnectorObservationSkip increments the counter when an observation is skipped.
func RecordConnectorObservationSkip(namespace, binding, reason string) {
	connectorObservationSkipCounter.WithLabelValues(namespace, binding, reason).Inc()
}

// SetConnectorCapabilityListeners records how many listeners are currently registered.
func SetConnectorCapabilityListeners(namespace, binding string, count float64) {
	connectorCapabilityListenerGauge.WithLabelValues(namespace, binding).Set(count)
}

// RecordConnectorDownstreamDrop increments the counter when Subscribe drops a frame.
func RecordConnectorDownstreamDrop(namespace, binding, reason string) {
	connectorDownstreamDropCounter.WithLabelValues(namespace, binding, reason).Inc()
}

// RecordConnectorControlDirective increments the counter for sent/received control directives.
func RecordConnectorControlDirective(namespace, binding, direction, typ string) {
	connectorControlDirectiveCounter.WithLabelValues(namespace, binding, direction, typ).Inc()
}

// RecordConnectorReporterExit increments the counter when the reporter stops draining observations.
func RecordConnectorReporterExit(namespace, binding, reason string) {
	connectorReporterExitCounter.WithLabelValues(namespace, binding, reason).Inc()
}

// RecordConnectorPatchFailure increments the counter when TransportBinding status patches fail.
func RecordConnectorPatchFailure(namespace, binding string) {
	connectorPatchFailureGauge.WithLabelValues(namespace, binding).Inc()
	connectorPatchFailureCounter.WithLabelValues(namespace, binding).Inc()
}

// RecordConnectorPatchSuccess increments the gauge when TransportBinding status patches succeed.
func RecordConnectorPatchSuccess(namespace, binding string) {
	connectorPatchSuccessGauge.WithLabelValues(namespace, binding).Inc()
}
