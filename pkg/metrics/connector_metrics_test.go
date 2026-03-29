package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRecordConnectorPatchSuccessIncrementsCounter(t *testing.T) {
	namespace := "ns"
	binding := t.Name()
	metric := connectorPatchSuccessCounter.WithLabelValues(namespace, binding)
	before := testutil.ToFloat64(metric)

	RecordConnectorPatchSuccess(namespace, binding)

	assert.Equal(t, before+1, testutil.ToFloat64(metric))
}

func TestRecordConnectorPatchFailureIncrementsCounter(t *testing.T) {
	namespace := "ns"
	binding := t.Name()
	metric := connectorPatchFailureCounter.WithLabelValues(namespace, binding)
	before := testutil.ToFloat64(metric)

	RecordConnectorPatchFailure(namespace, binding)

	assert.Equal(t, before+1, testutil.ToFloat64(metric))
}

func TestRecordConnectorConnectionState(t *testing.T) {
	RecordConnectorConnectionState("hub:9000", true)
	val := testutil.ToFloat64(connectorConnectionState.WithLabelValues("hub:9000"))
	assert.Equal(t, float64(1), val)

	RecordConnectorConnectionState("hub:9000", false)
	val = testutil.ToFloat64(connectorConnectionState.WithLabelValues("hub:9000"))
	assert.Equal(t, float64(0), val)
}
