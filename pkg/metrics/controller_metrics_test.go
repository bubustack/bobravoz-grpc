package metrics

import (
	"testing"
	"time"
)

func TestRecordTransportBindingPending(t *testing.T) {
	// Should not panic when called with valid args.
	RecordTransportBindingPending("test-transport")
}

func TestObserveTransportBindingWait(t *testing.T) {
	// Positive duration should record.
	ObserveTransportBindingWait("test-transport", "pending", 5*time.Second)

	// Zero duration should be a no-op (not panic).
	ObserveTransportBindingWait("test-transport", "pending", 0)

	// Negative duration should be a no-op.
	ObserveTransportBindingWait("test-transport", "pending", -1*time.Second)
}

func TestRecordEngramTransportReadyChange(t *testing.T) {
	RecordEngramTransportReadyChange(true)
	RecordEngramTransportReadyChange(false)
}

func TestRecordTransportCleanup(t *testing.T) {
	// Normal case.
	RecordTransportCleanup("default", 3)

	// Zero count should be a no-op.
	RecordTransportCleanup("default", 0)

	// Negative count should be a no-op.
	RecordTransportCleanup("default", -1)

	// Empty namespace should use "(unknown)".
	RecordTransportCleanup("", 1)
}
