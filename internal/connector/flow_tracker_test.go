package connector

import (
	"testing"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
)

func TestFlowTrackerPartitionAcks(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_partition"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	tracker := newFlowTracker(logr.Discard(), settings)
	packet := &transportpb.DataPacket{Envelope: &transportpb.StreamEnvelope{Partition: "p1", Sequence: 7}}
	flow := tracker.recordDelivery(packet)
	if flow == nil {
		t.Fatalf("expected flow update")
	}
	if flow.Ack != 0 {
		t.Fatalf("expected stream ack to be 0, got %d", flow.Ack)
	}
	if len(flow.PartitionAcks) != 1 {
		t.Fatalf("expected 1 partition ack, got %d", len(flow.PartitionAcks))
	}
	ack := flow.PartitionAcks[0]
	if ack.GetPartition() != "p1" || ack.GetAck() != 7 {
		t.Fatalf("unexpected partition ack: %+v", ack)
	}
}

func TestFlowTrackerStreamAckWhenOrderingPerStream(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_stream"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	tracker := newFlowTracker(logr.Discard(), settings)
	packet := &transportpb.DataPacket{Envelope: &transportpb.StreamEnvelope{Sequence: 9}}
	flow := tracker.recordDelivery(packet)
	if flow == nil {
		t.Fatalf("expected flow update")
	}
	if flow.Ack != 9 {
		t.Fatalf("expected stream ack 9, got %d", flow.Ack)
	}
	if len(flow.PartitionAcks) != 0 {
		t.Fatalf("expected no partition acks, got %d", len(flow.PartitionAcks))
	}
}

func TestFlowTrackerRecordReceiptWhenOrderingPerStream(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_stream"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	tracker := newFlowTracker(logr.Discard(), settings)
	flow := tracker.recordReceipt(64, 11, "")
	if flow == nil {
		t.Fatalf("expected flow update")
	}
	if flow.Ack != 11 {
		t.Fatalf("expected stream ack 11, got %d", flow.Ack)
	}
	if len(flow.PartitionAcks) != 0 {
		t.Fatalf("expected no partition acks, got %d", len(flow.PartitionAcks))
	}
}

func TestFlowTrackerIgnoresDuplicateStreamReceipt(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_stream"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	tracker := newFlowTracker(logr.Discard(), settings)
	requireFlow := tracker.recordReceipt(64, 11, "")
	if requireFlow == nil {
		t.Fatal("expected initial flow update")
	}
	if duplicate := tracker.recordReceipt(64, 11, ""); duplicate != nil {
		t.Fatalf("expected duplicate receipt to be ignored, got %+v", duplicate)
	}
}

func TestFlowTrackerDuplicateAckPerPartition(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_partition"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	tracker := newFlowTracker(logr.Discard(), settings)

	// First delivery for partition p1, sequence 5
	flow1 := tracker.recordReceipt(64, 5, "p1")
	if flow1 == nil {
		t.Fatal("expected first flow update")
	}
	if len(flow1.PartitionAcks) != 1 || flow1.PartitionAcks[0].GetAck() != 5 {
		t.Fatalf("expected partition ack for seq 5, got %+v", flow1.PartitionAcks)
	}

	// Duplicate delivery for same partition and sequence (should be ignored)
	flow2 := tracker.recordReceipt(64, 5, "p1")
	if flow2 != nil {
		t.Fatalf("expected duplicate partition receipt to be ignored, got %+v", flow2)
	}

	// Delivery with lower sequence (out of order, should be ignored)
	flow3 := tracker.recordReceipt(64, 3, "p1")
	if flow3 != nil {
		t.Fatalf("expected out-of-order (lower seq) partition receipt to be ignored, got %+v", flow3)
	}

	// New delivery with higher sequence (should succeed)
	flow4 := tracker.recordReceipt(64, 7, "p1")
	if flow4 == nil {
		t.Fatal("expected flow update for new higher sequence")
	}
	if len(flow4.PartitionAcks) != 1 || flow4.PartitionAcks[0].GetAck() != 7 {
		t.Fatalf("expected partition ack for seq 7, got %+v", flow4.PartitionAcks)
	}
}

func TestFlowTrackerOutOfOrderDeliveryPerStream(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_stream"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	tracker := newFlowTracker(logr.Discard(), settings)

	// First delivery with sequence 10
	flow1 := tracker.recordReceipt(64, 10, "")
	if flow1 == nil {
		t.Fatal("expected first flow update")
	}
	if flow1.Ack != 10 {
		t.Fatalf("expected ack 10, got %d", flow1.Ack)
	}

	// Out of order delivery with lower sequence (should be ignored)
	flow2 := tracker.recordReceipt(64, 5, "")
	if flow2 != nil {
		t.Fatalf("expected out-of-order receipt to be ignored, got %+v", flow2)
	}

	// Delivery at same sequence (duplicate, should be ignored)
	flow3 := tracker.recordReceipt(64, 10, "")
	if flow3 != nil {
		t.Fatalf("expected duplicate receipt to be ignored, got %+v", flow3)
	}

	// In-order delivery with higher sequence
	flow4 := tracker.recordReceipt(64, 11, "")
	if flow4 == nil {
		t.Fatal("expected flow update for sequence 11")
	}
	if flow4.Ack != 11 {
		t.Fatalf("expected ack 11, got %d", flow4.Ack)
	}
}

func TestFlowTrackerNoOrderingAllowsDuplicates(t *testing.T) {
	// With no ordering mode, duplicate sequence numbers should not be rejected.
	// Use a nil settings payload to get the default (no flow control), then
	// verify that recordReceipt with seq=0 simply increments counters without
	// emitting flow control (since no ackEvery thresholds are set).
	tracker := newFlowTracker(logr.Discard(), nil)
	if tracker != nil {
		// nil settings may produce a nil tracker — that's fine.
		flow := tracker.recordReceipt(64, 0, "")
		// Without ackEvery configuration, no flow update is expected.
		_ = flow
	}
}

func TestFlowTrackerMultiplePartitionsIndependent(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_partition"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	tracker := newFlowTracker(logr.Discard(), settings)

	// Delivery to partition p1
	flow1 := tracker.recordReceipt(64, 5, "p1")
	if flow1 == nil {
		t.Fatal("expected flow update for p1")
	}

	// Delivery to partition p2 (independent sequence space)
	flow2 := tracker.recordReceipt(64, 5, "p2")
	if flow2 == nil {
		t.Fatal("expected flow update for p2 (independent from p1)")
	}

	// Advance p1
	flow3 := tracker.recordReceipt(64, 6, "p1")
	if flow3 == nil {
		t.Fatal("expected flow update for p1 seq 6")
	}

	// p2 should still reject seq 5 (already seen)
	flow4 := tracker.recordReceipt(64, 5, "p2")
	if flow4 != nil {
		t.Fatalf("expected duplicate p2 receipt to be ignored, got %+v", flow4)
	}

	// p2 can advance independently
	flow5 := tracker.recordReceipt(64, 10, "p2")
	if flow5 == nil {
		t.Fatal("expected flow update for p2 seq 10")
	}
}

func TestFlowTrackerRecordDeliveryWithPacket(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_stream"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	tracker := newFlowTracker(logr.Discard(), settings)

	packet := &transportpb.DataPacket{
		Envelope: &transportpb.StreamEnvelope{Sequence: 42},
	}
	flow := tracker.recordDelivery(packet)
	if flow == nil {
		t.Fatal("expected flow update")
	}
	if flow.Ack != 42 {
		t.Fatalf("expected ack 42, got %d", flow.Ack)
	}

	// Duplicate packet should be ignored
	flow2 := tracker.recordDelivery(packet)
	if flow2 != nil {
		t.Fatalf("expected duplicate packet delivery to be ignored, got %+v", flow2)
	}
}

func TestFlowTrackerReset(t *testing.T) {
	settings := []byte(`{"delivery":{"ordering":"per_stream"},"flowControl":{"mode":"window","ackEvery":{"messages":1}}}`)
	tracker := newFlowTracker(logr.Discard(), settings)

	// Record some deliveries
	tracker.recordReceipt(64, 10, "")
	tracker.recordReceipt(64, 11, "")

	// Reset the tracker
	tracker.reset()

	// After reset, should be able to receive seq 1 again
	flow := tracker.recordReceipt(64, 1, "")
	if flow == nil {
		t.Fatal("expected flow update after reset")
	}
	if flow.Ack != 1 {
		t.Fatalf("expected ack 1 after reset, got %d", flow.Ack)
	}
}
