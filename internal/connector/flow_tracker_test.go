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
