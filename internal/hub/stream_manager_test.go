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

package hub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/core/contracts"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
)

// Using mockProcessServer defined in buffer_test.go; extend via wrapper for counters
type countingProcessServer struct {
	*mockProcessServer
	sendCount atomic.Int64
	failOnce  atomic.Bool
}

func (c *countingProcessServer) Send(resp *transportpb.ProcessResponse) error {
	if c.failOnce.Load() {
		c.failOnce.Store(false)
		return errors.New("transient send error")
	}
	c.sendCount.Add(1)
	return c.mockProcessServer.Send(resp)
}

func TestSendOrBufferFlushWithSerialization(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// start evictor to ensure no panics; not relied upon in this test
	sm.StartEvictor(ctx)

	// Prepare stream and buffer with one pending message
	s := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	entry := sm.AddStream(ctx, "sr", "tenant-a", "step", s)
	msg1 := &transportpb.DataPacket{Metadata: map[string]string{"i": "1"}}
	msg2 := &transportpb.DataPacket{Metadata: map[string]string{"i": "2"}}

	// Buffer a message by temporarily removing stream
	sm.RemoveStream("sr", "tenant-a", "step", entry)
	_ = sm.SendOrBuffer(ctx, "sr", "tenant-a", "step", msg1) // should buffer
	// Restore stream
	sm.AddStream(ctx, "sr", "tenant-a", "step", s)

	// Next send should flush buffer first, then send new message
	if ok := sm.SendOrBuffer(ctx, "sr", "tenant-a", "step", msg2); !ok {
		t.Fatalf("expected send-or-buffer to succeed")
	}
	if s.sendCount.Load() != 2 {
		t.Fatalf("expected 2 sends (flush+direct), got %d", s.sendCount.Load())
	}
}

func TestSendOrBufferRetriesOnTransientError(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	sm.AddStream(ctx, "sr", "tenant-a", "step", s)

	// Force next Send to fail once
	s.failOnce.Store(true)
	msg := &transportpb.DataPacket{Metadata: map[string]string{"i": "x"}}

	// send should buffer after transient error and return true (buffered)
	if ok := sm.SendOrBuffer(ctx, "sr", "tenant-a", "step", msg); !ok {
		t.Fatalf("expected buffering on transient error")
	}
}

func TestSendOrBufferWithPolicy_DisabledRetryDrops(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx := context.Background()
	policy := &bubuv1alpha1.RetryPolicy{MaxRetries: int32Ptr(0)}
	msg := &transportpb.DataPacket{Metadata: map[string]string{"i": "x"}}

	if ok := sm.SendOrBufferWithPolicy(ctx, "sr", "tenant-a", "step", msg, policy); ok {
		t.Fatalf("expected drop when retries disabled")
	}
	key := sm.streamKey("sr", "tenant-a", "step")
	_, ok := sm.buffers.Load(key)
	if ok {
		t.Fatalf("expected no buffer to be created when retries disabled")
	}
}

func TestSendOrBufferFlushesBufferedOnNextSend(t *testing.T) {
	t.Setenv(contracts.GRPCReconnectBaseBackoffEnv, "1ms")
	t.Setenv(contracts.GRPCReconnectMaxBackoffEnv, "5ms")
	sm := NewStreamManager(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	sm.AddStream(ctx, "sr", "tenant-a", "step", s)

	// Force first send to fail so message is buffered.
	s.failOnce.Store(true)
	msg1 := &transportpb.DataPacket{Metadata: map[string]string{"i": "1"}}
	msg2 := &transportpb.DataPacket{Metadata: map[string]string{"i": "2"}}

	if ok := sm.SendOrBuffer(ctx, "sr", "tenant-a", "step", msg1); !ok {
		t.Fatalf("expected buffering on transient error")
	}
	time.Sleep(2 * time.Millisecond)
	if ok := sm.SendOrBuffer(ctx, "sr", "tenant-a", "step", msg2); !ok {
		t.Fatalf("expected send-or-buffer to succeed")
	}
	if s.sendCount.Load() != 2 {
		t.Fatalf("expected 2 sends (flush+direct), got %d", s.sendCount.Load())
	}
}

func TestDrainTimeoutDropsBufferedMessages(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx := context.Background()
	packet := &transportpb.DataPacket{Metadata: map[string]string{"i": "drain"}}

	opts := StreamOptions{
		BufferLimits: defaultBufferLimits(),
		DrainTimeout: 10 * time.Millisecond,
		DrainEnabled: true,
	}

	if ok := sm.SendOrBufferWithOptions(ctx, "sr", "tenant-a", "step", packet, opts); !ok {
		t.Fatalf("expected buffer to accept packet when stream is missing")
	}
	key := sm.streamKey("sr", "tenant-a", "step")
	val, ok := sm.buffers.Load(key)
	if !ok {
		t.Fatalf("expected buffer to be created")
	}
	buffer, ok := val.(*MessageBuffer)
	if !ok || buffer == nil {
		t.Fatalf("expected message buffer")
	}
	if buffer.deadline.IsZero() {
		t.Fatalf("expected drain deadline to be set")
	}

	time.Sleep(15 * time.Millisecond)
	if buffer.ShouldRetry(time.Now()) {
		t.Fatalf("expected buffer to stop retrying after drain timeout")
	}
	if buffer.Size() != 0 {
		t.Fatalf("expected buffered messages to be dropped after drain timeout")
	}
}

func TestStreamManagerSeparatesNamespaces(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx := context.Background()
	sA := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	sB := &countingProcessServer{mockProcessServer: &mockProcessServer{}}

	sm.AddStream(ctx, "sr", "team-a", "step", sA)
	sm.AddStream(ctx, "sr", "team-b", "step", sB)

	msgA := &transportpb.DataPacket{Metadata: map[string]string{"i": "A"}}
	msgB := &transportpb.DataPacket{Metadata: map[string]string{"i": "B"}}

	if ok := sm.SendOrBuffer(ctx, "sr", "team-a", "step", msgA); !ok {
		t.Fatalf("expected send to team-a to succeed")
	}
	if ok := sm.SendOrBuffer(ctx, "sr", "team-b", "step", msgB); !ok {
		t.Fatalf("expected send to team-b to succeed")
	}

	if got := sA.sendCount.Load(); got != 1 {
		t.Fatalf("expected 1 send for team-a, got %d", got)
	}
	if got := sB.sendCount.Load(); got != 1 {
		t.Fatalf("expected 1 send for team-b, got %d", got)
	}
}

func TestAssignEnvelopePerPartitionSequencing(t *testing.T) {
	sm := NewStreamManager(nil)
	delivery := deliveryPolicy{ordering: orderingPerPartition, semantics: semanticsBestEffort}
	key := sm.streamKey("sr", "tenant-a", "step")
	state := sm.ensureState(key, "sr", "tenant-a", "step", flowControlPolicy{}, delivery, defaultBufferLimits(), 0)

	packetA := &transportpb.DataPacket{Envelope: &transportpb.StreamEnvelope{Partition: "p1"}}
	packetB := &transportpb.DataPacket{Envelope: &transportpb.StreamEnvelope{Partition: "p2"}}
	packetA2 := &transportpb.DataPacket{Envelope: &transportpb.StreamEnvelope{Partition: "p1"}}

	sm.assignEnvelope(state, packetA, delivery)
	sm.assignEnvelope(state, packetB, delivery)
	sm.assignEnvelope(state, packetA2, delivery)

	if packetA.GetEnvelope().GetSequence() != 1 {
		t.Fatalf("expected p1 first sequence=1, got %d", packetA.GetEnvelope().GetSequence())
	}
	if packetB.GetEnvelope().GetSequence() != 1 {
		t.Fatalf("expected p2 first sequence=1, got %d", packetB.GetEnvelope().GetSequence())
	}
	if packetA2.GetEnvelope().GetSequence() != 2 {
		t.Fatalf("expected p1 second sequence=2, got %d", packetA2.GetEnvelope().GetSequence())
	}
}

func TestApplyFlowPartitionAcksClearsUnacked(t *testing.T) {
	sm := NewStreamManager(nil)
	delivery := deliveryPolicy{ordering: orderingPerPartition, semantics: semanticsAtLeastOnce}
	key := sm.streamKey("sr", "tenant-a", "step")
	state := sm.ensureState(key, "sr", "tenant-a", "step", flowControlPolicy{}, delivery, defaultBufferLimits(), 0)

	packetA1 := &transportpb.DataPacket{Envelope: &transportpb.StreamEnvelope{Partition: "p1"}}
	packetA2 := &transportpb.DataPacket{Envelope: &transportpb.StreamEnvelope{Partition: "p1"}}
	packetB1 := &transportpb.DataPacket{Envelope: &transportpb.StreamEnvelope{Partition: "p2"}}

	sm.assignEnvelope(state, packetA1, delivery)
	sm.recordSent(state, packetA1)
	sm.assignEnvelope(state, packetA2, delivery)
	sm.recordSent(state, packetA2)
	sm.assignEnvelope(state, packetB1, delivery)
	sm.recordSent(state, packetB1)

	sm.ApplyFlow("sr", "tenant-a", "step", &transportpb.FlowControl{
		PartitionAcks: []*transportpb.PartitionAck{
			{Partition: "p1", Ack: packetA1.GetEnvelope().GetSequence()},
		},
	})

	state.mu.Lock()
	defer state.mu.Unlock()

	partitionA := state.partitions["p1"]
	if partitionA == nil {
		t.Fatalf("expected partition state for p1")
	}
	if len(partitionA.unacked) != 1 {
		t.Fatalf("expected 1 unacked for p1, got %d", len(partitionA.unacked))
	}
	if _, ok := partitionA.unacked[packetA2.GetEnvelope().GetSequence()]; !ok {
		t.Fatalf("expected p1 seq %d to remain unacked", packetA2.GetEnvelope().GetSequence())
	}

	partitionB := state.partitions["p2"]
	if partitionB == nil {
		t.Fatalf("expected partition state for p2")
	}
	if len(partitionB.unacked) != 1 {
		t.Fatalf("expected 1 unacked for p2, got %d", len(partitionB.unacked))
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}

// TestEnsureStateConcurrent verifies that concurrent calls to ensureState for the
// same key all return the same *streamState pointer (no TOCTOU race).
func TestEnsureStateConcurrent(t *testing.T) {
	sm := NewStreamManager(nil)
	const n = 50
	states := make([]*streamState, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			states[i] = sm.ensureState("key-concurrent", "run", "ns", "step",
				flowControlPolicy{}, deliveryPolicy{}, defaultBufferLimits(), 0)
		}()
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if states[i] != states[0] {
			t.Errorf("goroutine %d got a different *streamState pointer — TOCTOU race", i)
		}
	}
}
