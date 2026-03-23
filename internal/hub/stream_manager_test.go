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

func TestRecordSent_SkipsAlreadyAcked(t *testing.T) {
	sm := NewStreamManager(nil)
	delivery := deliveryPolicy{semantics: semanticsAtLeastOnce, ordering: orderingPerStream}
	key := sm.streamKey("sr", "tenant-a", "step")
	state := sm.ensureState(key, "sr", "tenant-a", "step",
		flowControlPolicy{}, delivery, defaultBufferLimits(), 0)

	state.mu.Lock()
	state.lastAck = 5
	if state.unacked == nil {
		state.unacked = make(map[uint64]*transportpb.DataPacket)
	}
	state.mu.Unlock()

	pkt := &transportpb.DataPacket{
		Envelope: &transportpb.StreamEnvelope{Sequence: 3}, // already acked (3 <= 5)
	}
	sm.recordSent(state, pkt)

	state.mu.Lock()
	_, present := state.unacked[3]
	state.mu.Unlock()
	if present {
		t.Error("already-acked seq 3 should not be inserted into unacked map")
	}
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

func TestAddStream_ShadowWithHigherGeneration(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx := context.Background()
	blue := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	green := &countingProcessServer{mockProcessServer: &mockProcessServer{}}

	// Register blue (gen 1) as active stream.
	blueEntry := sm.AddStream(ctx, "sr", "ns", "step", blue, 1)
	if blueEntry == nil {
		t.Fatal("expected blue entry")
	}
	if !sm.HasStream("sr", "ns", "step") {
		t.Fatal("expected active stream")
	}

	// Register green (gen 2) — goes to shadow then auto-cutovers to active.
	greenEntry := sm.AddStream(ctx, "sr", "ns", "step", green, 2)
	if greenEntry == nil {
		t.Fatal("expected green entry")
	}

	// Shadow should be empty after auto-cutover.
	if sm.HasShadow("sr", "ns", "step") {
		t.Fatal("expected shadow to be cleared after auto-cutover")
	}

	// Active stream should now be green (auto-promoted).
	key := sm.streamKey("sr", "ns", "step")
	val, _ := sm.streams.Load(key)
	if val != greenEntry {
		t.Fatal("expected green to be active stream after auto-cutover")
	}

	// Verify handoff phase is ready (cutover completed) and generation updated.
	stateVal, _ := sm.states.Load(key)
	state := stateVal.(*streamState)
	state.mu.Lock()
	phase := state.handoff.phase
	gen := state.generation
	shadowGen := state.shadowGeneration
	state.mu.Unlock()
	if phase != handoffPhaseReady {
		t.Fatalf("expected ready phase after auto-cutover, got %s", phase)
	}
	if gen != 2 {
		t.Fatalf("expected active generation 2, got %d", gen)
	}
	if shadowGen != 0 {
		t.Fatalf("expected shadow generation 0 after cutover, got %d", shadowGen)
	}

	// Messages should route to green (active), not blue (closed).
	msg := &transportpb.DataPacket{Metadata: map[string]string{"i": "1"}}
	if ok := sm.SendOrBuffer(ctx, "sr", "ns", "step", msg); !ok {
		t.Fatal("expected send to succeed")
	}
	if green.sendCount.Load() != 1 {
		t.Fatalf("expected green to receive 1 message, got %d", green.sendCount.Load())
	}
	if blue.sendCount.Load() != 0 {
		t.Fatalf("expected blue to receive 0 messages (closed), got %d", blue.sendCount.Load())
	}
}

func TestCutoverShadow_PromotesShadowToActive(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx := context.Background()
	blue := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	green := &countingProcessServer{mockProcessServer: &mockProcessServer{}}

	// Register blue (gen 1) then green (gen 2) — auto-cutover promotes green.
	sm.AddStream(ctx, "sr", "ns", "step", blue, 1)
	sm.AddStream(ctx, "sr", "ns", "step", green, 2)

	// Shadow should already be cleared by auto-cutover.
	if sm.HasShadow("sr", "ns", "step") {
		t.Fatal("expected shadow to be cleared by auto-cutover")
	}

	// Calling CutoverShadow again should return false (no shadow).
	if sm.CutoverShadow("sr", "ns", "step") {
		t.Fatal("cutover should fail when shadow already auto-promoted")
	}

	// Messages should route to green (auto-promoted).
	msg := &transportpb.DataPacket{Metadata: map[string]string{"i": "1"}}
	if ok := sm.SendOrBuffer(ctx, "sr", "ns", "step", msg); !ok {
		t.Fatal("expected send to succeed")
	}
	if green.sendCount.Load() != 1 {
		t.Fatalf("expected green to receive 1 message, got %d", green.sendCount.Load())
	}

	// Verify generation updated.
	key := sm.streamKey("sr", "ns", "step")
	stateVal, _ := sm.states.Load(key)
	state := stateVal.(*streamState)
	state.mu.Lock()
	gen := state.generation
	state.mu.Unlock()
	if gen != 2 {
		t.Fatalf("expected active generation 2 after auto-cutover, got %d", gen)
	}
}

func TestCutoverShadow_NoShadowReturnsFalse(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx := context.Background()
	s := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	sm.AddStream(ctx, "sr", "ns", "step", s, 1)

	if sm.CutoverShadow("sr", "ns", "step") {
		t.Fatal("cutover should fail when no shadow exists")
	}
}

func TestRemoveStream_ShadowCleanup(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx := context.Background()
	blue := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	green := &countingProcessServer{mockProcessServer: &mockProcessServer{}}

	sm.AddStream(ctx, "sr", "ns", "step", blue, 1)
	greenEntry := sm.AddStream(ctx, "sr", "ns", "step", green, 2)

	// After auto-cutover, shadow is cleared and green is active.
	if sm.HasShadow("sr", "ns", "step") {
		t.Fatal("shadow should be cleared by auto-cutover")
	}

	// Remove active (green) stream.
	sm.RemoveStream("sr", "ns", "step", greenEntry)

	// Active stream should be removed.
	if sm.HasStream("sr", "ns", "step") {
		t.Fatal("active stream should be removed after RemoveStream")
	}
}

func TestAddStream_ThirdGenerationReplacesShadow(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx := context.Background()
	blue := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	green := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	green2 := &countingProcessServer{mockProcessServer: &mockProcessServer{}}

	sm.AddStream(ctx, "sr", "ns", "step", blue, 1)
	sm.AddStream(ctx, "sr", "ns", "step", green, 2) // auto-cutovers: green becomes active
	// Third connector with gen 3 — sees gen 2 as active, auto-cutovers to gen 3.
	entry3 := sm.AddStream(ctx, "sr", "ns", "step", green2, 3)
	if entry3 == nil {
		t.Fatal("expected third entry")
	}

	// Shadow should be cleared by auto-cutover.
	if sm.HasShadow("sr", "ns", "step") {
		t.Fatal("expected shadow to be cleared after auto-cutover")
	}

	// Active stream should be gen 3.
	key := sm.streamKey("sr", "ns", "step")
	val, _ := sm.streams.Load(key)
	if val != entry3 {
		t.Fatal("expected gen 3 to be the active stream")
	}

	stateVal, _ := sm.states.Load(key)
	state := stateVal.(*streamState)
	state.mu.Lock()
	gen := state.generation
	shadowGen := state.shadowGeneration
	state.mu.Unlock()
	if gen != 3 {
		t.Fatalf("expected active generation 3, got %d", gen)
	}
	if shadowGen != 0 {
		t.Fatalf("expected shadow generation 0 after auto-cutover, got %d", shadowGen)
	}
}

func TestAddStream_AutoCutoverOnShadowConnect(t *testing.T) {
	sm := NewStreamManager(nil)
	ctx := context.Background()
	blue := &countingProcessServer{mockProcessServer: &mockProcessServer{}}
	green := &countingProcessServer{mockProcessServer: &mockProcessServer{}}

	// 1. Add blue stream with gen=1.
	sm.AddStream(ctx, "sr", "ns", "step", blue, 1)

	// 2. Add green stream with gen=2 — triggers auto-cutover.
	sm.AddStream(ctx, "sr", "ns", "step", green, 2)

	// 3. Verify green is now the active stream (auto-promoted).
	key := sm.streamKey("sr", "ns", "step")
	val, _ := sm.streams.Load(key)
	entry, ok := val.(*streamEntry)
	if !ok || entry == nil {
		t.Fatal("expected active stream entry")
	}
	if entry.generation != 2 {
		t.Fatalf("expected active generation 2, got %d", entry.generation)
	}

	// 4. Verify shadow map is empty.
	if sm.HasShadow("sr", "ns", "step") {
		t.Fatal("expected no shadow after auto-cutover")
	}

	// 5. Verify messages route to green.
	msg := &transportpb.DataPacket{Metadata: map[string]string{"i": "auto"}}
	if ok := sm.SendOrBuffer(ctx, "sr", "ns", "step", msg); !ok {
		t.Fatal("expected send to succeed")
	}
	if green.sendCount.Load() != 1 {
		t.Fatalf("expected green to receive 1 message, got %d", green.sendCount.Load())
	}

	// 6. Verify blue stream was closed (did not receive the message).
	if blue.sendCount.Load() != 0 {
		t.Fatalf("expected blue to receive 0 messages after cutover, got %d", blue.sendCount.Load())
	}
}
