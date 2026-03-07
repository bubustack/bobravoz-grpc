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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bobravoz-grpc/pkg/metrics"
	"github.com/bubustack/core/contracts"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Buffer holds buffered messages for a stream that is not yet connected.
// (legacy Buffer type removed; MessageBuffer is used instead)

type streamEntry struct {
	stream *Stream
}

// StreamOptions captures per-stream delivery and flow-control settings.
type StreamOptions struct {
	RetryPolicy  *bubuv1alpha1.RetryPolicy
	BufferLimits bufferLimits
	Flow         flowControlPolicy
	Delivery     deliveryPolicy
	MaxInFlight  int
	// DrainTimeout caps how long buffered messages may live when waiting for reconnect/cutover.
	DrainTimeout time.Duration
	// DrainEnabled gates whether DrainTimeout is applied for stream-not-found/retry scenarios.
	DrainEnabled bool
}

type partitionState struct {
	nextSequence uint64
	lastAck      uint64
	unacked      map[uint64]*transportpb.DataPacket
}

const (
	defaultMaxActiveStreams = 2000
	defaultMaxBuffers       = 1000

	handoffPhasePending  = "pending"
	handoffPhaseDraining = "draining"
	handoffPhaseCutover  = "cutover"
	handoffPhaseReady    = "ready"
)

type handoffState struct {
	phase     string
	reason    string
	updatedAt time.Time
}

type streamState struct {
	mu                sync.Mutex
	streamID          string
	storyRunName      string
	storyRunNamespace string
	stepID            string
	nextSequence      uint64
	lastAck           uint64
	unacked           map[uint64]*transportpb.DataPacket
	partitions        map[string]*partitionState
	flow              flowControlPolicy
	delivery          deliveryPolicy
	creditsMsg        int64
	creditsBytes      int64
	windowSize        uint64
	paused            bool
	drainPaused       bool
	lastCheckpoint    time.Time
	replayLoaded      bool
	maxInFlight       int
	handoff           handoffState
}

// StreamManager manages all active client streams.
type StreamManager struct {
	streams           sync.Map // map[streamKey]*streamEntry
	buffers           sync.Map // map[streamKey]*MessageBuffer
	states            sync.Map // map[streamKey]*streamState
	storage           *storage.StorageManager
	log               logr.Logger
	bufferMaxSize     int
	perMessageTimeout time.Duration
	retryBase         time.Duration
	retryMax          time.Duration
	activeCount       atomic.Int64
	bufferCount       atomic.Int64
	maxActiveStreams  int
	maxBuffers        int
}

// NewStreamManager creates a new StreamManager.
func NewStreamManager(store *storage.StorageManager) *StreamManager {
	maxSize := 1000 // Default max buffer size
	if v := os.Getenv(contracts.HubBufferMaxMessagesEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxSize = n
		}
	}
	logger := log.Log.WithName("stream-manager")
	perMessageTimeout := parsePerMessageTimeoutFromEnv(logger)
	retryBase, retryMax := resolveRetryBackoff(logger)
	return &StreamManager{
		log:               logger,
		bufferMaxSize:     maxSize,
		perMessageTimeout: perMessageTimeout,
		retryBase:         retryBase,
		retryMax:          retryMax,
		storage:           store,
		maxActiveStreams:  getMaxActiveStreams(),
		maxBuffers:        getMaxBuffers(),
	}
}

func getMaxActiveStreams() int {
	if v := os.Getenv(contracts.HubMaxActiveStreamsEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultMaxActiveStreams
}

func getMaxBuffers() int {
	if v := os.Getenv(contracts.HubMaxBuffersEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultMaxBuffers
}

func (sm *StreamManager) streamKey(storyRunName, storyRunNamespace, stepID string) string {
	return storyRunNamespace + "/" + storyRunName + "/" + stepID
}

// HasStream reports whether a stream is currently registered for the StepRun key.
func (sm *StreamManager) HasStream(storyRunName, storyRunNamespace, stepID string) bool {
	key := sm.streamKey(storyRunName, storyRunNamespace, stepID)
	_, ok := sm.streams.Load(key)
	return ok
}

// AddStream adds a new stream to the manager and returns its entry handle.
func (sm *StreamManager) AddStream(ctx context.Context, storyRunName, storyRunNamespace, stepID string, grpcStream transportpb.HubService_ProcessServer) *streamEntry {
	key := sm.streamKey(storyRunName, storyRunNamespace, stepID)
	if sm.maxActiveStreams > 0 && sm.activeCount.Load() >= int64(sm.maxActiveStreams) {
		sm.log.Info("Rejecting stream; max active streams reached", "key", key, "maxActiveStreams", sm.maxActiveStreams)
		metrics.RecordHubMessageDropped(storyRunName, stepID, "max_active_streams")
		return nil
	}
	stream := newStream(ctx, grpcStream, getChannelBufferSize())
	entry := &streamEntry{stream: stream}
	_, hadStream := sm.streams.Load(key)
	sm.streams.Store(key, entry)
	sm.log.Info("Stream added", "key", key)
	sm.activeCount.Add(1)

	baseCtx := safeStreamContext(grpcStream, ctx)
	state := sm.ensureState(key, storyRunName, storyRunNamespace, stepID, flowControlPolicy{}, deliveryPolicy{}, defaultBufferLimits(), 0)
	if state != nil {
		if hadStream {
			state.setHandoff(handoffPhaseCutover, "stream_replaced")
		} else {
			state.setHandoff(handoffPhaseReady, "stream_connected")
		}
		sm.replayUnacked(baseCtx, state, stream)
	}

	// Check for and drain any existing buffer for this stream
	if val, ok := sm.buffers.Load(key); ok {
		if buffer, ok := val.(*MessageBuffer); ok {
			sm.log.Info("Draining buffer for stream", "key", key, "size", buffer.Size())
			// Flush buffered messages synchronously to ensure deterministic test behavior
			// Use the stream's context when available; guard against test mocks without a valid context
			// baseCtx already computed for this stream
			_, flushErr := buffer.FlushWithSenderAndPolicy(baseCtx, func(p *transportpb.DataPacket) error {
				sendCtx := baseCtx
				if sm.perMessageTimeout > 0 {
					var cancel context.CancelFunc
					sendCtx, cancel = context.WithTimeout(baseCtx, sm.perMessageTimeout)
					defer cancel()
				}
				if err := stream.Send(sendCtx, p); err != nil {
					return err
				}
				sm.recordSent(state, p)
				return nil
			}, func(p *transportpb.DataPacket) bool {
				return sm.canSend(state, p)
			})
			buffer.RecordFlushResult(flushErr, sm.retryBase, sm.retryMax)
			sm.updatePauseResume(key, buffer, state)
			if buffer.Size() == 0 {
				sm.deleteBuffer(key) // Buffer drained completely
				sm.markBufferDrained(key, state, "buffer_drained")
			} else {
				sm.log.Info("Buffer partially flushed; retaining for retry",
					"key", key,
					"remaining", buffer.Size(),
				)
				if state != nil {
					state.setHandoff(handoffPhaseDraining, "buffer_pending")
				}
			}
		} else {
			// Unknown buffer type; drop it for safety
			sm.deleteBuffer(key)
		}
	} else if state != nil {
		// No buffer found — if state is stuck in cutover (from a previous stream
		// replacement with no backlog), advance it to ready now.
		state.mu.Lock()
		phase := state.handoff.phase
		state.mu.Unlock()
		if phase == handoffPhaseCutover {
			state.setHandoff(handoffPhaseReady, "no_buffer")
		}
	}
	return entry
}

// safeStreamContext returns the stream context when available or falls back to the provided context.
func safeStreamContext(gs transportpb.HubService_ProcessServer, fallback context.Context) context.Context {
	if gs != nil {
		if streamCtx := gs.Context(); streamCtx != nil {
			return streamCtx
		}
	}
	logger := log.Log.WithName("stream-manager")
	if fallback != nil {
		logger.Error(fmt.Errorf("gRPC stream provided nil context"),
			"BUG: falling back to caller context to preserve cancellation")
		return fallback
	}
	logger.Error(fmt.Errorf("gRPC stream provided nil context and no fallback"),
		"BUG: creating canceled context to avoid leaks")
	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	return ctx
}

// RemoveStream removes a stream from the manager when the supplied entry still matches.
func (sm *StreamManager) RemoveStream(storyRunName, storyRunNamespace, stepID string, entry *streamEntry) {
	if entry == nil {
		return
	}
	key := sm.streamKey(storyRunName, storyRunNamespace, stepID)
	entry.stream.Close()
	if val, ok := sm.streams.Load(key); ok {
		if val == entry {
			sm.streams.Delete(key)
			sm.log.Info("Stream removed", "key", key)
			sm.activeCount.Add(-1)
			if stateVal, ok := sm.states.Load(key); ok {
				if state, ok := stateVal.(*streamState); ok {
					state.setHandoff(handoffPhasePending, "stream_removed")
				}
			}
			return
		}
	}
	sm.log.V(1).Info("Skip removing stream; newer stream active", "key", key)
}

// SendOrBuffer tries to send a packet to a stream, or buffers it if the stream is not yet available.
func (sm *StreamManager) SendOrBuffer(ctx context.Context, storyRunName, storyRunNamespace, stepID string, packet *transportpb.DataPacket) bool {
	return sm.SendOrBufferWithOptions(ctx, storyRunName, storyRunNamespace, stepID, packet, StreamOptions{
		BufferLimits: defaultBufferLimits(),
	})
}

// SendOrBufferWithPolicy applies retry policy when buffering or retrying.
func (sm *StreamManager) SendOrBufferWithPolicy(ctx context.Context, storyRunName, storyRunNamespace, stepID string, packet *transportpb.DataPacket, policy *bubuv1alpha1.RetryPolicy) bool {
	return sm.SendOrBufferWithOptions(ctx, storyRunName, storyRunNamespace, stepID, packet, StreamOptions{
		RetryPolicy:  policy,
		BufferLimits: defaultBufferLimits(),
	})
}

// SendOrBufferWithPolicyAndLimits applies retry policy and buffer limits when buffering or retrying.
func (sm *StreamManager) SendOrBufferWithPolicyAndLimits(ctx context.Context, storyRunName, storyRunNamespace, stepID string, packet *transportpb.DataPacket, policy *bubuv1alpha1.RetryPolicy, limits bufferLimits) bool {
	return sm.SendOrBufferWithOptions(ctx, storyRunName, storyRunNamespace, stepID, packet, StreamOptions{
		RetryPolicy:  policy,
		BufferLimits: limits,
	})
}

// SendOrBufferWithOptions applies retry, buffering, flow-control, and delivery policies.
func (sm *StreamManager) SendOrBufferWithOptions(ctx context.Context, storyRunName, storyRunNamespace, stepID string, packet *transportpb.DataPacket, opts StreamOptions) bool {
	key := sm.streamKey(storyRunName, storyRunNamespace, stepID)
	if ctx != nil && ctx.Err() != nil {
		metrics.RecordHubMessageDropped(storyRunName, stepID, "context_done")
		return false
	}
	if packet == nil {
		metrics.RecordHubMessageDropped(storyRunName, stepID, "nil_packet")
		return false
	}

	limits := normalizeBufferLimits(opts.BufferLimits)
	allowRetry := policyAllowsRetry(opts.RetryPolicy)
	state := sm.ensureState(key, storyRunName, storyRunNamespace, stepID, opts.Flow, opts.Delivery, limits, opts.MaxInFlight)
	packet = sm.assignEnvelope(state, packet, opts.Delivery)

	// DEBUG: Log audio state in packet
	hasAudio := packet.GetAudio() != nil
	audioPcmLen := 0
	if hasAudio {
		audioPcmLen = len(packet.GetAudio().GetPcm())
	}
	sm.log.Info("SendOrBuffer called",
		"key", key,
		"storyRun", storyRunName,
		"step", stepID,
		"hasAudio", hasAudio,
		"audioPcmLen", audioPcmLen)

	if val, ok := sm.streams.Load(key); ok {
		entry := val.(*streamEntry)
		stream := entry.stream
		sm.log.Info("Stream found, sending packet directly",
			"key", key,
			"hasAudio", hasAudio,
			"audioPcmLen", audioPcmLen)

		if bval, ok := sm.buffers.Load(key); ok {
			if buffer, ok := bval.(*MessageBuffer); ok {
				buffer.ApplyLimits(limits)
				if opts.RetryPolicy != nil {
					if !allowRetry {
						buffer.DropAll("retry_disabled")
						sm.deleteBuffer(key)
					} else {
						buffer.ApplyRetryPolicy(opts.RetryPolicy, sm.retryMax)
					}
				}
				if buffer.Size() > 0 {
					if !buffer.ShouldRetry(time.Now()) {
						if !allowRetry {
							metrics.RecordHubMessageDropped(storyRunName, stepID, "retry_disabled")
							return false
						}
						if ctx != nil {
							if deadline, ok := ctx.Deadline(); ok {
								buffer.ApplyDeadline(deadline)
							}
						}
						return sm.bufferPacket(ctx, key, storyRunName, stepID, packet, opts.RetryPolicy, limits, allowRetry, state, opts.DrainTimeout, opts.DrainEnabled, "retry_wait")
					}
					sm.log.Info("Draining buffer before send", "key", key, "size", buffer.Size())
					baseCtx := ctx
					if baseCtx == nil {
						baseCtx = context.Background()
					}
					_, flushErr := buffer.FlushWithSenderAndPolicy(baseCtx, func(p *transportpb.DataPacket) error {
						sendCtx := baseCtx
						if sm.perMessageTimeout > 0 {
							var cancel context.CancelFunc
							sendCtx, cancel = context.WithTimeout(baseCtx, sm.perMessageTimeout)
							defer cancel()
						}
						if err := stream.Send(sendCtx, p); err != nil {
							return err
						}
						sm.recordSent(state, p)
						return nil
					}, func(p *transportpb.DataPacket) bool {
						return sm.canSend(state, p)
					})
					buffer.RecordFlushResult(flushErr, sm.retryBase, sm.retryMax)
					sm.updatePauseResume(key, buffer, state)
					if buffer.Size() == 0 {
						sm.deleteBuffer(key)
						sm.markBufferDrained(key, state, "buffer_drained")
					} else {
						if !allowRetry {
							metrics.RecordHubMessageDropped(storyRunName, stepID, "retry_disabled")
							return false
						}
						// Preserve ordering: buffer the new packet when older messages remain.
						if ctx != nil {
							if deadline, ok := ctx.Deadline(); ok {
								buffer.ApplyDeadline(deadline)
							}
						}
						if state != nil {
							state.setHandoff(handoffPhaseDraining, "buffer_pending")
						}
						return sm.bufferPacket(ctx, key, storyRunName, stepID, packet, opts.RetryPolicy, limits, allowRetry, state, opts.DrainTimeout, opts.DrainEnabled, "buffer_drain")
					}
				}
			} else {
				sm.deleteBuffer(key)
			}
		}

		if !sm.canSend(state, packet) {
			return sm.bufferPacket(ctx, key, storyRunName, stepID, packet, opts.RetryPolicy, limits, allowRetry, state, opts.DrainTimeout, opts.DrainEnabled, "flow_control")
		}

		// Send directly (Stream.Send handles serialization and context)
		if err := stream.Send(ctx, packet); err != nil {
			sm.log.Error(err, "Send failed; buffering packet for retry", "key", key)
			return sm.bufferPacket(ctx, key, storyRunName, stepID, packet, opts.RetryPolicy, limits, allowRetry, state, opts.DrainTimeout, opts.DrainEnabled, "send_failed")
		}
		sm.recordSent(state, packet)
		sm.log.Info("Packet successfully delivered to stream", "key", key)
		return true
	}

	// Stream not found, so buffer the packet.
	return sm.bufferPacket(ctx, key, storyRunName, stepID, packet, opts.RetryPolicy, limits, allowRetry, state, opts.DrainTimeout, opts.DrainEnabled, "stream_not_found")
}

func policyAllowsRetry(policy *bubuv1alpha1.RetryPolicy) bool {
	if policy == nil {
		return true
	}
	maxRetries := int32(3)
	if policy.MaxRetries != nil {
		maxRetries = *policy.MaxRetries
	}
	return maxRetries > 0
}

func (sm *StreamManager) ensureState(key, storyRunName, storyRunNamespace, stepID string, flow flowControlPolicy, delivery deliveryPolicy, limits bufferLimits, maxInFlight int) *streamState {
	if key == "" {
		return nil
	}
	candidate := &streamState{
		streamID:          buildStreamID(storyRunName, storyRunNamespace, stepID),
		storyRunName:      storyRunName,
		storyRunNamespace: storyRunNamespace,
		stepID:            stepID,
		nextSequence:      1,
		handoff: handoffState{
			phase:     handoffPhasePending,
			updatedAt: time.Now(),
		},
	}
	actual, loaded := sm.states.LoadOrStore(key, candidate)
	state := actual.(*streamState)
	if !loaded {
		// We stored the new state — initialise it.
		state.applyPolicies(flow, delivery, limits, maxInFlight)
		sm.loadCheckpoint(state)
		return state
	}
	// A concurrent goroutine already stored a state; update the existing one.
	state.mu.Lock()
	if state.streamID == "" {
		state.streamID = buildStreamID(storyRunName, storyRunNamespace, stepID)
	}
	if state.storyRunName == "" {
		state.storyRunName = storyRunName
	}
	if state.storyRunNamespace == "" {
		state.storyRunNamespace = storyRunNamespace
	}
	if state.stepID == "" {
		state.stepID = stepID
	}
	if state.handoff.phase == "" {
		state.handoff.phase = handoffPhasePending
		state.handoff.updatedAt = time.Now()
	}
	state.mu.Unlock()
	state.applyPolicies(flow, delivery, limits, maxInFlight)
	sm.loadCheckpoint(state)
	return state
}

func buildStreamID(storyRunName, storyRunNamespace, stepID string) string {
	if storyRunNamespace == "" {
		return fmt.Sprintf("%s/%s", storyRunName, stepID)
	}
	return fmt.Sprintf("%s/%s/%s", storyRunNamespace, storyRunName, stepID)
}

func (s *streamState) applyPolicies(flow flowControlPolicy, delivery deliveryPolicy, limits bufferLimits, maxInFlight int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if flow.mode != "" && flow != s.flow {
		s.flow = flow
		s.resetCreditsLocked(limits)
	}
	if delivery.ordering != "" || delivery.semantics != "" || delivery.replay.mode != "" {
		if delivery != s.delivery {
			s.delivery = delivery
			if s.delivery.atLeastOnce() {
				if s.delivery.ordering == orderingPerPartition {
					if s.partitions == nil {
						s.partitions = make(map[string]*partitionState)
					}
					for _, partition := range s.partitions {
						if partition.unacked == nil {
							partition.unacked = make(map[uint64]*transportpb.DataPacket)
						}
					}
				} else if s.unacked == nil {
					s.unacked = make(map[uint64]*transportpb.DataPacket)
				}
			}
		}
	}
	if maxInFlight > 0 {
		s.maxInFlight = maxInFlight
	} else if maxInFlight == 0 {
		s.maxInFlight = 0
	}
	if s.nextSequence == 0 {
		s.nextSequence = 1
	}
}

func (s *streamState) setHandoff(phase, reason string) {
	if s == nil || phase == "" {
		return
	}
	s.mu.Lock()
	s.handoff.phase = phase
	s.handoff.reason = reason
	s.handoff.updatedAt = time.Now()
	s.mu.Unlock()
}

func (s *streamState) setDrainPaused(paused bool) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	changed := s.drainPaused != paused
	s.drainPaused = paused
	s.mu.Unlock()
	return changed
}

func (s *streamState) resetCreditsLocked(limits bufferLimits) {
	switch s.flow.mode {
	case flowControlCredits:
		msgCredits := s.flow.initialCreditsMsg
		byteCredits := s.flow.initialCreditsBytes
		if msgCredits <= 0 {
			msgCredits = limits.maxMessages
		}
		if byteCredits <= 0 {
			byteCredits = limits.maxBytes
		}
		s.creditsMsg = int64(msgCredits)
		s.creditsBytes = int64(byteCredits)
	case flowControlWindow:
		window := s.flow.initialCreditsMsg
		if window <= 0 {
			window = limits.maxMessages
		}
		if window <= 0 {
			window = 1
		}
		s.windowSize = uint64(window)
		s.creditsMsg = int64(window)
		s.creditsBytes = int64(limits.maxBytes)
	default:
		s.creditsMsg = 0
		s.creditsBytes = 0
		s.windowSize = 0
	}
}

func (sm *StreamManager) assignEnvelope(state *streamState, packet *transportpb.DataPacket, delivery deliveryPolicy) *transportpb.DataPacket {
	if packet == nil || state == nil {
		return packet
	}
	if !(delivery.orderingEnabled() || delivery.atLeastOnce()) && packet.GetEnvelope() == nil {
		return packet
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	envelope := packet.GetEnvelope()
	if envelope == nil {
		envelope = &transportpb.StreamEnvelope{}
		packet.Envelope = envelope
	}
	if envelope.StreamId == "" {
		envelope.StreamId = state.streamID
	}
	if (delivery.orderingEnabled() || delivery.atLeastOnce()) && envelope.Sequence == 0 {
		if delivery.ordering == orderingPerPartition {
			partition := envelope.GetPartition()
			partitionState := ensurePartitionStateLocked(state, partition, delivery.atLeastOnce())
			envelope.Sequence = partitionState.nextSequence
			partitionState.nextSequence++
		} else {
			if state.nextSequence == 0 {
				state.nextSequence = 1
			}
			envelope.Sequence = state.nextSequence
			state.nextSequence++
		}
	} else if envelope.Sequence > 0 {
		if delivery.ordering == orderingPerPartition {
			partition := envelope.GetPartition()
			partitionState := ensurePartitionStateLocked(state, partition, delivery.atLeastOnce())
			if envelope.Sequence >= partitionState.nextSequence {
				partitionState.nextSequence = envelope.Sequence + 1
			}
		} else if envelope.Sequence >= state.nextSequence {
			state.nextSequence = envelope.Sequence + 1
		}
	}
	return packet
}

func ensurePartitionStateLocked(state *streamState, partition string, atLeastOnce bool) *partitionState {
	if state.partitions == nil {
		state.partitions = make(map[string]*partitionState)
	}
	ps, ok := state.partitions[partition]
	if !ok {
		ps = &partitionState{nextSequence: 1}
		state.partitions[partition] = ps
	}
	if ps.nextSequence == 0 {
		ps.nextSequence = 1
	}
	if atLeastOnce && ps.unacked == nil {
		ps.unacked = make(map[uint64]*transportpb.DataPacket)
	}
	return ps
}

func applyPartitionAckLocked(partition *partitionState, ack uint64) {
	if partition == nil || ack == 0 || ack <= partition.lastAck {
		return
	}
	partition.lastAck = ack
	if partition.nextSequence <= ack {
		partition.nextSequence = ack + 1
	}
	if len(partition.unacked) == 0 {
		return
	}
	for seq := range partition.unacked {
		if seq <= ack {
			delete(partition.unacked, seq)
		}
	}
}

func applyStreamAckLocked(state *streamState, ack uint64) {
	if state == nil || ack == 0 || ack <= state.lastAck {
		return
	}
	state.lastAck = ack
	if state.nextSequence <= ack {
		state.nextSequence = ack + 1
	}
	if len(state.unacked) == 0 {
		return
	}
	for seq := range state.unacked {
		if seq <= ack {
			delete(state.unacked, seq)
		}
	}
}

func (sm *StreamManager) canSend(state *streamState, packet *transportpb.DataPacket) bool {
	if state == nil {
		return true
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.paused {
		return false
	}
	if state.maxInFlight > 0 && state.delivery.atLeastOnce() {
		if state.delivery.ordering == orderingPerPartition {
			partition := packet.GetEnvelope().GetPartition()
			partitionState := ensurePartitionStateLocked(state, partition, true)
			if len(partitionState.unacked) >= state.maxInFlight {
				return false
			}
		} else if len(state.unacked) >= state.maxInFlight {
			return false
		}
	}
	switch state.flow.mode {
	case flowControlCredits:
		if state.creditsMsg <= 0 {
			return false
		}
		if state.creditsBytes >= 0 {
			size := int64(proto.Size(packet))
			if size > state.creditsBytes {
				return false
			}
		}
	case flowControlWindow:
		if state.windowSize <= 0 {
			return false
		}
		seq := packet.GetEnvelope().GetSequence()
		if seq == 0 {
			return true
		}
		if state.delivery.ordering == orderingPerPartition {
			partition := packet.GetEnvelope().GetPartition()
			partitionState := ensurePartitionStateLocked(state, partition, state.delivery.atLeastOnce())
			if seq > partitionState.lastAck+state.windowSize {
				return false
			}
		} else if seq > state.lastAck+state.windowSize {
			return false
		}
	}
	return true
}

func (sm *StreamManager) recordSent(state *streamState, packet *transportpb.DataPacket) {
	if state == nil || packet == nil {
		return
	}
	size := int64(proto.Size(packet))
	pending := 0
	state.mu.Lock()
	switch state.flow.mode {
	case flowControlCredits:
		if state.creditsMsg > 0 {
			state.creditsMsg--
		}
		if state.creditsBytes > 0 {
			state.creditsBytes -= size
			if state.creditsBytes < 0 {
				state.creditsBytes = 0
			}
		}
	}
	if state.delivery.atLeastOnce() {
		seq := packet.GetEnvelope().GetSequence()
		if seq > 0 {
			if state.delivery.ordering == orderingPerPartition {
				partition := packet.GetEnvelope().GetPartition()
				partitionState := ensurePartitionStateLocked(state, partition, true)
				if seq > partitionState.lastAck { // only track if not already acked
					partitionState.unacked[seq] = packet
				}
			} else {
				if state.unacked == nil {
					state.unacked = make(map[uint64]*transportpb.DataPacket)
				}
				if seq > state.lastAck { // only track if not already acked
					state.unacked[seq] = packet
				}
			}
		}
	}
	replay := state.delivery.replay.mode == replayDurable
	if replay && state.delivery.atLeastOnce() {
		if state.delivery.ordering == orderingPerPartition {
			for _, partState := range state.partitions {
				if partState != nil {
					pending += len(partState.unacked)
				}
			}
		} else {
			pending = len(state.unacked)
		}
	}
	state.mu.Unlock()
	if replay {
		sm.maybeCheckpoint(state)
		metrics.RecordHubReplayPending(state.storyRunName, state.stepID, pending)
	}
}

func (sm *StreamManager) bufferPacket(ctx context.Context, key, storyRunName, stepID string, packet *transportpb.DataPacket, policy *bubuv1alpha1.RetryPolicy, limits bufferLimits, allowRetry bool, state *streamState, drainTimeout time.Duration, drainEnabled bool, reason string) bool {
	if !allowRetry {
		metrics.RecordHubMessageDropped(storyRunName, stepID, "retry_disabled")
		return false
	}
	sm.log.Info("Buffering packet", "key", key, "reason", reason)
	buffer := sm.getOrCreateBuffer(key, storyRunName, stepID, limits)
	if buffer == nil {
		metrics.RecordHubMessageDropped(storyRunName, stepID, "max_buffers")
		return false
	}
	buffer.ApplyLimits(limits)
	if !buffer.ApplyRetryPolicy(policy, sm.retryMax) {
		buffer.DropAll("retry_disabled")
		sm.deleteBuffer(key)
		metrics.RecordHubMessageDropped(storyRunName, stepID, "retry_disabled")
		return false
	}
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			buffer.ApplyDeadline(deadline)
		}
	}
	if drainEnabled && drainTimeout > 0 && shouldApplyDrainTimeout(reason) {
		buffer.ApplyDeadline(time.Now().Add(drainTimeout))
	}
	if drainEnabled && state != nil {
		state.setHandoff(handoffPhaseDraining, reason)
		if shouldApplyDrainTimeout(reason) {
			if state.setDrainPaused(true) {
				sm.sendFlow(key, &transportpb.FlowControl{Pause: true})
			}
		}
	}
	if !buffer.Add(packet) {
		sm.log.Info("Buffer full, dropping packet", "key", key, "reason", reason)
		return false
	}
	sm.updatePauseResume(key, buffer, state)
	sm.log.Info("Packet buffered successfully", "key", key, "bufferSize", buffer.Size())
	return true
}

func (sm *StreamManager) getOrCreateBuffer(key, storyRunName, stepID string, limits bufferLimits) *MessageBuffer {
	if sm == nil {
		return nil
	}
	if val, ok := sm.buffers.Load(key); ok {
		buffer, _ := val.(*MessageBuffer)
		if buffer == nil {
			sm.deleteBuffer(key)
			return nil
		}
		return buffer
	}
	if sm.maxBuffers > 0 && sm.bufferCount.Load() >= int64(sm.maxBuffers) {
		sm.log.Info("Buffer cap reached; dropping packet", "key", key, "maxBuffers", sm.maxBuffers)
		return nil
	}
	buffer := NewMessageBufferWithLimits(storyRunName, stepID, limits)
	val, loaded := sm.buffers.LoadOrStore(key, buffer)
	if loaded {
		if existing, ok := val.(*MessageBuffer); ok && existing != nil {
			return existing
		}
		sm.deleteBuffer(key)
	}
	sm.bufferCount.Add(1)
	return buffer
}

func (sm *StreamManager) deleteBuffer(key string) {
	if sm == nil {
		return
	}
	if _, loaded := sm.buffers.LoadAndDelete(key); loaded {
		sm.bufferCount.Add(-1)
	}
}

func shouldApplyDrainTimeout(reason string) bool {
	switch reason {
	case "stream_not_found", "send_failed", "retry_wait":
		return true
	default:
		return false
	}
}

func (sm *StreamManager) updatePauseResume(key string, buffer *MessageBuffer, state *streamState) {
	if state == nil || buffer == nil {
		return
	}
	state.mu.Lock()
	pauseAt := state.flow.pauseThresholdPct
	resumeAt := state.flow.resumeThresholdPct
	paused := state.paused
	drainPaused := state.drainPaused
	state.mu.Unlock()
	if pauseAt <= 0 {
		return
	}
	utilization := buffer.Utilization()
	if !paused && utilization >= pauseAt {
		state.mu.Lock()
		state.paused = true
		state.mu.Unlock()
		if !drainPaused {
			sm.sendFlow(key, &transportpb.FlowControl{Pause: true})
		}
		return
	}
	if paused && resumeAt > 0 && utilization <= resumeAt {
		state.mu.Lock()
		state.paused = false
		state.mu.Unlock()
		if !drainPaused {
			sm.sendFlow(key, &transportpb.FlowControl{Resume: true})
		}
	}
}

func (sm *StreamManager) markBufferDrained(key string, state *streamState, reason string) {
	if state == nil {
		return
	}
	state.mu.Lock()
	paused := state.paused
	drainPaused := state.drainPaused
	state.drainPaused = false
	state.mu.Unlock()
	if drainPaused && !paused {
		sm.sendFlow(key, &transportpb.FlowControl{Resume: true})
	}
	state.setHandoff(handoffPhaseReady, reason)
}

func (sm *StreamManager) sendFlow(key string, flow *transportpb.FlowControl) {
	if flow == nil {
		return
	}
	val, ok := sm.streams.Load(key)
	if !ok {
		return
	}
	entry, ok := val.(*streamEntry)
	if !ok || entry.stream == nil {
		return
	}
	if err := entry.stream.SendFlow(context.Background(), flow); err != nil {
		sm.log.V(1).Info("Failed to send flow control update", "key", key, "error", err)
	}
}

// ApplyFlow updates flow-control and ack state based on connector feedback.
func (sm *StreamManager) ApplyFlow(storyRunName, storyRunNamespace, stepID string, flow *transportpb.FlowControl) {
	if flow == nil {
		return
	}
	key := sm.streamKey(storyRunName, storyRunNamespace, stepID)
	state := sm.ensureState(key, storyRunName, storyRunNamespace, stepID, flowControlPolicy{}, deliveryPolicy{}, defaultBufferLimits(), 0)
	if state == nil {
		return
	}
	state.mu.Lock()
	replay := state.delivery.replay.mode == replayDurable
	pending := 0
	type ackSample struct {
		partition string
		ack       uint64
	}
	var ackSamples []ackSample
	if flow.CreditsMessages > 0 {
		state.creditsMsg += int64(flow.CreditsMessages)
	}
	if flow.CreditsBytes > 0 {
		state.creditsBytes += int64(flow.CreditsBytes)
	}
	if state.delivery.ordering == orderingPerPartition {
		if len(flow.PartitionAcks) > 0 {
			for _, ack := range flow.PartitionAcks {
				if ack == nil || ack.Ack == 0 {
					continue
				}
				partitionState := ensurePartitionStateLocked(state, ack.Partition, state.delivery.atLeastOnce())
				applyPartitionAckLocked(partitionState, ack.Ack)
				if replay {
					ackSamples = append(ackSamples, ackSample{partition: ack.Partition, ack: partitionState.lastAck})
				}
			}
		} else if flow.Ack > 0 {
			partitionState := ensurePartitionStateLocked(state, "", state.delivery.atLeastOnce())
			applyPartitionAckLocked(partitionState, flow.Ack)
			if replay {
				ackSamples = append(ackSamples, ackSample{partition: "", ack: partitionState.lastAck})
			}
		}
	} else if flow.Ack > 0 {
		applyStreamAckLocked(state, flow.Ack)
		if replay {
			ackSamples = append(ackSamples, ackSample{partition: "", ack: state.lastAck})
		}
	}
	if replay && state.delivery.atLeastOnce() {
		if state.delivery.ordering == orderingPerPartition {
			for _, partState := range state.partitions {
				if partState != nil {
					pending += len(partState.unacked)
				}
			}
		} else {
			pending = len(state.unacked)
		}
	}
	state.mu.Unlock()
	if replay {
		sm.maybeCheckpoint(state)
		for _, sample := range ackSamples {
			metrics.RecordHubReplayLastAck(storyRunName, stepID, sample.partition, sample.ack)
		}
		metrics.RecordHubReplayPending(storyRunName, stepID, pending)
	}
}

func (sm *StreamManager) replayUnacked(ctx context.Context, state *streamState, stream *Stream) {
	if state == nil || stream == nil {
		return
	}
	state.mu.Lock()
	if !state.delivery.atLeastOnce() {
		state.mu.Unlock()
		return
	}
	type pendingEntry struct {
		partition string
		seq       uint64
		packet    *transportpb.DataPacket
	}
	entries := make([]pendingEntry, 0)
	if state.delivery.ordering == orderingPerPartition {
		for partition, partState := range state.partitions {
			if partState == nil || len(partState.unacked) == 0 {
				continue
			}
			for seq, packet := range partState.unacked {
				entries = append(entries, pendingEntry{partition: partition, seq: seq, packet: packet})
			}
		}
	} else if len(state.unacked) > 0 {
		for seq, packet := range state.unacked {
			entries = append(entries, pendingEntry{seq: seq, packet: packet})
		}
	}
	state.mu.Unlock()

	if len(entries) == 0 {
		return
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].partition == entries[j].partition {
			return entries[i].seq < entries[j].seq
		}
		return entries[i].partition < entries[j].partition
	})
	for _, entry := range entries {
		packet := entry.packet
		if packet == nil {
			continue
		}
		if !sm.canSend(state, packet) {
			return
		}
		if err := stream.Send(ctx, packet); err != nil {
			return
		}
		sm.recordSent(state, packet)
	}
}

type replayCheckpoint struct {
	StreamID      string            `json:"streamId"`
	LastAck       uint64            `json:"lastAck"`
	PartitionAcks map[string]uint64 `json:"partitionAcks,omitempty"`
	Pending       []replayRecord    `json:"pending,omitempty"`
	UpdatedAt     time.Time         `json:"updatedAt"`
}

type replayRecord struct {
	Seq       uint64 `json:"seq"`
	Partition string `json:"partition,omitempty"`
	Packet    []byte `json:"packet"`
}

func (sm *StreamManager) loadCheckpoint(state *streamState) {
	if state == nil || sm.storage == nil {
		return
	}
	state.mu.Lock()
	if state.replayLoaded || state.delivery.replay.mode != replayDurable {
		state.mu.Unlock()
		return
	}
	state.replayLoaded = true
	path := checkpointPath(state)
	state.mu.Unlock()

	payload, err := sm.storage.ReadBlob(context.Background(), path)
	if err != nil {
		return
	}
	var checkpoint replayCheckpoint
	if err := json.Unmarshal(payload, &checkpoint); err != nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.delivery.replay.retention > 0 && time.Since(checkpoint.UpdatedAt) > state.delivery.replay.retention {
		return
	}
	if state.delivery.ordering == orderingPerPartition {
		if len(checkpoint.PartitionAcks) > 0 {
			for partition, ack := range checkpoint.PartitionAcks {
				partitionState := ensurePartitionStateLocked(state, partition, state.delivery.atLeastOnce())
				applyPartitionAckLocked(partitionState, ack)
			}
		} else if checkpoint.LastAck > 0 {
			partitionState := ensurePartitionStateLocked(state, "", state.delivery.atLeastOnce())
			applyPartitionAckLocked(partitionState, checkpoint.LastAck)
		}
		if len(checkpoint.Pending) > 0 {
			for _, record := range checkpoint.Pending {
				partitionState := ensurePartitionStateLocked(state, record.Partition, state.delivery.atLeastOnce())
				if record.Seq <= partitionState.lastAck {
					continue
				}
				var packet transportpb.DataPacket
				if err := proto.Unmarshal(record.Packet, &packet); err != nil {
					continue
				}
				partitionState.unacked[record.Seq] = &packet
				if record.Seq >= partitionState.nextSequence {
					partitionState.nextSequence = record.Seq + 1
				}
			}
		}
		for _, partitionState := range state.partitions {
			if partitionState == nil {
				continue
			}
			if partitionState.nextSequence == 0 {
				if partitionState.lastAck > 0 {
					partitionState.nextSequence = partitionState.lastAck + 1
				} else {
					partitionState.nextSequence = 1
				}
			} else if partitionState.lastAck >= partitionState.nextSequence {
				partitionState.nextSequence = partitionState.lastAck + 1
			}
		}
		return
	}
	if checkpoint.LastAck > state.lastAck {
		state.lastAck = checkpoint.LastAck
	}
	if len(checkpoint.Pending) > 0 {
		if state.unacked == nil {
			state.unacked = make(map[uint64]*transportpb.DataPacket)
		}
		for _, record := range checkpoint.Pending {
			if record.Seq <= state.lastAck {
				continue
			}
			var packet transportpb.DataPacket
			if err := proto.Unmarshal(record.Packet, &packet); err != nil {
				continue
			}
			state.unacked[record.Seq] = &packet
			if record.Seq >= state.nextSequence {
				state.nextSequence = record.Seq + 1
			}
		}
	}
	if state.nextSequence == 0 {
		state.nextSequence = state.lastAck + 1
		if state.nextSequence == 0 {
			state.nextSequence = 1
		}
	}
}

func (sm *StreamManager) maybeCheckpoint(state *streamState) {
	if state == nil || sm.storage == nil {
		return
	}
	state.mu.Lock()
	if state.delivery.replay.mode != replayDurable {
		state.mu.Unlock()
		return
	}
	interval := state.delivery.replay.checkpointInterval
	if interval <= 0 {
		interval = time.Second
	}
	now := time.Now()
	if !state.lastCheckpoint.IsZero() && now.Sub(state.lastCheckpoint) < interval {
		state.mu.Unlock()
		return
	}
	checkpoint := replayCheckpoint{
		StreamID:  state.streamID,
		LastAck:   state.lastAck,
		UpdatedAt: now,
	}
	if state.delivery.ordering == orderingPerPartition {
		if len(state.partitions) > 0 {
			checkpoint.PartitionAcks = make(map[string]uint64, len(state.partitions))
			for partition, partitionState := range state.partitions {
				if partitionState == nil {
					continue
				}
				if partitionState.lastAck > 0 {
					checkpoint.PartitionAcks[partition] = partitionState.lastAck
				}
			}
		}
		type pendingEntry struct {
			partition string
			seq       uint64
			packet    *transportpb.DataPacket
		}
		entries := make([]pendingEntry, 0)
		for partition, partitionState := range state.partitions {
			if partitionState == nil || len(partitionState.unacked) == 0 {
				continue
			}
			for seq, packet := range partitionState.unacked {
				entries = append(entries, pendingEntry{partition: partition, seq: seq, packet: packet})
			}
		}
		if len(entries) > 0 {
			sort.Slice(entries, func(i, j int) bool {
				if entries[i].partition == entries[j].partition {
					return entries[i].seq < entries[j].seq
				}
				return entries[i].partition < entries[j].partition
			})
			maxPending := sm.bufferMaxSize
			if maxPending <= 0 {
				maxPending = len(entries)
			}
			if len(entries) > maxPending {
				entries = entries[:maxPending]
			}
			checkpoint.Pending = make([]replayRecord, 0, len(entries))
			for _, entry := range entries {
				if entry.packet == nil {
					continue
				}
				encoded, err := proto.Marshal(entry.packet)
				if err != nil {
					continue
				}
				checkpoint.Pending = append(checkpoint.Pending, replayRecord{Seq: entry.seq, Partition: entry.partition, Packet: encoded})
			}
		}
	} else if len(state.unacked) > 0 {
		seqs := make([]uint64, 0, len(state.unacked))
		for seq := range state.unacked {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		maxPending := sm.bufferMaxSize
		if maxPending <= 0 {
			maxPending = len(seqs)
		}
		if len(seqs) > maxPending {
			seqs = seqs[:maxPending]
		}
		checkpoint.Pending = make([]replayRecord, 0, len(seqs))
		for _, seq := range seqs {
			packet := state.unacked[seq]
			if packet == nil {
				continue
			}
			encoded, err := proto.Marshal(packet)
			if err != nil {
				continue
			}
			checkpoint.Pending = append(checkpoint.Pending, replayRecord{Seq: seq, Packet: encoded})
		}
	}
	state.mu.Unlock()

	payload, err := json.Marshal(checkpoint)
	if err != nil {
		return
	}
	if err := sm.storage.WriteBlob(context.Background(), checkpointPath(state), "application/json", payload); err != nil {
		// Write failed — do not update lastCheckpoint so the next interval retries.
		return
	}
	state.mu.Lock()
	state.lastCheckpoint = now
	state.mu.Unlock()
}

func checkpointPath(state *streamState) string {
	return fmt.Sprintf("streams/%s/%s/%s/replay.json", state.storyRunNamespace, state.storyRunName, state.stepID)
}

// StartEvictor starts a background goroutine to flush retryable buffers and evict old, unused ones.
func (sm *StreamManager) StartEvictor(ctx context.Context) {
	ttl := 10 * time.Minute
	if v := os.Getenv(contracts.HubBufferEvictionTTLEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			ttl = d
		}
	}
	interval := 1 * time.Minute
	if v := os.Getenv(contracts.HubBufferEvictionIntervalEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			interval = d
		}
	}
	flushInterval := interval
	if v := os.Getenv(contracts.HubBufferFlushIntervalEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			flushInterval = d
		}
	}

	evictTicker := time.NewTicker(interval)
	flushTicker := time.NewTicker(flushInterval)
	go func() {
		defer evictTicker.Stop()
		defer flushTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-flushTicker.C:
				sm.flushBuffers(ctx)
			case <-evictTicker.C:
				sm.evictOldBuffers(ttl)
				sm.log.Info("Hub stream heartbeat", "activeStreams", sm.activeCount.Load(), "bufferEntries", sm.bufferSize())
			}
		}
	}()
}

func (sm *StreamManager) flushBuffers(ctx context.Context) {
	now := time.Now()
	sm.buffers.Range(func(key, value any) bool {
		buffer, ok := value.(*MessageBuffer)
		if !ok {
			if keyStr, ok := key.(string); ok {
				sm.deleteBuffer(keyStr)
			}
			return true
		}
		if !buffer.ShouldRetry(now) {
			return true
		}
		entryVal, ok := sm.streams.Load(key)
		if !ok {
			return true
		}
		entry, ok := entryVal.(*streamEntry)
		if !ok || entry.stream == nil {
			return true
		}
		keyStr, _ := key.(string)
		var state *streamState
		if sval, ok := sm.states.Load(keyStr); ok {
			state, _ = sval.(*streamState)
		}
		baseCtx := ctx
		if baseCtx == nil {
			baseCtx = context.Background()
		}
		_, flushErr := buffer.FlushWithSenderAndPolicy(baseCtx, func(p *transportpb.DataPacket) error {
			sendCtx := baseCtx
			if sm.perMessageTimeout > 0 {
				var cancel context.CancelFunc
				sendCtx, cancel = context.WithTimeout(baseCtx, sm.perMessageTimeout)
				defer cancel()
			}
			if err := entry.stream.Send(sendCtx, p); err != nil {
				return err
			}
			sm.recordSent(state, p)
			return nil
		}, func(p *transportpb.DataPacket) bool {
			return sm.canSend(state, p)
		})
		buffer.RecordFlushResult(flushErr, sm.retryBase, sm.retryMax)
		if keyStr != "" {
			sm.updatePauseResume(keyStr, buffer, state)
		}
		if buffer.Size() == 0 {
			sm.deleteBuffer(keyStr)
			sm.markBufferDrained(keyStr, state, "buffer_drained")
		}
		return true
	})
}

func resolveRetryBackoff(logger logr.Logger) (time.Duration, time.Duration) {
	base := 500 * time.Millisecond
	if d, err := parsePositiveDuration(os.Getenv(contracts.GRPCReconnectBaseBackoffEnv)); err == nil && d > 0 {
		base = d
	}
	max := 30 * time.Second
	if d, err := parsePositiveDuration(os.Getenv(contracts.GRPCReconnectMaxBackoffEnv)); err == nil && d > 0 {
		max = d
	}
	if max > 0 && base > max {
		logger.Info("Hub buffer retry base backoff exceeds max; clamping", "base", base, "max", max)
		base = max
	}
	return base, max
}

func (sm *StreamManager) bufferSize() int {
	if sm == nil {
		return 0
	}
	return int(sm.bufferCount.Load())
}

func (sm *StreamManager) evictOldBuffers(ttl time.Duration) {
	sm.log.Info("Running buffer eviction")
	sm.buffers.Range(func(key, value any) bool {
		keyStr, _ := key.(string)
		buffer, ok := value.(*MessageBuffer)
		if !ok {
			sm.deleteBuffer(keyStr)
			return true
		}
		// LastActive() now returns duration since last activity using monotonic time
		lastActiveDuration := buffer.LastActive()
		isExpired := lastActiveDuration > ttl

		if isExpired {
			sm.deleteBuffer(keyStr)
			sm.log.Info("Evicted old buffer", "key", key, "age", lastActiveDuration)
		}
		return true
	})
}

// SendHeartbeats sends a heartbeat to all active streams.
func (sm *StreamManager) SendHeartbeats(ctx context.Context) error {
	heartbeatPacket := &transportpb.DataPacket{
		Metadata: map[string]string{"bubu-heartbeat": "true"},
		Payload:  &structpb.Struct{},
	}

	var errs []error
	sm.streams.Range(func(key, value any) bool {
		entry, ok := value.(*streamEntry)
		if !ok {
			return true // continue
		}
		stream := entry.stream

		// Use a timeout for sending heartbeats to avoid blocking the loop.
		sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := stream.Send(sendCtx, heartbeatPacket); err != nil {
			sm.log.Error(err, "Failed to send heartbeat", "key", key)
			errs = append(errs, fmt.Errorf("%v: %w", key, err))
		}
		cancel()
		return true // continue iteration
	})

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
