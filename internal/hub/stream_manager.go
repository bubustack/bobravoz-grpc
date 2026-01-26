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
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bubustack/core/contracts"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/structpb"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Buffer holds buffered messages for a stream that is not yet connected.
// (legacy Buffer type removed; MessageBuffer is used instead)

type streamEntry struct {
	stream *Stream
}

// StreamManager manages all active client streams.
type StreamManager struct {
	streams           sync.Map // map[streamKey]*streamEntry
	buffers           sync.Map // map[streamKey]*MessageBuffer
	log               logr.Logger
	bufferMaxSize     int
	perMessageTimeout time.Duration
	activeCount       atomic.Int64
}

// NewStreamManager creates a new StreamManager.
func NewStreamManager() *StreamManager {
	maxSize := 1000 // Default max buffer size
	if v := os.Getenv(contracts.HubBufferMaxMessagesEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxSize = n
		}
	}
	logger := log.Log.WithName("stream-manager")
	perMessageTimeout := parsePerMessageTimeoutFromEnv(logger)
	return &StreamManager{
		log:               logger,
		bufferMaxSize:     maxSize,
		perMessageTimeout: perMessageTimeout,
	}
}

func (sm *StreamManager) streamKey(storyRunName, storyRunNamespace, stepID string) string {
	return storyRunNamespace + "/" + storyRunName + "/" + stepID
}

// AddStream adds a new stream to the manager and returns its entry handle.
func (sm *StreamManager) AddStream(ctx context.Context, storyRunName, storyRunNamespace, stepID string, grpcStream transportpb.HubService_ProcessServer) *streamEntry {
	key := sm.streamKey(storyRunName, storyRunNamespace, stepID)
	stream := newStream(ctx, grpcStream, getChannelBufferSize())
	entry := &streamEntry{stream: stream}
	sm.streams.Store(key, entry)
	sm.log.Info("Stream added", "key", key)
	sm.activeCount.Add(1)

	// Check for and drain any existing buffer for this stream
	if val, ok := sm.buffers.Load(key); ok {
		if buffer, ok := val.(*MessageBuffer); ok {
			sm.log.Info("Draining buffer for stream", "key", key, "size", buffer.Size())
			// Flush buffered messages synchronously to ensure deterministic test behavior
			// Use the stream's context when available; guard against test mocks without a valid context
			baseCtx := safeStreamContext(grpcStream, ctx)
			buffer.FlushWithSender(baseCtx, func(p *transportpb.DataPacket) error {
				sendCtx := baseCtx
				if sm.perMessageTimeout > 0 {
					var cancel context.CancelFunc
					sendCtx, cancel = context.WithTimeout(baseCtx, sm.perMessageTimeout)
					defer cancel()
				}
				return stream.Send(sendCtx, p)
			})
			if buffer.Size() == 0 {
				sm.buffers.Delete(key) // Buffer drained completely
			} else {
				sm.log.Info("Buffer partially flushed; retaining for retry",
					"key", key,
					"remaining", buffer.Size(),
				)
			}
		} else {
			// Unknown buffer type; drop it for safety
			sm.buffers.Delete(key)
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
			return
		}
	}
	sm.log.V(1).Info("Skip removing stream; newer stream active", "key", key)
}

// SendOrBuffer tries to send a packet to a stream, or buffers it if the stream is not yet available.
func (sm *StreamManager) SendOrBuffer(ctx context.Context, storyRunName, storyRunNamespace, stepID string, packet *transportpb.DataPacket) bool {
	key := sm.streamKey(storyRunName, storyRunNamespace, stepID)
	sm.log.Info("SendOrBuffer called", "key", key, "storyRun", storyRunName, "step", stepID)
	if val, ok := sm.streams.Load(key); ok {
		entry := val.(*streamEntry)
		stream := entry.stream
		sm.log.Info("Stream found, sending packet directly", "key", key)

		// Send directly (Stream.Send handles serialization and context)
		if err := stream.Send(ctx, packet); err != nil {
			sm.log.Error(err, "Send failed; buffering packet for retry", "key", key)
			// On transient send error, fall back to buffering
			bval, _ := sm.buffers.LoadOrStore(key, NewMessageBuffer(storyRunName, stepID))
			buf, _ := bval.(*MessageBuffer)
			if buf == nil {
				buf = NewMessageBuffer(storyRunName, stepID)
				sm.buffers.Store(key, buf)
			}
			if !buf.Add(packet) {
				sm.log.Info("Buffer full after send error; dropping packet", "key", key, "reason", "buffer_full")
				return false
			}
			return true
		}
		sm.log.Info("Packet successfully delivered to stream", "key", key)
		return true
	}

	// Stream not found, so buffer the packet.
	sm.log.Info("Stream NOT found, buffering packet", "key", key, "storyRun", storyRunName, "step", stepID)
	val, _ := sm.buffers.LoadOrStore(key, NewMessageBuffer(storyRunName, stepID))
	buffer, _ := val.(*MessageBuffer)
	if buffer == nil {
		// Fallback: create a new message buffer if type assertion failed
		buffer = NewMessageBuffer(storyRunName, stepID)
		sm.buffers.Store(key, buffer)
	}
	if !buffer.Add(packet) {
		sm.log.Info("Buffer full, dropping packet", "key", key, "reason", "stream_not_found")
		return false
	}
	sm.log.Info("Packet buffered successfully", "key", key, "bufferSize", buffer.Size())
	return true
}

// StartEvictor starts a background goroutine to clean up old, unused buffers.
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

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sm.evictOldBuffers(ttl)
				sm.log.Info("Hub stream heartbeat", "activeStreams", sm.activeCount.Load(), "bufferEntries", sm.bufferSize())
			}
		}
	}()
}

func (sm *StreamManager) bufferSize() int {
	count := 0
	sm.buffers.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func (sm *StreamManager) evictOldBuffers(ttl time.Duration) {
	sm.log.Info("Running buffer eviction")
	sm.buffers.Range(func(key, value any) bool {
		buffer, ok := value.(*MessageBuffer)
		if !ok {
			sm.buffers.Delete(key)
			return true
		}
		// LastActive() now returns duration since last activity using monotonic time
		lastActiveDuration := buffer.LastActive()
		isExpired := lastActiveDuration > ttl

		if isExpired {
			sm.buffers.Delete(key)
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
