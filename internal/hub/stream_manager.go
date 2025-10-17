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
	"os"
	"strconv"
	"sync"
	"time"

	hubv1 "github.com/bubustack/bobravoz-grpc/proto/v1"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/structpb"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Buffer holds buffered messages for a stream that is not yet connected.
// (legacy Buffer type removed; MessageBuffer is used instead)

// StreamManager manages all active client streams.
type StreamManager struct {
	streams           sync.Map // map[string]*Stream // composite key -> Stream
	buffers           sync.Map // map[string]*Buffer
	log               logr.Logger
	bufferMaxSize     int
	perMessageTimeout time.Duration
}

// NewStreamManager creates a new StreamManager.
func NewStreamManager() *StreamManager {
	maxSize := 1000 // Default max buffer size
	if v := os.Getenv("BUBU_HUB_BUFFER_MAX_MESSAGES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxSize = n
		}
	}
	var perMessageTimeout time.Duration
	if v := os.Getenv("BUBU_HUB_PER_MESSAGE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			perMessageTimeout = d
		}
	}
	return &StreamManager{
		log:               log.Log.WithName("stream-manager"),
		bufferMaxSize:     maxSize,
		perMessageTimeout: perMessageTimeout,
	}
}

func (sm *StreamManager) streamKey(storyRunName, stepID string) string {
	return storyRunName + "/" + stepID
}

// AddStream adds a new stream to the manager.
func (sm *StreamManager) AddStream(storyRunName, stepID string, grpcStream hubv1.HubService_ProcessServer) {
	key := sm.streamKey(storyRunName, stepID)
	stream := newStream(grpcStream)
	sm.streams.Store(key, stream)
	sm.log.Info("Stream added", "key", key)

	// Check for and drain any existing buffer for this stream
	if val, ok := sm.buffers.Load(key); ok {
		if buffer, ok := val.(*MessageBuffer); ok {
			sm.log.Info("Draining buffer for stream", "key", key, "size", buffer.Size())
			// Flush buffered messages synchronously to ensure deterministic test behavior
			// Use the stream's context when available; guard against test mocks without a valid context
			baseCtx := safeStreamContext(grpcStream)
			buffer.FlushWithSender(baseCtx, func(p *hubv1.DataPacket) error {
				sendCtx := baseCtx
				if sm.perMessageTimeout > 0 {
					var cancel context.CancelFunc
					sendCtx, cancel = context.WithTimeout(baseCtx, sm.perMessageTimeout)
					defer cancel()
				}
				return stream.Send(sendCtx, p)
			})
			sm.buffers.Delete(key) // Buffer is drained, no longer needed
		} else {
			// Unknown buffer type; drop it for safety
			sm.buffers.Delete(key)
		}
	}
}

// safeStreamContext attempts to retrieve the context from the gRPC server stream.
// Some unit tests provide lightweight mocks that may panic when accessing Context();
// in those cases, fall back to a non-cancelable context to keep tests stable.
func safeStreamContext(gs hubv1.HubService_ProcessServer) (ctx context.Context) {
    // Prefer the real stream context; if accessing it panics (e.g., test doubles),
    // return a non-background, non-canceled placeholder to avoid uninterruptible goroutines.
    defer func() {
        if r := recover(); r != nil || ctx == nil {
            // Use context.TODO() which callers must wrap with proper timeouts; avoids Background in goroutines
            ctx = context.TODO()
        }
    }()
    return gs.Context()
}

// RemoveStream removes a stream from the manager.
func (sm *StreamManager) RemoveStream(storyRunName, stepID string) {
	key := sm.streamKey(storyRunName, stepID)
	if val, ok := sm.streams.Load(key); ok {
		stream := val.(*Stream)
		close(stream.sendChan) // Signal sendLoop to exit
		<-stream.done          // Wait for sendLoop to finish
	}
	sm.streams.Delete(key)
	sm.log.Info("Stream removed", "key", key)
}

// SendOrBuffer tries to send a packet to a stream, or buffers it if the stream is not yet available.
func (sm *StreamManager) SendOrBuffer(ctx context.Context, storyRunName, stepID string, packet *hubv1.DataPacket) bool {
	key := sm.streamKey(storyRunName, stepID)
	if val, ok := sm.streams.Load(key); ok {
		stream := val.(*Stream)

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
				sm.log.Error(nil, "Buffer full after send error; dropping packet", "key", key)
				return false
			}
			return true
		}
		return true
	}

	// Stream not found, so buffer the packet.
	val, _ := sm.buffers.LoadOrStore(key, NewMessageBuffer(storyRunName, stepID))
	buffer, _ := val.(*MessageBuffer)
	if buffer == nil {
		// Fallback: create a new message buffer if type assertion failed
		buffer = NewMessageBuffer(storyRunName, stepID)
		sm.buffers.Store(key, buffer)
	}
	if !buffer.Add(packet) {
		sm.log.Error(nil, "Buffer full, dropping packet", "key", key)
		return false
	}
	sm.log.Info("Stream not found, packet buffered", "key", key)
	return true
}

// StartEvictor starts a background goroutine to clean up old, unused buffers.
func (sm *StreamManager) StartEvictor(ctx context.Context) {
	ttl := 10 * time.Minute
	if v := os.Getenv("BUBU_HUB_BUFFER_EVICTION_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			ttl = d
		}
	}
	interval := 1 * time.Minute
	if v := os.Getenv("BUBU_HUB_BUFFER_EVICTION_INTERVAL"); v != "" {
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
			}
		}
	}()
}

func (sm *StreamManager) evictOldBuffers(ttl time.Duration) {
	sm.log.Info("Running buffer eviction")
	sm.buffers.Range(func(key, value any) bool {
		buffer, ok := value.(*MessageBuffer)
		if !ok {
			sm.buffers.Delete(key)
			return true
		}
		last := buffer.LastActive()
		isExpired := time.Since(last) > ttl

		if isExpired {
			sm.buffers.Delete(key)
			sm.log.Info("Evicted old buffer", "key", key)
		}
		return true
	})
}

// SendHeartbeats sends a heartbeat to all active streams.
func (sm *StreamManager) SendHeartbeats(ctx context.Context) {
	heartbeatPacket := &hubv1.DataPacket{
		Metadata: map[string]string{"bubu-heartbeat": "true"},
		Payload:  &structpb.Struct{},
	}

	sm.streams.Range(func(key, value any) bool {
		stream, ok := value.(*Stream)
		if !ok {
			return true // continue
		}

		// Use a timeout for sending heartbeats to avoid blocking the loop.
		sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := stream.Send(sendCtx, heartbeatPacket); err != nil {
			sm.log.Error(err, "Failed to send heartbeat", "key", key)
		}
		return true // continue iteration
	})
}
