package hub

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/bubustack/bobravoz-grpc/pkg/metrics"
	hubv1 "github.com/bubustack/bobravoz-grpc/proto/v1"
	"github.com/go-logr/logr"
	pbproto "google.golang.org/protobuf/proto"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// Default buffer limits - can be overridden via env vars
	defaultMaxBufferSize  = 100
	defaultMaxBufferBytes = 10 * 1024 * 1024 // 10 MB
)

var (
	// MaxBufferSize is the maximum number of messages to buffer per downstream engram
	MaxBufferSize = getMaxBufferSize()
	// MaxBufferBytes is the maximum total size of buffered messages in bytes
	MaxBufferBytes = getMaxBufferBytes()
)

func getMaxBufferSize() int {
	if v := os.Getenv("BUBU_HUB_BUFFER_MAX_MESSAGES"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return defaultMaxBufferSize
}

func getMaxBufferBytes() int {
	if v := os.Getenv("BUBU_HUB_BUFFER_MAX_BYTES"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return defaultMaxBufferBytes
}

// MessageBuffer holds buffered messages for a downstream engram that's not ready
type MessageBuffer struct {
	mu           sync.Mutex
	messages     []*hubv1.DataPacket
	totalBytes   int
	droppedCount int64
	log          logr.Logger
	storyRunName string
	stepName     string
	lastActive   time.Time
}

// NewMessageBuffer creates a new message buffer
func NewMessageBuffer(storyRunName, stepName string) *MessageBuffer {
	return &MessageBuffer{
		messages:     make([]*hubv1.DataPacket, 0, MaxBufferSize),
		storyRunName: storyRunName,
		stepName:     stepName,
		log:          log.Log.WithName("message-buffer").WithValues("storyRun", storyRunName, "step", stepName),
		lastActive:   time.Now(),
	}
}

// Add attempts to add a message to the buffer. Returns true if added, false if buffer is full.
func (mb *MessageBuffer) Add(msg *hubv1.DataPacket) bool {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	defer func() {
		metrics.RecordHubBufferSize(mb.storyRunName, mb.stepName, len(mb.messages))
		metrics.RecordHubBufferBytes(mb.storyRunName, mb.stepName, mb.totalBytes)
	}()

	// Compute actual protobuf-encoded size for accurate byte accounting
	msgSize := pbproto.Size(msg)

	// If a single message exceeds the max bytes budget, drop it immediately
	if msgSize > MaxBufferBytes {
		mb.droppedCount++
		mb.log.V(1).Info("Message exceeds MaxBufferBytes, dropping",
			"msgSize", msgSize,
			"maxBufferBytes", MaxBufferBytes,
		)
		metrics.RecordHubMessageDropped(mb.storyRunName, mb.stepName, "oversize")
		// Emit aggregated warning every 100 drops
		if mb.droppedCount%100 == 1 { // on start and every 100
			mb.log.Info("Dropping messages due to size or capacity limits",
				"droppedTotal", mb.droppedCount,
				"reason", "oversize",
				"bufferedCount", len(mb.messages),
				"bufferedBytes", mb.totalBytes,
			)
		}
		return false
	}

	// Check capacity limits
	if len(mb.messages) >= MaxBufferSize || mb.totalBytes+msgSize > MaxBufferBytes {
		mb.droppedCount++
		mb.log.V(1).Info("Buffer full, dropping message",
			"bufferedCount", len(mb.messages),
			"bufferedBytes", mb.totalBytes,
			"droppedTotal", mb.droppedCount,
			"msgSize", msgSize)
		metrics.RecordHubMessageDropped(mb.storyRunName, mb.stepName, "buffer_full")
		// Emit aggregated warning every 100 drops
		if mb.droppedCount%100 == 1 { // on start and every 100
			mb.log.Info("Dropping messages due to size or capacity limits",
				"droppedTotal", mb.droppedCount,
				"reason", "buffer_full",
				"bufferedCount", len(mb.messages),
				"bufferedBytes", mb.totalBytes,
			)
		}
		return false
	}

	mb.messages = append(mb.messages, msg)
	mb.totalBytes += msgSize
	mb.log.V(1).Info("Message buffered",
		"bufferedCount", len(mb.messages),
		"bufferedBytes", mb.totalBytes)
	mb.lastActive = time.Now()
	return true
}

// Flush attempts to send all buffered messages to the downstream. Returns number of messages flushed.
func (mb *MessageBuffer) Flush(ctx context.Context, downstream hubv1.HubService_ProcessServer) int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	defer func() {
		metrics.RecordHubBufferSize(mb.storyRunName, mb.stepName, len(mb.messages))
		metrics.RecordHubBufferBytes(mb.storyRunName, mb.stepName, mb.totalBytes)
	}()

	if len(mb.messages) == 0 {
		return 0
	}

	flushed := 0
	remaining := make([]*hubv1.DataPacket, 0, len(mb.messages))

	for _, msg := range mb.messages {
		// Check context before each send
		if ctx.Err() != nil {
			// Context canceled, keep remaining messages buffered
			remaining = append(remaining, msg)
			continue
		}

		if err := downstream.Send(&hubv1.ProcessResponse{Packet: msg}); err != nil {
			mb.log.Error(err, "Failed to flush buffered message, will retry", "flushed", flushed)
			// Keep this message and all remaining in buffer
			remaining = append(remaining, msg)
			break
		}
		flushed++
		mb.totalBytes -= pbproto.Size(msg)
	}

	mb.messages = remaining
	if flushed > 0 {
		metrics.RecordHubBufferFlush(mb.storyRunName, mb.stepName, flushed)
		mb.log.Info("Flushed buffered messages",
			"flushedCount", flushed,
			"remainingCount", len(mb.messages),
			"remainingBytes", mb.totalBytes)
		mb.lastActive = time.Now()
	}

	return flushed
}

// FlushWithSender flushes buffered messages using the provided sender function.
// The sender function should perform the actual send (e.g., stream.Send) and
// handle any necessary serialization outside this method.
func (mb *MessageBuffer) FlushWithSender(ctx context.Context, send func(*hubv1.DataPacket) error) int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	defer func() {
		metrics.RecordHubBufferSize(mb.storyRunName, mb.stepName, len(mb.messages))
		metrics.RecordHubBufferBytes(mb.storyRunName, mb.stepName, mb.totalBytes)
	}()

	if len(mb.messages) == 0 {
		return 0
	}

	flushed := 0
	remaining := make([]*hubv1.DataPacket, 0, len(mb.messages))

	for _, msg := range mb.messages {
		// Check context before each send
		if ctx.Err() != nil {
			// Context canceled, keep remaining messages buffered
			remaining = append(remaining, msg)
			continue
		}

		if err := send(msg); err != nil {
			mb.log.Error(err, "Failed to flush buffered message, will retry", "flushed", flushed)
			// Keep this message and all remaining in buffer
			remaining = append(remaining, msg)
			break
		}
		flushed++
		mb.totalBytes -= pbproto.Size(msg)
	}

	mb.messages = remaining
	if flushed > 0 {
		metrics.RecordHubBufferFlush(mb.storyRunName, mb.stepName, flushed)
		mb.log.Info("Flushed buffered messages",
			"flushedCount", flushed,
			"remainingCount", len(mb.messages),
			"remainingBytes", mb.totalBytes)
		mb.lastActive = time.Now()
	}

	return flushed
}

// Size returns the current number of buffered messages
func (mb *MessageBuffer) Size() int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return len(mb.messages)
}

// DroppedCount returns the total number of dropped messages
func (mb *MessageBuffer) DroppedCount() int64 {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.droppedCount
}

// LastActive returns the last time this buffer had activity (add/flush)
func (mb *MessageBuffer) LastActive() time.Time {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.lastActive
}

// estimateMessageSize returns a rough estimate of message size in bytes
// estimateMessageSize retains the previous rough estimator used in tests; new logic uses
// protobuf Size for accounting, but we keep this for compatibility with existing tests.
func estimateMessageSize(msg *hubv1.DataPacket) int {
	if msg == nil {
		return 0
	}
	size := 0
	for k, v := range msg.Metadata {
		size += len(k) + len(v)
	}
	if msg.Payload != nil {
		size += 1024
	}
	return size
}
