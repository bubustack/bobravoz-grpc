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
	"strings"
	"sync"
	"time"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobravoz-grpc/pkg/metrics"
	"github.com/bubustack/core/contracts"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	pbproto "google.golang.org/protobuf/proto"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// Default buffer limits - can be overridden via env vars
	defaultMaxBufferSize  = 100
	defaultMaxBufferBytes = 10 * 1024 * 1024 // 10 MB
	defaultRetryDelay     = time.Second
)

type bufferDropPolicy string

const (
	bufferDropNewest bufferDropPolicy = "drop_newest"
	bufferDropOldest bufferDropPolicy = "drop_oldest"
)

var (
	// MaxBufferSize is the maximum number of messages to buffer per downstream engram
	MaxBufferSize = getMaxBufferSize()
	// MaxBufferBytes is the maximum total size of buffered messages in bytes
	MaxBufferBytes = getMaxBufferBytes()
)

func getMaxBufferSize() int {
	if v := os.Getenv(contracts.HubBufferMaxMessagesEnv); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return defaultMaxBufferSize
}

func getMaxBufferBytes() int {
	if v := os.Getenv(contracts.HubBufferMaxBytesEnv); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return defaultMaxBufferBytes
}

type bufferLimits struct {
	maxMessages     int
	maxBytes        int
	dropPolicy      bufferDropPolicy
	maxAge          time.Duration
	laneMaxMessages map[string]int
	laneMaxBytes    map[string]int
}

func defaultBufferLimits() bufferLimits {
	return bufferLimits{
		maxMessages: MaxBufferSize,
		maxBytes:    MaxBufferBytes,
		dropPolicy:  bufferDropNewest,
		maxAge:      0,
	}
}

func normalizeBufferLimits(limits bufferLimits) bufferLimits {
	if limits.maxMessages <= 0 {
		limits.maxMessages = MaxBufferSize
	}
	if limits.maxBytes <= 0 {
		limits.maxBytes = MaxBufferBytes
	}
	switch limits.dropPolicy {
	case bufferDropNewest, bufferDropOldest:
	default:
		limits.dropPolicy = bufferDropNewest
	}
	if limits.maxAge < 0 {
		limits.maxAge = 0
	}
	if len(limits.laneMaxMessages) > 0 {
		cleaned := make(map[string]int, len(limits.laneMaxMessages))
		for lane, limit := range limits.laneMaxMessages {
			name := strings.ToLower(strings.TrimSpace(lane))
			if name == "" || limit <= 0 {
				continue
			}
			cleaned[name] = limit
		}
		if len(cleaned) > 0 {
			limits.laneMaxMessages = cleaned
		} else {
			limits.laneMaxMessages = nil
		}
	} else {
		limits.laneMaxMessages = nil
	}
	if len(limits.laneMaxBytes) > 0 {
		cleaned := make(map[string]int, len(limits.laneMaxBytes))
		for lane, limit := range limits.laneMaxBytes {
			name := strings.ToLower(strings.TrimSpace(lane))
			if name == "" || limit <= 0 {
				continue
			}
			cleaned[name] = limit
		}
		if len(cleaned) > 0 {
			limits.laneMaxBytes = cleaned
		} else {
			limits.laneMaxBytes = nil
		}
	} else {
		limits.laneMaxBytes = nil
	}
	return limits
}

func cloneLaneLimits(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// MessageBuffer holds buffered messages for a downstream engram that's not ready
type MessageBuffer struct {
	mu              sync.Mutex
	messages        []*transportpb.DataPacket
	arrivalTimes    []time.Time
	totalBytes      int
	laneCounts      map[string]int
	laneBytes       map[string]int
	droppedCount    int64
	maxMessages     int
	maxBytes        int
	dropPolicy      bufferDropPolicy
	maxAge          time.Duration
	laneMaxMessages map[string]int
	laneMaxBytes    map[string]int
	log             logr.Logger
	storyRunName    string
	stepName        string
	lastActive      time.Time
	retryCount      int
	nextRetry       time.Time
	retryPolicy     *bufferRetryPolicy
	deadline        time.Time
}

type bufferRetryPolicy struct {
	maxRetries int32
	baseDelay  time.Duration
	backoff    enums.BackoffStrategy
	maxDelay   time.Duration
}

// NewMessageBuffer creates a new message buffer
func NewMessageBuffer(storyRunName, stepName string) *MessageBuffer {
	return NewMessageBufferWithLimits(storyRunName, stepName, bufferLimits{})
}

// NewMessageBufferWithLimits creates a new message buffer with explicit limits.
func NewMessageBufferWithLimits(storyRunName, stepName string, limits bufferLimits) *MessageBuffer {
	normalized := normalizeBufferLimits(limits)
	return &MessageBuffer{
		messages:        make([]*transportpb.DataPacket, 0, normalized.maxMessages),
		arrivalTimes:    make([]time.Time, 0, normalized.maxMessages),
		maxMessages:     normalized.maxMessages,
		maxBytes:        normalized.maxBytes,
		dropPolicy:      normalized.dropPolicy,
		maxAge:          normalized.maxAge,
		laneCounts:      make(map[string]int),
		laneBytes:       make(map[string]int),
		laneMaxMessages: cloneLaneLimits(normalized.laneMaxMessages),
		laneMaxBytes:    cloneLaneLimits(normalized.laneMaxBytes),
		storyRunName:    storyRunName,
		stepName:        stepName,
		log:             log.Log.WithName("message-buffer").WithValues("storyRun", storyRunName, "step", stepName),
		lastActive:      time.Now(),
	}
}

// ApplyLimits updates the buffer limits in-place.
func (mb *MessageBuffer) ApplyLimits(limits bufferLimits) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	normalized := normalizeBufferLimits(limits)
	mb.maxMessages = normalized.maxMessages
	mb.maxBytes = normalized.maxBytes
	mb.dropPolicy = normalized.dropPolicy
	mb.maxAge = normalized.maxAge
	mb.laneMaxMessages = cloneLaneLimits(normalized.laneMaxMessages)
	mb.laneMaxBytes = cloneLaneLimits(normalized.laneMaxBytes)
	mb.evictExpiredLocked(time.Now())
	if mb.dropPolicy == bufferDropOldest {
		mb.trimToLimitsLocked("drop_oldest")
		mb.trimLaneLimitsLocked("drop_oldest")
	}
}

// Add attempts to add a message to the buffer. Returns true if added, false if buffer is full.
func (mb *MessageBuffer) Add(msg *transportpb.DataPacket) bool {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	defer func() {
		metrics.RecordHubBufferSize(mb.storyRunName, mb.stepName, len(mb.messages))
		metrics.RecordHubBufferBytes(mb.storyRunName, mb.stepName, mb.totalBytes)
	}()

	normalized := normalizeBufferLimits(bufferLimits{
		maxMessages: mb.maxMessages,
		maxBytes:    mb.maxBytes,
		dropPolicy:  mb.dropPolicy,
		maxAge:      mb.maxAge,
	})
	mb.maxMessages = normalized.maxMessages
	mb.maxBytes = normalized.maxBytes
	mb.dropPolicy = normalized.dropPolicy
	mb.maxAge = normalized.maxAge

	// Compute actual protobuf-encoded size for accurate byte accounting
	msgSize := pbproto.Size(msg)
	lane := laneForPacket(msg)

	mb.evictExpiredLocked(time.Now())

	// If a single message exceeds the max bytes budget, drop it immediately
	if msgSize > mb.maxBytes {
		mb.recordDropLocked("oversize")
		mb.log.V(1).Info("Message exceeds buffer max bytes, dropping",
			"msgSize", msgSize,
			"maxBufferBytes", mb.maxBytes,
		)
		return false
	}

	if limit, ok := mb.laneMaxBytes[lane]; ok && limit > 0 && msgSize > limit {
		mb.recordDropLocked("lane_oversize")
		mb.log.V(1).Info("Message exceeds lane buffer max bytes, dropping",
			"lane", lane,
			"msgSize", msgSize,
			"laneMaxBytes", limit,
		)
		return false
	}

	if mb.laneLimitExceededLocked(lane, 1, msgSize) {
		if mb.dropPolicy == bufferDropOldest {
			dropped := mb.dropOldestLaneToFitLocked(lane, 1, msgSize, "drop_oldest_lane")
			if dropped > 0 {
				mb.log.V(1).Info("Dropped oldest lane messages to make room",
					"lane", lane,
					"droppedCount", dropped,
					"laneCount", mb.laneCounts[lane],
					"laneBytes", mb.laneBytes[lane],
				)
			}
		}
		if mb.laneLimitExceededLocked(lane, 1, msgSize) {
			mb.recordDropLocked("lane_full")
			mb.log.V(1).Info("Lane buffer full, dropping message",
				"lane", lane,
				"laneCount", mb.laneCounts[lane],
				"laneBytes", mb.laneBytes[lane],
				"laneMaxMessages", mb.laneMaxMessages[lane],
				"laneMaxBytes", mb.laneMaxBytes[lane],
				"msgSize", msgSize,
				"dropPolicy", mb.dropPolicy,
			)
			return false
		}
	}

	// Check capacity limits
	if len(mb.messages) >= mb.maxMessages || mb.totalBytes+msgSize > mb.maxBytes {
		if mb.dropPolicy == bufferDropOldest {
			dropped := mb.dropOldestToFitLocked(msgSize)
			if dropped > 0 {
				mb.log.V(1).Info("Dropped oldest buffered messages to make room",
					"droppedCount", dropped,
					"bufferedCount", len(mb.messages),
					"bufferedBytes", mb.totalBytes,
				)
			}
		}
		if len(mb.messages) >= mb.maxMessages || mb.totalBytes+msgSize > mb.maxBytes {
			mb.recordDropLocked("buffer_full")
			mb.log.V(1).Info("Buffer full, dropping message",
				"bufferedCount", len(mb.messages),
				"bufferedBytes", mb.totalBytes,
				"droppedTotal", mb.droppedCount,
				"msgSize", msgSize,
				"dropPolicy", mb.dropPolicy,
			)
			return false
		}
	}

	mb.messages = append(mb.messages, msg)
	mb.totalBytes += msgSize
	mb.arrivalTimes = append(mb.arrivalTimes, time.Now())
	mb.incrementLaneLocked(lane, msgSize)
	mb.log.V(1).Info("Message buffered",
		"bufferedCount", len(mb.messages),
		"bufferedBytes", mb.totalBytes)
	mb.lastActive = time.Now()
	return true
}

func laneForPacket(packet *transportpb.DataPacket) string {
	if packet == nil {
		return ""
	}
	if packet.Audio != nil {
		return "audio"
	}
	if packet.Video != nil {
		return "video"
	}
	if packet.Binary != nil {
		return "binary"
	}
	return "payload"
}

func (mb *MessageBuffer) laneLimitExceededLocked(lane string, extraCount int, extraBytes int) bool {
	if lane == "" {
		return false
	}
	if limit, ok := mb.laneMaxMessages[lane]; ok && limit > 0 {
		if mb.laneCounts[lane]+extraCount > limit {
			return true
		}
	}
	if limit, ok := mb.laneMaxBytes[lane]; ok && limit > 0 {
		if mb.laneBytes[lane]+extraBytes > limit {
			return true
		}
	}
	return false
}

func (mb *MessageBuffer) incrementLaneLocked(lane string, size int) {
	if lane == "" {
		return
	}
	mb.laneCounts[lane]++
	mb.laneBytes[lane] += size
}

func (mb *MessageBuffer) decrementLaneLocked(lane string, size int) {
	if lane == "" {
		return
	}
	if mb.laneCounts[lane] > 0 {
		mb.laneCounts[lane]--
	}
	mb.laneBytes[lane] -= size
	if mb.laneBytes[lane] < 0 {
		mb.laneBytes[lane] = 0
	}
}

func (mb *MessageBuffer) dropOldestLaneToFitLocked(lane string, extraCount int, extraBytes int, reason string) int {
	if lane == "" {
		return 0
	}
	dropped := 0
	for mb.laneLimitExceededLocked(lane, extraCount, extraBytes) {
		index := -1
		for i, msg := range mb.messages {
			if laneForPacket(msg) == lane {
				index = i
				break
			}
		}
		if index < 0 {
			break
		}
		mb.dropMessageAtLocked(index, reason)
		dropped++
	}
	return dropped
}

func (mb *MessageBuffer) dropMessageAtLocked(index int, reason string) {
	if index < 0 || index >= len(mb.messages) {
		return
	}
	msg := mb.messages[index]
	mb.messages = append(mb.messages[:index], mb.messages[index+1:]...)
	if len(mb.arrivalTimes) > index {
		mb.arrivalTimes = append(mb.arrivalTimes[:index], mb.arrivalTimes[index+1:]...)
	}
	msgSize := pbproto.Size(msg)
	mb.totalBytes -= msgSize
	mb.decrementLaneLocked(laneForPacket(msg), msgSize)
	mb.recordDropLocked(reason)
}

func (mb *MessageBuffer) dropOldestToFitLocked(incomingSize int) int {
	dropped := 0
	for len(mb.messages) > 0 && (len(mb.messages) >= mb.maxMessages || mb.totalBytes+incomingSize > mb.maxBytes) {
		mb.dropMessageAtLocked(0, "drop_oldest")
		dropped++
	}
	return dropped
}

func (mb *MessageBuffer) trimToLimitsLocked(reason string) {
	for len(mb.messages) > 0 && (len(mb.messages) > mb.maxMessages || mb.totalBytes > mb.maxBytes) {
		mb.dropMessageAtLocked(0, reason)
	}
}

func (mb *MessageBuffer) trimLaneLimitsLocked(reason string) {
	if len(mb.laneMaxMessages) == 0 && len(mb.laneMaxBytes) == 0 {
		return
	}
	seen := make(map[string]struct{}, len(mb.laneMaxMessages)+len(mb.laneMaxBytes))
	for lane := range mb.laneMaxMessages {
		seen[lane] = struct{}{}
	}
	for lane := range mb.laneMaxBytes {
		seen[lane] = struct{}{}
	}
	for lane := range seen {
		mb.dropOldestLaneToFitLocked(lane, 0, 0, reason)
	}
}

func (mb *MessageBuffer) evictExpiredLocked(now time.Time) {
	if mb.maxAge <= 0 || len(mb.messages) == 0 {
		return
	}
	cutoff := now.Add(-mb.maxAge)
	for len(mb.messages) > 0 && len(mb.arrivalTimes) > 0 {
		if mb.arrivalTimes[0].After(cutoff) {
			break
		}
		mb.dropMessageAtLocked(0, "expired")
	}
}

func (mb *MessageBuffer) recordDropLocked(reason string) {
	mb.droppedCount++
	metrics.RecordHubMessageDropped(mb.storyRunName, mb.stepName, reason)
	// Emit aggregated warning every 100 drops
	if mb.droppedCount%100 == 1 { // on start and every 100
		mb.log.Info("Dropping messages due to size or capacity limits",
			"droppedTotal", mb.droppedCount,
			"reason", reason,
			"bufferedCount", len(mb.messages),
			"bufferedBytes", mb.totalBytes,
		)
	}
}

// FlushWithSender flushes buffered messages using the provided sender function.
// The sender function should perform the actual send (e.g., stream.Send) and
// handle any necessary serialization outside this method.
func (mb *MessageBuffer) FlushWithSender(ctx context.Context, send func(*transportpb.DataPacket) error) (int, error) {
	return mb.FlushWithSenderAndPolicy(ctx, send, nil)
}

// FlushWithSenderAndPolicy flushes buffered messages using the provided sender function
// and an optional allow predicate to honor flow-control decisions.
func (mb *MessageBuffer) FlushWithSenderAndPolicy(ctx context.Context, send func(*transportpb.DataPacket) error, allow func(*transportpb.DataPacket) bool) (int, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	defer func() {
		metrics.RecordHubBufferSize(mb.storyRunName, mb.stepName, len(mb.messages))
		metrics.RecordHubBufferBytes(mb.storyRunName, mb.stepName, mb.totalBytes)
	}()

	mb.evictExpiredLocked(time.Now())

	if len(mb.messages) == 0 {
		return 0, nil
	}

	flushed := 0
	remaining := make([]*transportpb.DataPacket, 0, len(mb.messages))
	remainingTimes := make([]time.Time, 0, len(mb.messages))
	var flushErr error

	for i, msg := range mb.messages {
		// Check context before each send
		if ctx.Err() != nil {
			// Context canceled, keep remaining messages buffered
			remaining = append(remaining, msg)
			if i < len(mb.arrivalTimes) {
				remainingTimes = append(remainingTimes, mb.arrivalTimes[i])
			}
			if flushErr == nil {
				flushErr = ctx.Err()
			}
			continue
		}
		if allow != nil && !allow(msg) {
			remaining = append(remaining, mb.messages[i:]...)
			if i < len(mb.arrivalTimes) {
				remainingTimes = append(remainingTimes, mb.arrivalTimes[i:]...)
			}
			break
		}

		if err := send(msg); err != nil {
			mb.log.Error(err, "Failed to flush buffered message, will retry", "flushed", flushed)
			// Keep this message and all remaining in buffer
			remaining = append(remaining, mb.messages[i:]...)
			if i < len(mb.arrivalTimes) {
				remainingTimes = append(remainingTimes, mb.arrivalTimes[i:]...)
			}
			flushErr = err
			break
		}
		flushed++
		msgSize := pbproto.Size(msg)
		mb.totalBytes -= msgSize
		mb.decrementLaneLocked(laneForPacket(msg), msgSize)
	}

	mb.messages = remaining
	mb.arrivalTimes = remainingTimes
	if flushed > 0 {
		metrics.RecordHubBufferFlush(mb.storyRunName, mb.stepName, flushed)
		mb.log.Info("Flushed buffered messages",
			"flushedCount", flushed,
			"remainingCount", len(mb.messages),
			"remainingBytes", mb.totalBytes)
		mb.lastActive = time.Now()
	}

	return flushed, flushErr
}

// ApplyRetryPolicy sets retry behavior for this buffer. Returns false when retries are disabled.
func (mb *MessageBuffer) ApplyRetryPolicy(policy *bubuv1alpha1.RetryPolicy, maxDelay time.Duration) bool {
	if policy == nil {
		return true
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()

	mb.retryPolicy = normalizeRetryPolicy(policy, maxDelay, mb.log)
	if mb.retryPolicy == nil {
		return true
	}
	return mb.retryPolicy.maxRetries > 0
}

// DropAll removes all buffered messages and records them as dropped.
func (mb *MessageBuffer) DropAll(reason string) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.dropAllLocked(reason)
}

func (mb *MessageBuffer) dropAllLocked(reason string) {
	if len(mb.messages) == 0 {
		return
	}
	dropped := len(mb.messages)
	mb.messages = nil
	mb.arrivalTimes = nil
	mb.totalBytes = 0
	mb.laneCounts = make(map[string]int)
	mb.laneBytes = make(map[string]int)
	mb.deadline = time.Time{}
	mb.droppedCount += int64(dropped)
	for i := 0; i < dropped; i++ {
		metrics.RecordHubMessageDropped(mb.storyRunName, mb.stepName, reason)
	}
	mb.log.Info("Dropping buffered messages", "count", dropped, "reason", reason)
}

// ShouldRetry returns true if the buffer has messages and the retry window has elapsed.
func (mb *MessageBuffer) ShouldRetry(now time.Time) bool {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if len(mb.messages) == 0 {
		return false
	}
	if !mb.deadline.IsZero() && now.After(mb.deadline) {
		mb.dropAllLocked("timeout")
		mb.retryCount = 0
		mb.nextRetry = time.Time{}
		return false
	}
	if mb.retryPolicy != nil && mb.retryPolicy.maxRetries <= 0 {
		return false
	}
	if mb.nextRetry.IsZero() {
		return true
	}
	return !now.Before(mb.nextRetry)
}

// RecordFlushResult updates retry tracking based on the last flush attempt.
func (mb *MessageBuffer) RecordFlushResult(err error, base, max time.Duration) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if err == nil {
		if len(mb.messages) == 0 {
			mb.retryCount = 0
			mb.nextRetry = time.Time{}
			mb.deadline = time.Time{}
		}
		return
	}
	mb.retryCount++
	if mb.retryPolicy != nil {
		if mb.retryPolicy.maxRetries > 0 && int32(mb.retryCount) >= mb.retryPolicy.maxRetries {
			mb.dropAllLocked("retry_exhausted")
			mb.retryCount = 0
			mb.nextRetry = time.Time{}
			return
		}
		delay := computeRetryDelay(mb.retryPolicy, mb.retryCount, max)
		mb.nextRetry = time.Now().Add(delay)
		return
	}
	delay := backoffDuration(base, max, mb.retryCount)
	mb.nextRetry = time.Now().Add(delay)
}

func backoffDuration(base, max time.Duration, attempt int) time.Duration {
	if attempt <= 0 {
		return base
	}
	backoff := base
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if max > 0 && backoff >= max {
			return max
		}
	}
	if max > 0 && backoff > max {
		return max
	}
	return backoff
}

func computeRetryDelay(policy *bufferRetryPolicy, attempt int, fallbackMax time.Duration) time.Duration {
	if policy == nil {
		return backoffDuration(defaultRetryDelay, fallbackMax, attempt)
	}
	baseDelay := policy.baseDelay
	if baseDelay <= 0 {
		baseDelay = defaultRetryDelay
	}
	delay := baseDelay
	switch policy.backoff {
	case enums.BackoffStrategyLinear:
		delay = baseDelay * time.Duration(attempt)
	case enums.BackoffStrategyConstant:
		delay = baseDelay
	default:
		multiplier := 1 << max(attempt-1, 0)
		delay = baseDelay * time.Duration(multiplier)
	}
	maxDelay := policy.maxDelay
	if maxDelay <= 0 {
		maxDelay = fallbackMax
	}
	if maxDelay > 0 && delay > maxDelay {
		return maxDelay
	}
	return delay
}

func normalizeRetryPolicy(policy *bubuv1alpha1.RetryPolicy, maxDelay time.Duration, logger logr.Logger) *bufferRetryPolicy {
	if policy == nil {
		return nil
	}
	out := &bufferRetryPolicy{
		maxDelay: maxDelay,
		backoff:  enums.BackoffStrategyExponential,
	}
	if policy.MaxRetries != nil {
		out.maxRetries = *policy.MaxRetries
	} else {
		out.maxRetries = 3
	}
	delay := defaultRetryDelay
	if policy.Delay != nil && strings.TrimSpace(*policy.Delay) != "" {
		if parsed, err := time.ParseDuration(strings.TrimSpace(*policy.Delay)); err == nil && parsed > 0 {
			delay = parsed
		} else {
			logger.Info("Invalid retry delay; using default", "value", *policy.Delay, "default", defaultRetryDelay)
		}
	}
	out.baseDelay = delay
	if policy.Backoff != nil {
		out.backoff = *policy.Backoff
	}
	return out
}

// ApplyDeadline ensures the buffer has an expiration deadline; the earliest deadline wins.
func (mb *MessageBuffer) ApplyDeadline(deadline time.Time) {
	if deadline.IsZero() {
		return
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.deadline.IsZero() || deadline.Before(mb.deadline) {
		mb.deadline = deadline
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Size returns the current number of buffered messages
func (mb *MessageBuffer) Size() int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return len(mb.messages)
}

// Utilization returns the max of message-count and byte-capacity utilization (0..1).
func (mb *MessageBuffer) Utilization() float64 {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.maxMessages <= 0 && mb.maxBytes <= 0 {
		return 0
	}
	msgPct := 0.0
	if mb.maxMessages > 0 {
		msgPct = float64(len(mb.messages)) / float64(mb.maxMessages)
	}
	bytePct := 0.0
	if mb.maxBytes > 0 {
		bytePct = float64(mb.totalBytes) / float64(mb.maxBytes)
	}
	if bytePct > msgPct {
		return bytePct
	}
	return msgPct
}

// DroppedCount returns the total number of dropped messages
func (mb *MessageBuffer) DroppedCount() int64 {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.droppedCount
}

// LastActive returns the duration since the last buffer activity (add/flush)
// using monotonic time to avoid issues with system clock adjustments
func (mb *MessageBuffer) LastActive() time.Duration {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.lastActive.IsZero() {
		return 0
	}
	return time.Since(mb.lastActive)
}

// estimateMessageSize returns a rough estimate of message size in bytes
// estimateMessageSize retains the previous rough estimator used in tests; new logic uses
// protobuf Size for accounting, but we keep this for compatibility with existing tests.
func estimateMessageSize(msg *transportpb.DataPacket) int {
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
