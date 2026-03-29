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
	"testing"
	"time"

	"github.com/bubustack/core/contracts"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestMessageBuffer_Add_Success(t *testing.T) {
	buf := NewMessageBuffer("test-run", "test-step")

	msg := &transportpb.DataPacket{
		Metadata: map[string]string{"key": "value"},
		Payload:  &structpb.Struct{},
	}

	if !buf.Add(msg) {
		t.Errorf("Add() should succeed when buffer has capacity")
	}

	if buf.Size() != 1 {
		t.Errorf("Expected size 1, got %d", buf.Size())
	}
}

func TestMessageBuffer_Add_Overflow(t *testing.T) {
	// Save original values
	origMaxBufferSize := MaxBufferSize
	origMaxBufferBytes := MaxBufferBytes
	defer func() {
		MaxBufferSize = origMaxBufferSize
		MaxBufferBytes = origMaxBufferBytes
	}()

	// Set small buffer size for testing
	if err := os.Setenv(contracts.HubBufferMaxMessagesEnv, "2"); err != nil {
		t.Fatalf("failed to set env: %v", err)
	}
	defer func() {
		_ = os.Unsetenv(contracts.HubBufferMaxMessagesEnv)
	}()

	// Reset MaxBufferSize to pick up the env var
	MaxBufferSize = getMaxBufferSize()

	buf := NewMessageBuffer("test-run", "test-step")

	// Add 2 messages (should succeed)
	msg1 := &transportpb.DataPacket{Payload: &structpb.Struct{}}
	msg2 := &transportpb.DataPacket{Payload: &structpb.Struct{}}

	if !buf.Add(msg1) || !buf.Add(msg2) {
		t.Fatalf("First 2 adds should succeed")
	}

	// Add 3rd message (should drop)
	msg3 := &transportpb.DataPacket{Payload: &structpb.Struct{}}
	if buf.Add(msg3) {
		t.Errorf("Add() should fail when buffer full")
	}

	if buf.Size() != 2 {
		t.Errorf("Expected size 2, got %d", buf.Size())
	}

	if buf.DroppedCount() != 1 {
		t.Errorf("Expected 1 dropped message, got %d", buf.DroppedCount())
	}
}

func TestMessageBuffer_Add_DropOldest(t *testing.T) {
	buf := NewMessageBufferWithLimits("test-run", "test-step", bufferLimits{
		maxMessages: 2,
		maxBytes:    MaxBufferBytes,
		dropPolicy:  bufferDropOldest,
	})

	msg1 := &transportpb.DataPacket{Metadata: map[string]string{"id": "1"}, Payload: &structpb.Struct{}}
	msg2 := &transportpb.DataPacket{Metadata: map[string]string{"id": "2"}, Payload: &structpb.Struct{}}
	msg3 := &transportpb.DataPacket{Metadata: map[string]string{"id": "3"}, Payload: &structpb.Struct{}}

	if !buf.Add(msg1) || !buf.Add(msg2) {
		t.Fatalf("expected initial adds to succeed")
	}
	if !buf.Add(msg3) {
		t.Fatalf("expected drop_oldest to accept newest message")
	}

	var got []string
	_, err := buf.FlushWithSender(context.Background(), func(p *transportpb.DataPacket) error {
		got = append(got, p.Metadata["id"])
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected flush error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 messages flushed, got %d", len(got))
	}
	if got[0] != "2" || got[1] != "3" {
		t.Fatalf("expected messages [2 3], got %v", got)
	}
	if buf.DroppedCount() != 1 {
		t.Errorf("expected 1 dropped message, got %d", buf.DroppedCount())
	}
}

func TestMessageBuffer_ApplyLimitsTrimsExistingMessagesForDropNewest(t *testing.T) {
	buf := NewMessageBufferWithLimits("test-run", "test-step", bufferLimits{
		maxMessages: 3,
		maxBytes:    MaxBufferBytes,
		dropPolicy:  bufferDropNewest,
	})

	msg1 := &transportpb.DataPacket{Metadata: map[string]string{"id": "1"}, Payload: &structpb.Struct{}}
	msg2 := &transportpb.DataPacket{Metadata: map[string]string{"id": "2"}, Payload: &structpb.Struct{}}
	msg3 := &transportpb.DataPacket{Metadata: map[string]string{"id": "3"}, Payload: &structpb.Struct{}}

	if !buf.Add(msg1) || !buf.Add(msg2) || !buf.Add(msg3) {
		t.Fatal("expected initial adds to succeed")
	}

	buf.ApplyLimits(bufferLimits{
		maxMessages: 2,
		maxBytes:    MaxBufferBytes,
		dropPolicy:  bufferDropNewest,
	})

	if got := buf.Size(); got != 2 {
		t.Fatalf("expected buffer size 2 after shrinking limits, got %d", got)
	}

	msg4 := &transportpb.DataPacket{Metadata: map[string]string{"id": "4"}, Payload: &structpb.Struct{}}
	if buf.Add(msg4) {
		t.Fatal("expected add to fail once shrunken buffer is back at capacity")
	}

	var got []string
	_, err := buf.FlushWithSender(context.Background(), func(p *transportpb.DataPacket) error {
		got = append(got, p.Metadata["id"])
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected flush error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 messages flushed, got %d", len(got))
	}
	if got[0] != "2" || got[1] != "3" {
		t.Fatalf("expected messages [2 3] to remain after trim, got %v", got)
	}
}

func TestMessageBuffer_FlushWithSender_Success(t *testing.T) {
	buf := NewMessageBuffer("test-run", "test-step")

	// Add messages to buffer
	for range 3 {
		msg := &transportpb.DataPacket{
			Payload: &structpb.Struct{},
		}
		buf.Add(msg)
	}

	ctx := context.Background()
	flushed, err := buf.FlushWithSender(ctx, func(pkt *transportpb.DataPacket) error {
		return nil
	})

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if flushed != 3 {
		t.Errorf("Expected 3 messages flushed, got %d", flushed)
	}
	if buf.Size() != 0 {
		t.Errorf("Expected buffer empty after flush, got size %d", buf.Size())
	}
}

func TestMessageBuffer_FlushWithSender_ContextCanceled(t *testing.T) {
	buf := NewMessageBuffer("test-run", "test-step")

	// Add messages
	for range 5 {
		msg := &transportpb.DataPacket{Payload: &structpb.Struct{}}
		buf.Add(msg)
	}

	// Create canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	flushed, err := buf.FlushWithSender(ctx, func(pkt *transportpb.DataPacket) error {
		return nil
	})

	// Should not flush any messages (context canceled immediately)
	if err == nil || err != context.Canceled {
		t.Fatalf("Expected context.Canceled error, got %v", err)
	}
	if flushed != 0 {
		t.Errorf("Expected 0 messages flushed (context canceled), got %d", flushed)
	}

	// Buffer should still have all messages
	if buf.Size() != 5 {
		t.Errorf("Expected buffer size 5 (no flush), got %d", buf.Size())
	}
}

// mockProcessServer implements proto.Hub_ProcessServer for testing
type mockProcessServer struct {
	transportpb.HubService_ProcessServer
	sendFunc func(*transportpb.DataPacket) error
}

func (m *mockProcessServer) Send(resp *transportpb.ProcessResponse) error {
	if m.sendFunc != nil {
		return m.sendFunc(resp.GetPacket())
	}
	return nil
}

func (m *mockProcessServer) Context() context.Context {
	return context.Background()
}

func TestBufferConcurrentEnqueueAndFlush(t *testing.T) {
	buf := NewMessageBufferWithLimits("test-run", "test-step", bufferLimits{
		maxMessages: 1000,
		maxBytes:    100 * 1024 * 1024, // 100 MB to avoid byte limit
		dropPolicy:  bufferDropNewest,
	})

	const numEnqueuers = 10
	const messagesPerEnqueuer = 100

	var wg sync.WaitGroup

	// Start 10 goroutines each enqueuing messages
	for i := range numEnqueuers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range messagesPerEnqueuer {
				msg := &transportpb.DataPacket{
					Metadata: map[string]string{
						"enqueuer": strconv.Itoa(id),
						"msg":      strconv.Itoa(j),
					},
					Payload: &structpb.Struct{},
				}
				buf.Add(msg)
			}
		}(i)
	}

	// Start 1 goroutine flushing periodically
	flushDone := make(chan struct{})
	var totalFlushed int64
	go func() {
		defer close(flushDone)
		ctx := context.Background()
		for {
			select {
			case <-time.After(time.Millisecond):
				flushed, _ := buf.FlushWithSender(ctx, func(pkt *transportpb.DataPacket) error {
					return nil
				})
				totalFlushed += int64(flushed)
			default:
				if buf.Size() == 0 {
					// Check if all enqueuers are done
					time.Sleep(10 * time.Millisecond)
					// Final flush
					flushed, _ := buf.FlushWithSender(ctx, func(pkt *transportpb.DataPacket) error {
						return nil
					})
					totalFlushed += int64(flushed)
					if buf.Size() == 0 {
						return
					}
				}
			}
		}
	}()

	wg.Wait()

	// Wait for flush goroutine to finish
	select {
	case <-flushDone:
	case <-time.After(5 * time.Second):
		t.Fatal("flush goroutine did not complete in time")
	}

	// Final cleanup flush
	ctx := context.Background()
	flushed, _ := buf.FlushWithSender(ctx, func(pkt *transportpb.DataPacket) error {
		return nil
	})
	totalFlushed += int64(flushed)

	// Verify no messages were lost (flushed + remaining + dropped = total enqueued)
	remaining := buf.Size()
	dropped := buf.DroppedCount()

	if remaining != 0 {
		t.Errorf("expected buffer to be empty after final flush, got %d remaining", remaining)
	}

	totalEnqueued := int64(numEnqueuers * messagesPerEnqueuer)
	totalAccounted := totalFlushed + int64(remaining) + dropped
	if totalAccounted != totalEnqueued {
		t.Errorf("message accounting mismatch: flushed=%d + remaining=%d + dropped=%d = %d, expected %d",
			totalFlushed, remaining, dropped, totalAccounted, totalEnqueued)
	}
}

func TestEstimateMessageSize(t *testing.T) {
	tests := []struct {
		name    string
		msg     *transportpb.DataPacket
		minSize int
		maxSize int
	}{
		{
			name:    "nil message",
			msg:     nil,
			minSize: 0,
			maxSize: 0,
		},
		{
			name: "empty message",
			msg: &transportpb.DataPacket{
				Metadata: map[string]string{},
				Payload:  &structpb.Struct{},
			},
			minSize: 1000, // Should have ~1KB for payload
			maxSize: 1500,
		},
		{
			name: "message with metadata",
			msg: &transportpb.DataPacket{
				Metadata: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
				Payload: &structpb.Struct{},
			},
			minSize: 1020, // Metadata + payload
			maxSize: 1600,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := estimateMessageSize(tt.msg)
			if size < tt.minSize || size > tt.maxSize {
				t.Errorf("estimateMessageSize() = %d, want between %d and %d", size, tt.minSize, tt.maxSize)
			}
		})
	}
}

func TestBufferLaneLimits(t *testing.T) { //nolint:gocyclo // table-driven subtests for lane limit scenarios
	// Test per-lane message limits with drop_newest policy (default).
	t.Run("lane_max_messages_drop_newest", func(t *testing.T) {
		buf := NewMessageBufferWithLimits("test-run", "test-step", bufferLimits{
			maxMessages: 100, // High global limit
			maxBytes:    100 * 1024 * 1024,
			dropPolicy:  bufferDropNewest,
			laneMaxMessages: map[string]int{
				"audio": 2,
			},
		})

		audio1 := &transportpb.DataPacket{
			Metadata: map[string]string{"id": "audio1"},
			Frame:    &transportpb.DataPacket_Audio{Audio: &transportpb.AudioFrame{Codec: "pcm16"}},
		}
		audio2 := &transportpb.DataPacket{
			Metadata: map[string]string{"id": "audio2"},
			Frame:    &transportpb.DataPacket_Audio{Audio: &transportpb.AudioFrame{Codec: "pcm16"}},
		}
		audio3 := &transportpb.DataPacket{
			Metadata: map[string]string{"id": "audio3"},
			Frame:    &transportpb.DataPacket_Audio{Audio: &transportpb.AudioFrame{Codec: "pcm16"}},
		}

		if !buf.Add(audio1) {
			t.Fatal("expected audio1 to be added")
		}
		if !buf.Add(audio2) {
			t.Fatal("expected audio2 to be added")
		}
		// Third audio message should be rejected due to lane limit.
		if buf.Add(audio3) {
			t.Fatal("expected audio3 to be rejected due to lane limit")
		}

		if buf.DroppedCount() != 1 {
			t.Fatalf("expected 1 dropped message, got %d", buf.DroppedCount())
		}
		if buf.Size() != 2 {
			t.Fatalf("expected buffer size 2, got %d", buf.Size())
		}
	})

	// Test per-lane message limits with drop_oldest policy.
	t.Run("lane_max_messages_drop_oldest", func(t *testing.T) {
		buf := NewMessageBufferWithLimits("test-run", "test-step", bufferLimits{
			maxMessages: 100,
			maxBytes:    100 * 1024 * 1024,
			dropPolicy:  bufferDropOldest,
			laneMaxMessages: map[string]int{
				"video": 2,
			},
		})

		video1 := &transportpb.DataPacket{
			Metadata: map[string]string{"id": "video1"},
			Frame:    &transportpb.DataPacket_Video{Video: &transportpb.VideoFrame{Codec: "h264"}},
		}
		video2 := &transportpb.DataPacket{
			Metadata: map[string]string{"id": "video2"},
			Frame:    &transportpb.DataPacket_Video{Video: &transportpb.VideoFrame{Codec: "h264"}},
		}
		video3 := &transportpb.DataPacket{
			Metadata: map[string]string{"id": "video3"},
			Frame:    &transportpb.DataPacket_Video{Video: &transportpb.VideoFrame{Codec: "h264"}},
		}

		if !buf.Add(video1) {
			t.Fatal("expected video1 to be added")
		}
		if !buf.Add(video2) {
			t.Fatal("expected video2 to be added")
		}
		// Third video message should trigger drop_oldest and be accepted.
		if !buf.Add(video3) {
			t.Fatal("expected video3 to be accepted (should drop oldest)")
		}

		if buf.DroppedCount() != 1 {
			t.Fatalf("expected 1 dropped message, got %d", buf.DroppedCount())
		}
		if buf.Size() != 2 {
			t.Fatalf("expected buffer size 2, got %d", buf.Size())
		}

		// Verify video1 was dropped and video2, video3 remain.
		var got []string
		_, err := buf.FlushWithSender(context.Background(), func(p *transportpb.DataPacket) error {
			got = append(got, p.Metadata["id"])
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected flush error: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("expected 2 messages flushed, got %d", len(got))
		}
		if got[0] != "video2" || got[1] != "video3" {
			t.Fatalf("expected messages [video2 video3], got %v", got)
		}
	})

	// Test that lanes do not interfere with each other.
	t.Run("independent_lanes", func(t *testing.T) {
		buf := NewMessageBufferWithLimits("test-run", "test-step", bufferLimits{
			maxMessages: 100,
			maxBytes:    100 * 1024 * 1024,
			dropPolicy:  bufferDropNewest,
			laneMaxMessages: map[string]int{
				"audio": 1,
				"video": 1,
			},
		})

		audio := &transportpb.DataPacket{
			Metadata: map[string]string{"id": "audio"},
			Frame:    &transportpb.DataPacket_Audio{Audio: &transportpb.AudioFrame{Codec: "pcm16"}},
		}
		video := &transportpb.DataPacket{
			Metadata: map[string]string{"id": "video"},
			Frame:    &transportpb.DataPacket_Video{Video: &transportpb.VideoFrame{Codec: "h264"}},
		}

		if !buf.Add(audio) {
			t.Fatal("expected audio to be added")
		}
		if !buf.Add(video) {
			t.Fatal("expected video to be added")
		}

		// Both lanes should be at limit, second audio should be rejected.
		audio2 := &transportpb.DataPacket{
			Metadata: map[string]string{"id": "audio2"},
			Frame:    &transportpb.DataPacket_Audio{Audio: &transportpb.AudioFrame{Codec: "pcm16"}},
		}
		if buf.Add(audio2) {
			t.Fatal("expected audio2 to be rejected due to audio lane limit")
		}

		// Second video should also be rejected.
		video2 := &transportpb.DataPacket{
			Metadata: map[string]string{"id": "video2"},
			Frame:    &transportpb.DataPacket_Video{Video: &transportpb.VideoFrame{Codec: "h264"}},
		}
		if buf.Add(video2) {
			t.Fatal("expected video2 to be rejected due to video lane limit")
		}

		if buf.DroppedCount() != 2 {
			t.Fatalf("expected 2 dropped messages, got %d", buf.DroppedCount())
		}
		if buf.Size() != 2 {
			t.Fatalf("expected buffer size 2, got %d", buf.Size())
		}
	})

	// Test ApplyLimits enforces lane limits on existing messages.
	t.Run("apply_limits_trims_lanes", func(t *testing.T) {
		buf := NewMessageBufferWithLimits("test-run", "test-step", bufferLimits{
			maxMessages: 100,
			maxBytes:    100 * 1024 * 1024,
			dropPolicy:  bufferDropNewest,
		})

		// Add 3 audio messages without lane limits.
		for i := range 3 {
			msg := &transportpb.DataPacket{
				Metadata: map[string]string{"id": strconv.Itoa(i)},
				Frame:    &transportpb.DataPacket_Audio{Audio: &transportpb.AudioFrame{Codec: "pcm16"}},
			}
			if !buf.Add(msg) {
				t.Fatalf("expected message %d to be added", i)
			}
		}

		if buf.Size() != 3 {
			t.Fatalf("expected buffer size 3, got %d", buf.Size())
		}

		// Apply new limits with audio lane limit of 1.
		buf.ApplyLimits(bufferLimits{
			maxMessages: 100,
			maxBytes:    100 * 1024 * 1024,
			dropPolicy:  bufferDropNewest,
			laneMaxMessages: map[string]int{
				"audio": 1,
			},
		})

		// Should trim to 1 audio message.
		if buf.Size() != 1 {
			t.Fatalf("expected buffer size 1 after applying lane limit, got %d", buf.Size())
		}
		if buf.DroppedCount() != 2 {
			t.Fatalf("expected 2 dropped messages after trim, got %d", buf.DroppedCount())
		}
	})
}
