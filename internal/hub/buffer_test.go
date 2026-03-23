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
	"testing"

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

func TestMessageBuffer_FlushWithSender_Success(t *testing.T) {
	buf := NewMessageBuffer("test-run", "test-step")

	// Add messages to buffer
	for i := 0; i < 3; i++ {
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
	for i := 0; i < 5; i++ {
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
