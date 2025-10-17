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

	hubv1 "github.com/bubustack/bobravoz-grpc/proto/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestMessageBuffer_Add_Success(t *testing.T) {
	buf := NewMessageBuffer("test-run", "test-step")

	msg := &hubv1.DataPacket{
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
	if err := os.Setenv("BUBU_HUB_BUFFER_MAX_MESSAGES", "2"); err != nil {
		t.Fatalf("failed to set env: %v", err)
	}
	defer func() {
		_ = os.Unsetenv("BUBU_HUB_BUFFER_MAX_MESSAGES")
	}()

	// Reset MaxBufferSize to pick up the env var
	MaxBufferSize = getMaxBufferSize()

	buf := NewMessageBuffer("test-run", "test-step")

	// Add 2 messages (should succeed)
	msg1 := &hubv1.DataPacket{Payload: &structpb.Struct{}}
	msg2 := &hubv1.DataPacket{Payload: &structpb.Struct{}}

	if !buf.Add(msg1) || !buf.Add(msg2) {
		t.Fatalf("First 2 adds should succeed")
	}

	// Add 3rd message (should drop)
	msg3 := &hubv1.DataPacket{Payload: &structpb.Struct{}}
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

func TestMessageBuffer_Flush_Success(t *testing.T) {
	buf := NewMessageBuffer("test-run", "test-step")

	// Add messages to buffer
	for i := 0; i < 3; i++ {
		msg := &hubv1.DataPacket{
			Payload: &structpb.Struct{},
		}
		buf.Add(msg)
	}

	// Mock downstream stream
	mockStream := &mockProcessServer{
		sendFunc: func(packet *hubv1.DataPacket) error {
			return nil // Success
		},
	}

	ctx := context.Background()
	flushed := buf.Flush(ctx, mockStream)

	if flushed != 3 {
		t.Errorf("Expected 3 messages flushed, got %d", flushed)
	}

	if buf.Size() != 0 {
		t.Errorf("Expected buffer empty after flush, got size %d", buf.Size())
	}
}

func TestMessageBuffer_Flush_ContextCanceled(t *testing.T) {
	buf := NewMessageBuffer("test-run", "test-step")

	// Add messages
	for i := 0; i < 5; i++ {
		msg := &hubv1.DataPacket{Payload: &structpb.Struct{}}
		buf.Add(msg)
	}

	// Create canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockStream := &mockProcessServer{
		sendFunc: func(packet *hubv1.DataPacket) error {
			return nil
		},
	}

	flushed := buf.Flush(ctx, mockStream)

	// Should not flush any messages (context canceled immediately)
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
	hubv1.HubService_ProcessServer
	sendFunc func(*hubv1.DataPacket) error
}

func (m *mockProcessServer) Send(resp *hubv1.ProcessResponse) error {
	if m.sendFunc != nil {
		return m.sendFunc(resp.GetPacket())
	}
	return nil
}

func TestEstimateMessageSize(t *testing.T) {
	tests := []struct {
		name    string
		msg     *hubv1.DataPacket
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
			msg: &hubv1.DataPacket{
				Metadata: map[string]string{},
				Payload:  &structpb.Struct{},
			},
			minSize: 1000, // Should have ~1KB for payload
			maxSize: 1500,
		},
		{
			name: "message with metadata",
			msg: &hubv1.DataPacket{
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
