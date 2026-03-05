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
	"sync/atomic"
	"testing"

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
	sm := NewStreamManager()
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
	sm := NewStreamManager()
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

func TestStreamManagerSeparatesNamespaces(t *testing.T) {
	sm := NewStreamManager()
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
