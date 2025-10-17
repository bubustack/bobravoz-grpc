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

	hubv1 "github.com/bubustack/bobravoz-grpc/proto/v1"
)

// Using mockProcessServer defined in buffer_test.go; extend via wrapper for counters
type countingProcessServer struct {
	*mockProcessServer
	sendCount atomic.Int64
	failOnce  atomic.Bool
}

func (c *countingProcessServer) Send(resp *hubv1.ProcessResponse) error {
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
	sm.AddStream("sr", "step", s)
	msg1 := &hubv1.DataPacket{Metadata: map[string]string{"i": "1"}}
	msg2 := &hubv1.DataPacket{Metadata: map[string]string{"i": "2"}}

	// Buffer a message by temporarily removing stream
	sm.RemoveStream("sr", "step")
	_ = sm.SendOrBuffer(ctx, "sr", "step", msg1) // should buffer
	// Restore stream
	sm.AddStream("sr", "step", s)

	// Next send should flush buffer first, then send new message
	if ok := sm.SendOrBuffer(ctx, "sr", "step", msg2); !ok {
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
	sm.AddStream("sr", "step", s)

	// Force next Send to fail once
	s.failOnce.Store(true)
	msg := &hubv1.DataPacket{Metadata: map[string]string{"i": "x"}}

	// send should buffer after transient error and return true (buffered)
	if ok := sm.SendOrBuffer(ctx, "sr", "step", msg); !ok {
		t.Fatalf("expected buffering on transient error")
	}
}
