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
	"time"

	grpc_metrics "github.com/bubustack/bobravoz-grpc/pkg/metrics"
	hubv1 "github.com/bubustack/bobravoz-grpc/proto/v1"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Stream represents a single client stream.
type Stream struct {
	grpcStream hubv1.HubService_ProcessServer
	sendChan   chan sendRequest
	done       chan struct{}
	logger     logr.Logger
}

type sendRequest struct {
	resp *hubv1.ProcessResponse
	done chan error
}

func newStream(grpcStream hubv1.HubService_ProcessServer) *Stream {
	s := &Stream{
		grpcStream: grpcStream,
		sendChan:   make(chan sendRequest, 100), // Buffered channel
		done:       make(chan struct{}),
		logger:     log.Log.WithName("hub-stream"),
	}
	go s.sendLoop()
	return s
}

func (s *Stream) sendLoop() {
	defer close(s.done)
	baseCtx := safeStreamContext(s.grpcStream)
	for {
		select {
		case <-baseCtx.Done():
			// Stream canceled/closed; exit send loop
			return
		case req, ok := <-s.sendChan:
			if !ok {
				return
			}
			// Guard Send with a timeout so we don't block indefinitely on dead peers.
			// Prefer any deadline already present on baseCtx; otherwise apply a reasonable cap.
			sendCtx := baseCtx
			var cancel context.CancelFunc
			if _, hasDeadline := baseCtx.Deadline(); !hasDeadline {
				sendCtx, cancel = context.WithTimeout(baseCtx, 30*time.Second)
			}

			// Execute Send in a goroutine to allow timeout/cancel select.
			done := make(chan error, 1)
			go func() { done <- s.grpcStream.Send(req.resp) }()

			var err error
			select {
			case <-sendCtx.Done():
				err = sendCtx.Err()
			case err = <-done:
			}
			if cancel != nil {
				cancel()
			}
			if req.done != nil {
				// Non-blocking in case caller timed out
				select {
				case req.done <- err:
				default:
				}
			}
			if err != nil {
				s.logger.Error(err, "Failed to send packet to stream")
				return
			}
		}
	}
}

// Send sends a packet to the stream.
func (s *Stream) Send(ctx context.Context, req *hubv1.DataPacket) error {
	// Enqueue response for the single send loop; wait for completion or context cancel
	sr := sendRequest{resp: &hubv1.ProcessResponse{Packet: req}, done: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.sendChan <- sr:
		// enqueued
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-sr.done:
		if err == nil {
			// Attempt to attribute the send to storyrun/step if metadata present
			if req != nil && req.Metadata != nil {
				story := req.Metadata["storyrun-name"]
				step := req.Metadata["current-step-id"]
				if story != "" && step != "" {
					grpc_metrics.RecordHubMessageSent(story, step)
				}
			}
		}
		return err
	}
}
