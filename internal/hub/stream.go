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
	"io"
	"sync"

	grpc_metrics "github.com/bubustack/bobravoz-grpc/pkg/metrics"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Stream represents a single client stream.
type Stream struct {
	grpcStream  transportpb.HubService_ProcessServer
	sendChan    chan sendRequest
	done        chan struct{}
	logger      logr.Logger
	closeOnce   sync.Once
	fallbackCtx context.Context
}

type sendRequest struct {
	resp *transportpb.ProcessResponse
	done chan error
}

func newStream(fallback context.Context, grpcStream transportpb.HubService_ProcessServer, bufferSize int) *Stream {
	s := &Stream{
		grpcStream:  grpcStream,
		sendChan:    make(chan sendRequest, bufferSize), // Buffered channel
		done:        make(chan struct{}),
		logger:      log.Log.WithName("hub-stream"),
		fallbackCtx: fallback,
	}
	go s.sendLoop()
	return s
}

func (s *Stream) sendLoop() {
	defer close(s.done)
	baseCtx := safeStreamContext(s.grpcStream, s.fallbackCtx)
	for {
		select {
		case <-baseCtx.Done():
			// Stream canceled/closed; exit send loop
			return
		case req, ok := <-s.sendChan:
			if !ok {
				return
			}

			err := s.grpcStream.Send(req.resp)

			if req.done != nil {
				// Non-blocking in case caller timed out
				select {
				case req.done <- err:
				default:
				}
			}
			if err != nil {
				s.logger.Error(err, "failed to send packet to stream")
				// Keep the send loop alive so transient send errors don't stall future sends.
				continue
			}
		}
	}
}

// Send sends a packet to the stream.
func (s *Stream) Send(ctx context.Context, req *transportpb.DataPacket) error {
	if ctx == nil {
		s.logger.Info("Send called with nil context; cancellation signals will not propagate")
		ctx = context.Background()
	}
	// Enqueue response for the single send loop; wait for completion or context cancel
	sr := sendRequest{resp: &transportpb.ProcessResponse{Packet: req}, done: make(chan error, 1)}
	select {
	case <-s.done:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	case s.sendChan <- sr:
		// enqueued
	}
	select {
	case <-s.done:
		return io.ErrClosedPipe
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

// SendFlow sends a flow-control update to the stream.
func (s *Stream) SendFlow(ctx context.Context, flow *transportpb.FlowControl) error {
	if flow == nil {
		return nil
	}
	if ctx == nil {
		s.logger.Info("SendFlow called with nil context; cancellation signals will not propagate")
		ctx = context.Background()
	}
	sr := sendRequest{resp: &transportpb.ProcessResponse{Flow: flow}, done: make(chan error, 1)}
	select {
	case <-s.done:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	case s.sendChan <- sr:
	}
	select {
	case <-s.done:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	case err := <-sr.done:
		return err
	}
}

// Close signals the send loop to exit and waits for it to drain.
func (s *Stream) Close() {
	s.closeOnce.Do(func() {
		close(s.sendChan)
		<-s.done
	})
}
