package hub

import (
	"fmt"
	"sync"

	"github.com/bubustack/bobravoz-grpc/proto"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// StreamManager manages the active gRPC streams.
type StreamManager struct {
	mu      sync.RWMutex
	streams map[string]proto.Hub_ProcessServer
	log     logr.Logger
}

// NewStreamManager creates a new StreamManager.
func NewStreamManager() *StreamManager {
	return &StreamManager{
		streams: make(map[string]proto.Hub_ProcessServer),
		log:     log.Log.WithName("stream-manager"),
	}
}

// AddStream registers a new stream.
func (sm *StreamManager) AddStream(storyRunName, stepName string, stream proto.Hub_ProcessServer) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	key := sm.streamKey(storyRunName, stepName)
	sm.streams[key] = stream
	sm.log.Info("Stream added", "key", key)
}

// RemoveStream removes a stream.
func (sm *StreamManager) RemoveStream(storyRunName, stepName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	key := sm.streamKey(storyRunName, stepName)
	delete(sm.streams, key)
	sm.log.Info("Stream removed", "key", key)
}

// GetStream retrieves a stream.
func (sm *StreamManager) GetStream(storyRunName, stepName string) (proto.Hub_ProcessServer, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	key := sm.streamKey(storyRunName, stepName)
	stream, ok := sm.streams[key]
	return stream, ok
}

func (sm *StreamManager) streamKey(storyRunName, stepName string) string {
	return fmt.Sprintf("%s/%s", storyRunName, stepName)
}
