/*
Copyright 2025 BubuStack.
*/

package hub

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

var cycleIDCounter atomic.Int64

type pipelineCycle struct {
	id        int64
	ctx       context.Context
	cancel    context.CancelFunc
	startedAt time.Time
	completed atomic.Bool
}

func (c *pipelineCycle) complete() {
	c.completed.Store(true)
	c.cancel()
}

type cycleTracker struct {
	mu     sync.Mutex
	active map[string]*pipelineCycle
}

func newCycleTracker() *cycleTracker {
	return &cycleTracker{
		active: make(map[string]*pipelineCycle),
	}
}

// start creates a new pipeline cycle for the given key. In cancelPrevious mode,
// any existing active cycle for the same key is cancelled. Returns the new cycle
// with a derived context.
func (ct *cycleTracker) start(key, mode string, parent context.Context) *pipelineCycle {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	cycle := &pipelineCycle{
		id:        cycleIDCounter.Add(1),
		ctx:       ctx,
		cancel:    cancel,
		startedAt: time.Now(),
	}

	switch mode {
	case "cancelPrevious":
		if old, ok := ct.active[key]; ok && !old.completed.Load() {
			old.cancel()
		}
		ct.active[key] = cycle
	case "serial":
		if old, ok := ct.active[key]; ok && !old.completed.Load() {
			// Wait for old cycle to finish (release lock while waiting).
			ct.mu.Unlock()
			<-old.ctx.Done()
			ct.mu.Lock()
		}
		ct.active[key] = cycle
	default:
		// parallel — no tracking needed
	}

	return cycle
}

// remove cleans up a completed cycle from the tracker.
func (ct *cycleTracker) remove(key string, id int64) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	if c, ok := ct.active[key]; ok && c.id == id {
		delete(ct.active, key)
	}
}
