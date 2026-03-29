/*
Copyright 2025 BubuStack.
*/

package hub

import (
	"context"
	"sync"
	"testing"
	"time"
)

const cycleTrackerKey = "ns/storyrun:buffer"

func TestCycleTracker_CancelPrevious(t *testing.T) {
	ct := newCycleTracker()

	ctx1 := t.Context()
	cycle1 := ct.start(cycleTrackerKey, "cancelPrevious", ctx1)

	if cycle1.ctx.Err() != nil {
		t.Fatal("cycle1 should not be cancelled yet")
	}

	ctx2 := t.Context()
	cycle2 := ct.start(cycleTrackerKey, "cancelPrevious", ctx2)

	select {
	case <-cycle1.ctx.Done():
		// expected
	case <-time.After(time.Second):
		t.Fatal("cycle1 should be cancelled")
	}

	if cycle2.ctx.Err() != nil {
		t.Fatal("cycle2 should still be active")
	}
}

func TestCycleTracker_Parallel(t *testing.T) {
	ct := newCycleTracker()

	ctx1 := t.Context()
	cycle1 := ct.start(cycleTrackerKey, "parallel", ctx1)

	ctx2 := t.Context()
	_ = ct.start(cycleTrackerKey, "parallel", ctx2)

	if cycle1.ctx.Err() != nil {
		t.Fatal("cycle1 should not be cancelled in parallel mode")
	}
}

func TestCycleTracker_Complete(t *testing.T) {
	ct := newCycleTracker()

	ctx1 := t.Context()
	cycle1 := ct.start(cycleTrackerKey, "cancelPrevious", ctx1)
	cycle1.complete()

	ctx2 := t.Context()
	cycle2 := ct.start(cycleTrackerKey, "cancelPrevious", ctx2)

	if cycle2.ctx.Err() != nil {
		t.Fatal("cycle2 should be active")
	}
}

func TestCycleTracker_Remove(t *testing.T) {
	ct := newCycleTracker()

	ctx1 := t.Context()
	cycle1 := ct.start(cycleTrackerKey, "cancelPrevious", ctx1)

	ct.remove(cycleTrackerKey, cycle1.id)

	ct.mu.Lock()
	_, exists := ct.active[cycleTrackerKey]
	ct.mu.Unlock()
	if exists {
		t.Fatal("cycle should be removed from active map")
	}
}

func TestCycleTracker_RemoveWrongID(t *testing.T) {
	ct := newCycleTracker()

	ctx1 := t.Context()
	_ = ct.start(cycleTrackerKey, "cancelPrevious", ctx1)

	// Try removing with wrong ID — should not remove.
	ct.remove(cycleTrackerKey, 99999)

	ct.mu.Lock()
	_, exists := ct.active[cycleTrackerKey]
	ct.mu.Unlock()
	if !exists {
		t.Fatal("cycle should still be in active map (wrong ID)")
	}
}

func TestCycleTracker_ParticipantScope(t *testing.T) {
	ct := newCycleTracker()
	keyA := "ns/storyrun:buffer:alice"
	keyB := "ns/storyrun:buffer:bob"

	ctx1 := t.Context()
	cycleA := ct.start(keyA, "cancelPrevious", ctx1)

	ctx2 := t.Context()
	_ = ct.start(keyB, "cancelPrevious", ctx2)

	if cycleA.ctx.Err() != nil {
		t.Fatal("alice's cycle should not be affected by bob's cycle")
	}

	ctx3 := t.Context()
	_ = ct.start(keyA, "cancelPrevious", ctx3)

	select {
	case <-cycleA.ctx.Done():
		// expected
	case <-time.After(time.Second):
		t.Fatal("alice's old cycle should be cancelled")
	}
}

func TestCycleTrackerSerialConcurrent(t *testing.T) {
	ct := newCycleTracker()
	const key = "ns/storyrun:serial-test"
	const numGoroutines = 10

	// Create a parent context with a 5-second timeout to detect deadlock
	parentCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	completedCycles := make(chan int, numGoroutines)

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each goroutine starts a serial cycle
			cycle := ct.start(key, "serial", parentCtx)
			if cycle == nil {
				t.Errorf("goroutine %d got nil cycle", id)
				return
			}

			// Simulate some work
			time.Sleep(time.Millisecond)

			// Complete the cycle
			cycle.complete()
			ct.remove(key, cycle.id)

			completedCycles <- id
		}(i)
	}

	// Wait for all goroutines with a timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed successfully
	case <-parentCtx.Done():
		t.Fatal("test timed out after 5 seconds - possible deadlock in serial mode")
	}

	close(completedCycles)

	// Verify all cycles completed
	completed := 0
	for range completedCycles {
		completed++
	}

	if completed != numGoroutines {
		t.Errorf("expected %d cycles to complete, got %d", numGoroutines, completed)
	}
}

func TestCycleTrackerSerialWaitsForPrevious(t *testing.T) {
	ct := newCycleTracker()
	const key = "ns/storyrun:serial-wait"

	parentCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start first cycle and don't complete it immediately
	cycle1 := ct.start(key, "serial", parentCtx)
	if cycle1 == nil {
		t.Fatal("cycle1 should not be nil")
	}

	// Track when cycle2 starts
	cycle2Started := make(chan struct{})
	var cycle2 *pipelineCycle

	go func() {
		cycle2 = ct.start(key, "serial", parentCtx)
		close(cycle2Started)
	}()

	// Cycle2 should be blocked waiting for cycle1
	select {
	case <-cycle2Started:
		t.Fatal("cycle2 started before cycle1 completed - serial mode not working")
	case <-time.After(50 * time.Millisecond):
		// Expected: cycle2 is blocked
	}

	// Complete cycle1
	cycle1.complete()

	// Now cycle2 should start
	select {
	case <-cycle2Started:
		// Expected
		if cycle2 == nil {
			t.Fatal("cycle2 should not be nil after starting")
		}
		if cycle2.ctx.Err() != nil {
			t.Fatal("cycle2 should be active after starting")
		}
	case <-time.After(time.Second):
		t.Fatal("cycle2 did not start after cycle1 completed")
	}

	cycle2.complete()
}
