/*
Copyright 2025 BubuStack.
*/

package hub

import (
	"context"
	"testing"
	"time"
)

func TestCycleTracker_CancelPrevious(t *testing.T) {
	ct := newCycleTracker()
	key := "ns/storyrun:buffer"

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	cycle1 := ct.start(key, "cancelPrevious", ctx1)

	if cycle1.ctx.Err() != nil {
		t.Fatal("cycle1 should not be cancelled yet")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	cycle2 := ct.start(key, "cancelPrevious", ctx2)

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
	key := "ns/storyrun:buffer"

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	cycle1 := ct.start(key, "parallel", ctx1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	_ = ct.start(key, "parallel", ctx2)

	if cycle1.ctx.Err() != nil {
		t.Fatal("cycle1 should not be cancelled in parallel mode")
	}
}

func TestCycleTracker_Complete(t *testing.T) {
	ct := newCycleTracker()
	key := "ns/storyrun:buffer"

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	cycle1 := ct.start(key, "cancelPrevious", ctx1)
	cycle1.complete()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	cycle2 := ct.start(key, "cancelPrevious", ctx2)

	if cycle2.ctx.Err() != nil {
		t.Fatal("cycle2 should be active")
	}
}

func TestCycleTracker_Remove(t *testing.T) {
	ct := newCycleTracker()
	key := "ns/storyrun:buffer"

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	cycle1 := ct.start(key, "cancelPrevious", ctx1)

	ct.remove(key, cycle1.id)

	ct.mu.Lock()
	_, exists := ct.active[key]
	ct.mu.Unlock()
	if exists {
		t.Fatal("cycle should be removed from active map")
	}
}

func TestCycleTracker_RemoveWrongID(t *testing.T) {
	ct := newCycleTracker()
	key := "ns/storyrun:buffer"

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	_ = ct.start(key, "cancelPrevious", ctx1)

	// Try removing with wrong ID — should not remove.
	ct.remove(key, 99999)

	ct.mu.Lock()
	_, exists := ct.active[key]
	ct.mu.Unlock()
	if !exists {
		t.Fatal("cycle should still be in active map (wrong ID)")
	}
}

func TestCycleTracker_ParticipantScope(t *testing.T) {
	ct := newCycleTracker()
	keyA := "ns/storyrun:buffer:alice"
	keyB := "ns/storyrun:buffer:bob"

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	cycleA := ct.start(keyA, "cancelPrevious", ctx1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	_ = ct.start(keyB, "cancelPrevious", ctx2)

	if cycleA.ctx.Err() != nil {
		t.Fatal("alice's cycle should not be affected by bob's cycle")
	}

	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()
	_ = ct.start(keyA, "cancelPrevious", ctx3)

	select {
	case <-cycleA.ctx.Done():
		// expected
	case <-time.After(time.Second):
		t.Fatal("alice's old cycle should be cancelled")
	}
}
