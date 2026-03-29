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
	"container/list"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

func TestJoinCache_FanInAllMode(t *testing.T) {
	cache := newJoinCache(logr.Discard())
	key := joinKey{
		storyRunName: "run-1",
		storyRunNS:   "default",
		stepID:       "step-join",
		joinID:       "join-1",
	}
	required := []string{"step-a", "step-b", "step-c"}

	// First input arrives.
	outputs, ready := cache.record(key, "step-a", map[string]any{"data": "a"}, required, fanInPolicy{mode: "all"})
	if ready {
		t.Fatal("expected join not ready after first input")
	}
	if outputs != nil {
		t.Fatal("expected nil outputs when not ready")
	}

	// Second input arrives.
	_, ready = cache.record(key, "step-b", map[string]any{"data": "b"}, required, fanInPolicy{mode: "all"})
	if ready {
		t.Fatal("expected join not ready after second input")
	}

	// Third input arrives, should complete.
	outputs, ready = cache.record(key, "step-c", map[string]any{"data": "c"}, required, fanInPolicy{mode: "all"})
	if !ready {
		t.Fatal("expected join to be ready after all inputs")
	}
	if outputs == nil {
		t.Fatal("expected non-nil outputs when ready")
	}

	// Verify all steps are present in the output.
	for _, stepID := range required {
		stepData, ok := outputs[stepID]
		if !ok {
			t.Fatalf("expected step %s in outputs", stepID)
		}
		stepMap, ok := stepData.(map[string]any)
		if !ok {
			t.Fatalf("expected step %s data to be map", stepID)
		}
		if _, ok := stepMap["outputs"]; !ok {
			t.Fatalf("expected step %s to have outputs key", stepID)
		}
	}

	// Entry should be removed after completion.
	cache.mu.Lock()
	_, exists := cache.entries[key]
	cache.mu.Unlock()
	if exists {
		t.Fatal("expected entry to be removed after join completion")
	}
}

func TestJoinCache_FanInAnyMode(t *testing.T) {
	cache := newJoinCache(logr.Discard())
	key := joinKey{
		storyRunName: "run-2",
		storyRunNS:   "default",
		stepID:       "step-join-any",
		joinID:       "join-2",
	}
	required := []string{"step-a", "step-b", "step-c"}

	// First input should trigger completion in "any" mode.
	outputs, ready := cache.record(key, "step-a", map[string]any{"data": "a"}, required, fanInPolicy{mode: "any"})
	if !ready {
		t.Fatal("expected join to be ready after first input in any mode")
	}
	if outputs == nil {
		t.Fatal("expected non-nil outputs")
	}

	// Only step-a should be present.
	if _, ok := outputs["step-a"]; !ok {
		t.Fatal("expected step-a in outputs")
	}

	// Entry should be removed.
	cache.mu.Lock()
	_, exists := cache.entries[key]
	cache.mu.Unlock()
	if exists {
		t.Fatal("expected entry to be removed after join completion")
	}
}

func TestJoinCache_FanInQuorumMode(t *testing.T) {
	cache := newJoinCache(logr.Discard())
	key := joinKey{
		storyRunName: "run-3",
		storyRunNS:   "default",
		stepID:       "step-join-quorum",
		joinID:       "join-3",
	}
	required := []string{"step-a", "step-b", "step-c", "step-d"}

	// First input, quorum is 2.
	_, ready := cache.record(key, "step-a", map[string]any{"data": "a"}, required, fanInPolicy{mode: "quorum", quorum: 2})
	if ready {
		t.Fatal("expected join not ready after first input (quorum=2)")
	}

	// Second input, quorum should be reached.
	outputs, ready := cache.record(key, "step-b", map[string]any{"data": "b"}, required, fanInPolicy{mode: "quorum", quorum: 2})
	if !ready {
		t.Fatal("expected join to be ready after quorum (2) inputs")
	}
	if outputs == nil {
		t.Fatal("expected non-nil outputs")
	}

	// Should have step-a and step-b.
	if _, ok := outputs["step-a"]; !ok {
		t.Fatal("expected step-a in outputs")
	}
	if _, ok := outputs["step-b"]; !ok {
		t.Fatal("expected step-b in outputs")
	}
}

func TestJoinCache_MultipleInputsFromSameStep(t *testing.T) {
	cache := newJoinCache(logr.Discard())
	key := joinKey{
		storyRunName: "run-4",
		storyRunNS:   "default",
		stepID:       "step-join-dup",
		joinID:       "join-4",
	}
	required := []string{"step-a", "step-b"}

	// First input from step-a.
	_, ready := cache.record(key, "step-a", map[string]any{"data": "a1"}, required, fanInPolicy{mode: "all"})
	if ready {
		t.Fatal("expected join not ready")
	}

	// Second input from step-a (should update, not duplicate).
	_, ready = cache.record(key, "step-a", map[string]any{"data": "a2"}, required, fanInPolicy{mode: "all"})
	if ready {
		t.Fatal("expected join not ready (still waiting for step-b)")
	}

	// Input from step-b completes.
	outputs, ready := cache.record(key, "step-b", map[string]any{"data": "b"}, required, fanInPolicy{mode: "all"})
	if !ready {
		t.Fatal("expected join to be ready")
	}

	// Verify step-a has latest data.
	stepA, ok := outputs["step-a"].(map[string]any)
	if !ok {
		t.Fatal("expected step-a in outputs")
	}
	stepAOutputs, ok := stepA["outputs"].(map[string]any)
	if !ok {
		t.Fatal("expected step-a outputs")
	}
	if stepAOutputs["data"] != "a2" {
		t.Fatalf("expected step-a data to be 'a2', got %v", stepAOutputs["data"])
	}
}

func TestJoinCache_TTLExpiration(t *testing.T) {
	cache := &joinCache{
		entries:    make(map[joinKey]*joinEntry),
		order:      nil,
		ttl:        100 * time.Millisecond,
		maxEntries: 1000,
		log:        logr.Discard(),
	}
	cache.order = newList()

	key := joinKey{
		storyRunName: "run-5",
		storyRunNS:   "default",
		stepID:       "step-join-ttl",
		joinID:       "join-5",
	}
	required := []string{"step-a", "step-b"}

	// Add first input.
	_, ready := cache.record(key, "step-a", map[string]any{"data": "a"}, required, fanInPolicy{mode: "all"})
	if ready {
		t.Fatal("expected join not ready")
	}

	// Wait for TTL to expire.
	time.Sleep(150 * time.Millisecond)

	// Next record should trigger eviction of expired entry.
	key2 := joinKey{
		storyRunName: "run-5b",
		storyRunNS:   "default",
		stepID:       "step-other",
		joinID:       "join-5b",
	}
	cache.record(key2, "step-x", map[string]any{}, []string{"step-x"}, fanInPolicy{mode: "any"})

	// Original entry should be gone.
	cache.mu.Lock()
	_, exists := cache.entries[key]
	cache.mu.Unlock()
	if exists {
		t.Fatal("expected expired entry to be evicted")
	}
}

func TestJoinCache_MaxEntriesEviction(t *testing.T) {
	cache := &joinCache{
		entries:    make(map[joinKey]*joinEntry),
		order:      nil,
		ttl:        time.Hour, // Long TTL to avoid time-based eviction.
		maxEntries: 3,
		log:        logr.Discard(),
	}
	cache.order = newList()

	// Add 3 entries.
	for i := range 3 {
		key := joinKey{
			storyRunName: "run",
			storyRunNS:   "default",
			stepID:       "step",
			joinID:       string(rune('a' + i)),
		}
		cache.record(key, "step-a", map[string]any{"i": i}, []string{"step-a", "step-b"}, fanInPolicy{mode: "all"})
	}

	cache.mu.Lock()
	if len(cache.entries) != 3 {
		cache.mu.Unlock()
		t.Fatalf("expected 3 entries, got %d", len(cache.entries))
	}
	cache.mu.Unlock()

	// Add a 4th entry, should evict the oldest.
	key4 := joinKey{
		storyRunName: "run",
		storyRunNS:   "default",
		stepID:       "step",
		joinID:       "d",
	}
	cache.record(key4, "step-a", map[string]any{"i": 3}, []string{"step-a", "step-b"}, fanInPolicy{mode: "all"})

	cache.mu.Lock()
	numEntries := len(cache.entries)
	_, firstExists := cache.entries[joinKey{storyRunName: "run", storyRunNS: "default", stepID: "step", joinID: "a"}]
	_, lastExists := cache.entries[joinKey{storyRunName: "run", storyRunNS: "default", stepID: "step", joinID: "d"}]
	cache.mu.Unlock()

	if numEntries != 3 {
		t.Fatalf("expected 3 entries after eviction, got %d", numEntries)
	}
	if firstExists {
		t.Fatal("expected oldest entry 'a' to be evicted")
	}
	if !lastExists {
		t.Fatal("expected newest entry 'd' to exist")
	}
}

func TestJoinCache_InputNotInRequiredList(t *testing.T) {
	cache := newJoinCache(logr.Discard())
	key := joinKey{
		storyRunName: "run-6",
		storyRunNS:   "default",
		stepID:       "step-join-filter",
		joinID:       "join-6",
	}
	required := []string{"step-a", "step-b"}

	// Input from step-c (not in required list).
	_, ready := cache.record(key, "step-c", map[string]any{"data": "c"}, required, fanInPolicy{mode: "all"})
	if ready {
		t.Fatal("expected join not ready (input not in required list)")
	}

	// Input from step-a.
	_, ready = cache.record(key, "step-a", map[string]any{"data": "a"}, required, fanInPolicy{mode: "all"})
	if ready {
		t.Fatal("expected join not ready")
	}

	// Input from step-b completes.
	outputs, ready := cache.record(key, "step-b", map[string]any{"data": "b"}, required, fanInPolicy{mode: "all"})
	if !ready {
		t.Fatal("expected join to be ready")
	}

	// step-c should NOT be in outputs.
	if _, ok := outputs["step-c"]; ok {
		t.Fatal("expected step-c NOT to be in outputs")
	}
}

func TestJoinCache_PolicyTimeout(t *testing.T) {
	cache := &joinCache{
		entries:    make(map[joinKey]*joinEntry),
		order:      nil,
		ttl:        time.Hour, // Long default TTL.
		maxEntries: 1000,
		log:        logr.Discard(),
	}
	cache.order = newList()

	key := joinKey{
		storyRunName: "run-7",
		storyRunNS:   "default",
		stepID:       "step-join-policy-timeout",
		joinID:       "join-7",
	}
	required := []string{"step-a", "step-b"}

	// Add first input with a short policy timeout.
	_, ready := cache.record(key, "step-a", map[string]any{"data": "a"}, required, fanInPolicy{
		mode:       "all",
		hasTimeout: true,
		timeout:    100 * time.Millisecond,
	})
	if ready {
		t.Fatal("expected join not ready")
	}

	// Wait for policy timeout to expire.
	time.Sleep(150 * time.Millisecond)

	// Trigger eviction via another record.
	key2 := joinKey{
		storyRunName: "run-7b",
		storyRunNS:   "default",
		stepID:       "step-other",
		joinID:       "join-7b",
	}
	cache.record(key2, "step-x", map[string]any{}, []string{"step-x"}, fanInPolicy{mode: "any"})

	// Original entry should be gone.
	cache.mu.Lock()
	_, exists := cache.entries[key]
	cache.mu.Unlock()
	if exists {
		t.Fatal("expected entry to be evicted due to policy timeout")
	}
}

// newList is a helper to create a new list.List for tests.
func newList() *list.List {
	return list.New()
}
