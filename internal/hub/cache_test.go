package hub

import (
	"testing"
	"time"
)

func TestStoryCacheEvictsExpiredEntries(t *testing.T) {
	originalTTL := storyCacheTTL
	storyCacheTTL = 5 * time.Millisecond
	t.Cleanup(func() { storyCacheTTL = originalTTL })

	cache := newStoryCache(nil)

	cache.mu.Lock()
	cache.storeLocked("default/run-1", cacheEntry{createdAt: time.Now().Add(-time.Minute)})
	cache.pruneLocked(time.Now())
	_, exists := cache.cache["default/run-1"]
	cache.mu.Unlock()

	if exists {
		t.Fatalf("expected expired entry to be removed from cache")
	}
}

func TestStoryCacheBoundsSize(t *testing.T) {
	originalMax := maxStoryCacheEntries
	maxStoryCacheEntries = 2
	t.Cleanup(func() { maxStoryCacheEntries = originalMax })

	cache := newStoryCache(nil)

	cache.mu.Lock()
	cache.storeLocked("default/run-a", cacheEntry{createdAt: time.Now().Add(-3 * time.Second)})
	cache.storeLocked("default/run-b", cacheEntry{createdAt: time.Now().Add(-2 * time.Second)})
	cache.storeLocked("default/run-c", cacheEntry{createdAt: time.Now().Add(-1 * time.Second)})
	cache.mu.Unlock()

	cache.mu.Lock()
	_, stillHasOldest := cache.cache["default/run-a"]
	size := len(cache.cache)
	cache.mu.Unlock()

	if stillHasOldest {
		t.Fatalf("expected oldest entry to be evicted when cache exceeds capacity")
	}
	if size != 2 {
		t.Fatalf("expected cache size to equal max entries, got %d", size)
	}
}
