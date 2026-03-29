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
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/bubustack/core/contracts"
	"github.com/go-logr/logr"
)

const (
	defaultJoinTTL        = 2 * time.Minute
	defaultJoinMaxEntries = 1000
)

type joinKey struct {
	storyRunName string
	storyRunNS   string
	stepID       string
	joinID       string
}

type joinEntry struct {
	createdAt time.Time
	expiresAt time.Time
	steps     map[string]map[string]any
	listElem  *list.Element
}

type joinCache struct {
	mu         sync.Mutex
	entries    map[joinKey]*joinEntry
	order      *list.List // insertion order for O(1) eviction
	ttl        time.Duration
	maxEntries int
	log        logr.Logger
}

func newJoinCache(logger logr.Logger) *joinCache {
	ttl := defaultJoinTTL
	if v := os.Getenv(contracts.HubJoinCacheTTLEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			ttl = d
		} else {
			logger.Info("invalid join cache config, using defaults",
				"envVar", contracts.HubJoinCacheTTLEnv, "value", v, "default", defaultJoinTTL)
		}
	}
	maxEntries := defaultJoinMaxEntries
	if v := os.Getenv(contracts.HubJoinCacheMaxEntriesEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxEntries = n
		} else {
			logger.Info("invalid join cache config, using defaults",
				"envVar", contracts.HubJoinCacheMaxEntriesEnv, "value", v, "default", defaultJoinMaxEntries)
		}
	}
	return &joinCache{
		entries:    make(map[joinKey]*joinEntry),
		order:      list.New(),
		ttl:        ttl,
		maxEntries: maxEntries,
		log:        logger.WithName("join-cache"),
	}
}

//nolint:gocyclo // Join cache updates combine eviction, fan-in policy handling, and output merge semantics in one critical path.
func (c *joinCache) record(key joinKey, upstreamStepID string, outputs map[string]any, required []string, policy fanInPolicy) (map[string]any, bool) {
	if c == nil {
		return nil, false
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.evictExpiredLocked(now)
	entry := c.entries[key]
	if entry == nil {
		maxEntries := c.maxEntries
		if policy.maxEntries > 0 {
			maxEntries = policy.maxEntries
		}
		if maxEntries > 0 && len(c.entries) >= maxEntries {
			c.evictOldestLocked()
		}
		entry = &joinEntry{
			createdAt: now,
			steps:     make(map[string]map[string]any),
		}
		if policy.hasTimeout {
			if policy.timeout > 0 {
				entry.expiresAt = now.Add(policy.timeout)
			}
		} else if c.ttl > 0 {
			entry.expiresAt = now.Add(c.ttl)
		}
		entry.listElem = c.order.PushBack(key)
		c.entries[key] = entry
	}
	requiredSet := make(map[string]struct{}, len(required))
	for _, need := range required {
		if need == "" {
			continue
		}
		requiredSet[need] = struct{}{}
	}
	if outputs != nil {
		if _, ok := requiredSet[upstreamStepID]; ok {
			entry.steps[upstreamStepID] = outputs
		}
	}

	requiredCount := len(requiredSet)
	available := 0
	for need := range requiredSet {
		if _, ok := entry.steps[need]; ok {
			available++
		}
	}

	ready := false
	switch policy.mode {
	case "any":
		ready = available >= 1
	case "quorum":
		quorum := policy.quorum
		if quorum <= 0 {
			quorum = requiredCount
		}
		ready = available >= quorum
	default:
		ready = available >= requiredCount
	}
	if !ready {
		return nil, false
	}

	stepVars := make(map[string]any, available)
	for need := range requiredSet {
		if outputs, ok := entry.steps[need]; ok {
			stepVars[need] = map[string]any{
				"outputs": outputs,
			}
		}
	}
	if entry.listElem != nil {
		c.order.Remove(entry.listElem)
	}
	delete(c.entries, key)
	return stepVars, true
}

func (c *joinCache) evictExpiredLocked(now time.Time) {
	for k, entry := range c.entries {
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			if entry.listElem != nil {
				c.order.Remove(entry.listElem)
			}
			delete(c.entries, k)
		}
	}
}

func (c *joinCache) evictOldestLocked() {
	front := c.order.Front()
	if front == nil {
		return
	}
	oldestKey, ok := front.Value.(joinKey)
	if !ok {
		c.order.Remove(front)
		return
	}
	c.order.Remove(front)
	delete(c.entries, oldestKey)
	c.log.V(1).Info("Evicted oldest join entry", "storyRun", oldestKey.storyRunName, "step", oldestKey.stepID)
}
