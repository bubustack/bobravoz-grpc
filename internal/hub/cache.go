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
	"context"
	"os"
	"sync"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	storyCacheTTL        = 30 * time.Second
	maxStoryCacheEntries = 2048
)

func init() {
	if v := os.Getenv("BOBRAVOZ_HUB_STORY_CACHE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			storyCacheTTL = d
		}
	}
}

type cacheEntry struct {
	storyRun  *runsv1alpha1.StoryRun
	story     *bubuv1alpha1.Story
	createdAt time.Time
}

type storyCache struct {
	client client.Client
	mu     sync.Mutex
	cache  map[string]cacheEntry
	order  *list.List
	index  map[string]*list.Element
}

func newStoryCache(k8sClient client.Client) *storyCache {
	return &storyCache{
		client: k8sClient,
		cache:  make(map[string]cacheEntry),
	}
}

func (c *storyCache) Get(ctx context.Context, storyRunName, storyRunNS string) (*runsv1alpha1.StoryRun, *bubuv1alpha1.Story, error) {
	key := storyRunNS + "/" + storyRunName
	c.mu.Lock()
	if entry, found := c.cache[key]; found {
		if time.Since(entry.createdAt) < storyCacheTTL {
			c.mu.Unlock()
			return entry.storyRun, entry.story, nil
		}
		c.removeLocked(key)
	}
	c.mu.Unlock()

	// Use a short timeout to avoid blocking processPacket when an Informer
	// hasn't synced yet (same issue as Transport Informer — see backpressure.go).
	getCtx, getCancel := context.WithTimeout(ctx, 2*time.Second)
	defer getCancel()

	// Fetch from API server
	var storyRun runsv1alpha1.StoryRun
	if err := c.client.Get(getCtx, k8stypes.NamespacedName{Name: storyRunName, Namespace: storyRunNS}, &storyRun); err != nil {
		return nil, nil, err
	}

	storyNamespace := refs.ResolveNamespace(&storyRun, &storyRun.Spec.StoryRef.ObjectReference)
	var story bubuv1alpha1.Story
	if err := c.client.Get(getCtx, k8stypes.NamespacedName{Name: storyRun.Spec.StoryRef.Name, Namespace: storyNamespace}, &story); err != nil {
		return nil, nil, err
	}

	// Update cache
	c.mu.Lock()
	c.storeLocked(key, cacheEntry{
		storyRun:  &storyRun,
		story:     &story,
		createdAt: time.Now(),
	})
	c.mu.Unlock()

	return &storyRun, &story, nil
}

func (c *storyCache) storeLocked(key string, entry cacheEntry) {
	if c.cache == nil {
		c.cache = make(map[string]cacheEntry)
	}
	if c.order == nil {
		c.order = list.New()
	}
	if c.index == nil {
		c.index = make(map[string]*list.Element)
	}
	if entry.createdAt.IsZero() {
		entry.createdAt = time.Now()
	}
	c.cache[key] = entry
	c.touchOrderLocked(key)
	c.pruneLocked(time.Now())
}

func (c *storyCache) touchOrderLocked(key string) {
	if elem, ok := c.index[key]; ok {
		c.order.MoveToBack(elem)
		return
	}
	elem := c.order.PushBack(key)
	if c.index == nil {
		c.index = make(map[string]*list.Element)
	}
	c.index[key] = elem
}

func (c *storyCache) pruneLocked(now time.Time) {
	if c.order != nil {
		for {
			elem := c.order.Front()
			if elem == nil {
				break
			}
			key := elem.Value.(string)
			entry, ok := c.cache[key]
			if !ok {
				c.removeElementLocked(elem, key)
				continue
			}
			if now.Sub(entry.createdAt) >= storyCacheTTL {
				c.removeElementLocked(elem, key)
				continue
			}
			break
		}
	}
	for len(c.cache) > maxStoryCacheEntries {
		elem := c.order.Front()
		if elem == nil {
			break
		}
		key := elem.Value.(string)
		c.removeElementLocked(elem, key)
	}
}

func (c *storyCache) removeLocked(key string) {
	if elem, ok := c.index[key]; ok {
		c.removeElementLocked(elem, key)
		return
	}
	delete(c.cache, key)
}

func (c *storyCache) removeElementLocked(elem *list.Element, key string) {
	if elem != nil && c.order != nil {
		c.order.Remove(elem)
	}
	delete(c.index, key)
	delete(c.cache, key)
}
