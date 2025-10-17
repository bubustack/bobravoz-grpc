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
	"sync"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	cacheTTL = 1 * time.Minute
)

type cacheEntry struct {
	storyRun  *runsv1alpha1.StoryRun
	story     *bubuv1alpha1.Story
	createdAt time.Time
}

type storyCache struct {
	client client.Client
	mu     sync.Mutex
	cache  map[string]cacheEntry
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
	entry, found := c.cache[key]
	if found && time.Since(entry.createdAt) < cacheTTL {
		c.mu.Unlock()
		return entry.storyRun, entry.story, nil
	}
	c.mu.Unlock()

	// Fetch from API server
	var storyRun runsv1alpha1.StoryRun
	if err := c.client.Get(ctx, k8stypes.NamespacedName{Name: storyRunName, Namespace: storyRunNS}, &storyRun); err != nil {
		return nil, nil, err
	}

	var story bubuv1alpha1.Story
	if err := c.client.Get(ctx, k8stypes.NamespacedName{Name: storyRun.Spec.StoryRef.Name, Namespace: storyRunNS}, &story); err != nil {
		return nil, nil, err
	}

	// Update cache
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		storyRun:  &storyRun,
		story:     &story,
		createdAt: time.Now(),
	}
	c.mu.Unlock()

	return &storyRun, &story, nil
}
