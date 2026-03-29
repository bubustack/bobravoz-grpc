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
	"testing"
	"time"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolveBufferLimits_StorySettings(t *testing.T) {
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Transports: []bubuv1alpha1.StoryTransport{
				{
					Name:         "rt",
					TransportRef: "rt",
					Settings: rawExtensionFromMap(t, map[string]any{
						"backpressure": map[string]any{
							"buffer": map[string]any{
								"maxMessages": 5,
								"maxBytes":    2048,
								"dropPolicy":  "drop_oldest",
							},
						},
					}),
				},
			},
		},
	}

	limits := resolveBufferLimits(context.Background(), nil, story, "rt")
	assert.Equal(t, 5, limits.maxMessages)
	assert.Equal(t, 2048, limits.maxBytes)
	assert.Equal(t, bufferDropOldest, limits.dropPolicy)
}

func TestResolveBufferLimits_MergeTransportDefaults(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))
	transport := &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{Name: "rt"},
		Spec: transportv1alpha1.TransportSpec{
			DefaultSettings: rawExtensionFromMap(t, map[string]any{
				"backpressure": map[string]any{
					"buffer": map[string]any{
						"maxMessages": 100,
						"maxBytes":    8192,
						"dropPolicy":  "drop_newest",
					},
				},
			}),
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(transport).Build()
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Transports: []bubuv1alpha1.StoryTransport{
				{
					Name:         "rt",
					TransportRef: "rt",
					Settings: rawExtensionFromMap(t, map[string]any{
						"backpressure": map[string]any{
							"buffer": map[string]any{
								"maxMessages": 10,
								"dropPolicy":  "drop_oldest",
							},
						},
					}),
				},
			},
		},
	}

	limits := resolveBufferLimits(context.Background(), client, story, "rt")
	assert.Equal(t, 10, limits.maxMessages)
	assert.Equal(t, 8192, limits.maxBytes)
	assert.Equal(t, bufferDropOldest, limits.dropPolicy)
}

func TestEvictStaleTransportDefaults_RemovesOldEntries(t *testing.T) {
	// Save original cache state and restore after test.
	transportDefaultsCache.mu.Lock()
	originalEntries := transportDefaultsCache.entries
	transportDefaultsCache.entries = make(map[string]transportDefaultsEntry)
	transportDefaultsCache.mu.Unlock()
	defer func() {
		transportDefaultsCache.mu.Lock()
		transportDefaultsCache.entries = originalEntries
		transportDefaultsCache.mu.Unlock()
	}()

	// Insert entries with varying lastAccess times.
	now := time.Now()
	stale := now.Add(-transportDefaultsMaxAge - time.Second)
	recent := now.Add(-time.Second)

	transportDefaultsCache.mu.Lock()
	transportDefaultsCache.entries["stale-transport"] = transportDefaultsEntry{
		fetchedAt:  stale,
		lastAccess: stale,
	}
	transportDefaultsCache.entries["recent-transport"] = transportDefaultsEntry{
		fetchedAt:  recent,
		lastAccess: recent,
	}
	transportDefaultsCache.mu.Unlock()

	// Call eviction routine.
	transportDefaultsCache.mu.Lock()
	evictStaleTransportDefaults(now)
	remaining := len(transportDefaultsCache.entries)
	_, recentExists := transportDefaultsCache.entries["recent-transport"]
	_, staleExists := transportDefaultsCache.entries["stale-transport"]
	transportDefaultsCache.mu.Unlock()

	assert.Equal(t, 1, remaining, "expected only recent entry to remain")
	assert.True(t, recentExists, "recent-transport should not be evicted")
	assert.False(t, staleExists, "stale-transport should be evicted")
}

func TestTransportDefaultsCache_StaleEviction(t *testing.T) {
	// Verify that stale entries (older than transportDefaultsMaxAge) are evicted
	// during cache lookup. This tests the evictStaleTransportDefaults behavior.
	transportDefaultsCache.mu.Lock()
	originalEntries := transportDefaultsCache.entries
	transportDefaultsCache.entries = make(map[string]transportDefaultsEntry)
	// Insert a stale entry.
	transportDefaultsCache.entries["stale-key"] = transportDefaultsEntry{
		fetchedAt:  time.Now().Add(-2 * transportDefaultsTTL),
		lastAccess: time.Now().Add(-transportDefaultsMaxAge - time.Minute),
	}
	// Insert a fresh entry.
	transportDefaultsCache.entries["fresh-key"] = transportDefaultsEntry{
		fetchedAt:  time.Now(),
		lastAccess: time.Now(),
	}
	transportDefaultsCache.mu.Unlock()
	defer func() {
		transportDefaultsCache.mu.Lock()
		transportDefaultsCache.entries = originalEntries
		transportDefaultsCache.mu.Unlock()
	}()

	// Trigger eviction.
	evictStaleTransportDefaults(time.Now())

	transportDefaultsCache.mu.Lock()
	_, staleExists := transportDefaultsCache.entries["stale-key"]
	_, freshExists := transportDefaultsCache.entries["fresh-key"]
	transportDefaultsCache.mu.Unlock()

	assert.False(t, staleExists, "stale entry should be evicted")
	assert.True(t, freshExists, "fresh entry should be kept")
}
