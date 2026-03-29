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
	"bytes"
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	"github.com/xeipuuv/gojsonschema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	templateSchemaCacheTTL        = 1 * time.Minute
	maxTemplateSchemaCacheEntries = 2048
)

type templateSchemaEntry struct {
	inputSchema      *gojsonschema.Schema
	inputSchemaName  string
	inputSchemaRaw   []byte
	outputSchema     *gojsonschema.Schema
	outputSchemaName string
	outputSchemaRaw  []byte
	resourceVersion  string
	createdAt        time.Time
	hasInputSchema   bool
	hasOutputSchema  bool
}

type templateSchemaCache struct {
	client client.Client
	mu     sync.Mutex
	cache  map[string]templateSchemaEntry
	order  *list.List
	index  map[string]*list.Element
}

func newTemplateSchemaCache(k8sClient client.Client) *templateSchemaCache {
	return &templateSchemaCache{
		client: k8sClient,
		cache:  make(map[string]templateSchemaEntry),
	}
}

// TODO: Pre-warm the template schema cache during StoryRun registration to
// avoid cold-start latency on the first packet. This would require the hub
// to fetch template schemas proactively when AddStream is called.

// Get returns the compiled input schema, its display name, and the raw schema bytes
// (used for default application). Returns (nil, "", nil, nil) if no input schema is defined.
func (c *templateSchemaCache) Get(ctx context.Context, templateName string) (*gojsonschema.Schema, string, []byte, error) {
	entry, err := c.getEntry(ctx, templateName)
	if err != nil {
		return nil, "", nil, err
	}
	if entry.hasInputSchema {
		return entry.inputSchema, entry.inputSchemaName, entry.inputSchemaRaw, nil
	}
	return nil, "", nil, nil
}

// GetOutputSchema returns the compiled output schema and its display name.
// Returns (nil, "", nil) if no output schema is defined.
func (c *templateSchemaCache) GetOutputSchema(ctx context.Context, templateName string) (*gojsonschema.Schema, string, error) {
	entry, err := c.getEntry(ctx, templateName)
	if err != nil {
		return nil, "", err
	}
	if entry.hasOutputSchema {
		return entry.outputSchema, entry.outputSchemaName, nil
	}
	return nil, "", nil
}

func (c *templateSchemaCache) getEntry(ctx context.Context, templateName string) (templateSchemaEntry, error) {
	if templateName == "" {
		return templateSchemaEntry{}, fmt.Errorf("template name is required")
	}
	cached, found := c.loadCachedEntry(templateName)
	if found && time.Since(cached.createdAt) >= templateSchemaCacheTTL {
		c.mu.Lock()
		c.removeLocked(templateName)
		c.mu.Unlock()
		found = false
	}

	// Use a short timeout to avoid blocking processPacket when the
	// EngramTemplate Informer hasn't synced yet (same issue as Transport
	// Informer in getTransportDefaultSettings).
	template, err := c.loadTemplate(ctx, templateName)
	if err != nil {
		if found {
			c.mu.Lock()
			c.removeLocked(templateName)
			c.mu.Unlock()
		}
		return templateSchemaEntry{}, err
	}
	if found && cached.matchesTemplate(&template) {
		c.mu.Lock()
		c.touchOrderLocked(templateName)
		c.mu.Unlock()
		return cached, nil
	}

	entry, err := buildTemplateSchemaEntry(&template)
	if err != nil {
		return templateSchemaEntry{}, err
	}

	c.mu.Lock()
	c.storeLocked(templateName, entry)
	c.mu.Unlock()

	return entry, nil
}

func (c *templateSchemaCache) loadCachedEntry(templateName string) (templateSchemaEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, found := c.cache[templateName]
	return entry, found
}

func (c *templateSchemaCache) loadTemplate(ctx context.Context, templateName string) (catalogv1alpha1.EngramTemplate, error) {
	getCtx, getCancel := context.WithTimeout(ctx, 2*time.Second)
	defer getCancel()
	var template catalogv1alpha1.EngramTemplate
	if err := c.client.Get(getCtx, types.NamespacedName{Name: templateName}, &template); err != nil {
		return catalogv1alpha1.EngramTemplate{}, err
	}
	return template, nil
}

func buildTemplateSchemaEntry(template *catalogv1alpha1.EngramTemplate) (templateSchemaEntry, error) {
	if template == nil {
		return templateSchemaEntry{}, fmt.Errorf("template is required")
	}
	entry := templateSchemaEntry{
		inputSchemaName:  fmt.Sprintf("EngramTemplate %q input", template.Name),
		outputSchemaName: fmt.Sprintf("EngramTemplate %q output", template.Name),
		resourceVersion:  template.ResourceVersion,
		createdAt:        time.Now(),
	}

	if template.Spec.InputSchema != nil && len(template.Spec.InputSchema.Raw) > 0 {
		entry.inputSchemaRaw = append([]byte(nil), template.Spec.InputSchema.Raw...)
		compiled, err := compileSchema(entry.inputSchemaRaw, entry.inputSchemaName)
		if err != nil {
			return templateSchemaEntry{}, err
		}
		entry.inputSchema = compiled
		entry.hasInputSchema = compiled != nil
	}

	if template.Spec.OutputSchema != nil && len(template.Spec.OutputSchema.Raw) > 0 {
		entry.outputSchemaRaw = append([]byte(nil), template.Spec.OutputSchema.Raw...)
		compiled, err := compileSchema(entry.outputSchemaRaw, entry.outputSchemaName)
		if err != nil {
			return templateSchemaEntry{}, err
		}
		entry.outputSchema = compiled
		entry.hasOutputSchema = compiled != nil
	}

	return entry, nil
}

func (e templateSchemaEntry) matchesTemplate(template *catalogv1alpha1.EngramTemplate) bool {
	if template == nil {
		return false
	}
	var inputSchemaRaw []byte
	if template.Spec.InputSchema != nil {
		inputSchemaRaw = template.Spec.InputSchema.Raw
	}
	var outputSchemaRaw []byte
	if template.Spec.OutputSchema != nil {
		outputSchemaRaw = template.Spec.OutputSchema.Raw
	}
	if e.resourceVersion != "" && template.ResourceVersion != "" && e.resourceVersion != template.ResourceVersion {
		return false
	}
	return bytes.Equal(e.inputSchemaRaw, inputSchemaRaw) && bytes.Equal(e.outputSchemaRaw, outputSchemaRaw)
}

func (c *templateSchemaCache) storeLocked(key string, entry templateSchemaEntry) {
	if c.cache == nil {
		c.cache = make(map[string]templateSchemaEntry)
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

func (c *templateSchemaCache) touchOrderLocked(key string) {
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

func (c *templateSchemaCache) pruneLocked(now time.Time) {
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
			if now.Sub(entry.createdAt) >= templateSchemaCacheTTL {
				c.removeElementLocked(elem, key)
				continue
			}
			break
		}
	}
	for len(c.cache) > maxTemplateSchemaCacheEntries {
		elem := c.order.Front()
		if elem == nil {
			break
		}
		key := elem.Value.(string)
		c.removeElementLocked(elem, key)
	}
}

func (c *templateSchemaCache) removeLocked(key string) {
	if elem, ok := c.index[key]; ok {
		c.removeElementLocked(elem, key)
		return
	}
	delete(c.cache, key)
}

func (c *templateSchemaCache) removeElementLocked(elem *list.Element, key string) {
	if elem != nil && c.order != nil {
		c.order.Remove(elem)
	}
	delete(c.index, key)
	delete(c.cache, key)
}
