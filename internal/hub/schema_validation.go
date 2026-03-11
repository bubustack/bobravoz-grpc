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
	"encoding/json"
	"fmt"

	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/xeipuuv/gojsonschema"
)

func validateJSONAgainstSchema(doc []byte, schema *gojsonschema.Schema, schemaName string) error {
	if schema == nil {
		return nil
	}
	documentLoader := gojsonschema.NewBytesLoader(doc)
	result, err := schema.Validate(documentLoader)
	if err != nil {
		return fmt.Errorf("error validating against %s schema: %w", schemaName, err)
	}
	if !result.Valid() {
		var errs []string
		for _, desc := range result.Errors() {
			errs = append(errs, desc.String())
		}
		return fmt.Errorf("object is invalid against %s schema: %v", schemaName, errs)
	}
	return nil
}

func compileSchema(schema []byte, schemaName string) (*gojsonschema.Schema, error) {
	if len(schema) == 0 {
		return nil, nil
	}
	normalizedSchema, err := normalizeSchemaBytes(schema)
	if err != nil {
		return nil, fmt.Errorf("error validating %s schema: failed to normalize schema: %w", schemaName, err)
	}
	schemaLoader := gojsonschema.NewBytesLoader(normalizedSchema)
	compiled, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		return nil, fmt.Errorf("error validating %s schema: %w", schemaName, err)
	}
	return compiled, nil
}

func normalizeSchemaBytes(schema []byte) ([]byte, error) {
	if len(schema) == 0 {
		return schema, nil
	}
	var root any
	if err := json.Unmarshal(schema, &root); err != nil {
		return nil, err
	}
	normalized := normalizeSchemaNode(root)
	out, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeSchemaNode(node any) any {
	switch typed := node.(type) {
	case map[string]any:
		normalized := normalizeObjectSchema(typed)
		return allowStorageRefAlternative(normalized)
	case []any:
		for i := range typed {
			typed[i] = normalizeSchemaNode(typed[i])
		}
		return typed
	default:
		return node
	}
}

func normalizeObjectSchema(obj map[string]any) map[string]any {
	requiredSet := liftInlineRequiredFlags(obj)
	mergeRequiredSet(obj, requiredSet)
	normalizeNestedSchemaLocations(obj)
	return obj
}

func allowStorageRefAlternative(schema map[string]any) any {
	if schema == nil || len(schema) == 0 {
		return schema
	}
	if isImplicitRefSchema(schema) {
		return schema
	}
	if anyOf, ok := schema["anyOf"].([]any); ok {
		if !schemaSliceHasImplicitRef(anyOf) {
			schema["anyOf"] = append(anyOf, storageRefSchema(), configMapRefSchema(), secretRefSchema())
		}
		if !schemaSliceHasTemplateString(anyOf) {
			schema["anyOf"] = append(schema["anyOf"].([]any), templateStringSchema())
		}
		return schema
	}
	if oneOf, ok := schema["oneOf"].([]any); ok {
		if !schemaSliceHasImplicitRef(oneOf) {
			schema["oneOf"] = append(oneOf, storageRefSchema(), configMapRefSchema(), secretRefSchema())
		}
		if !schemaSliceHasTemplateString(oneOf) {
			schema["oneOf"] = append(schema["oneOf"].([]any), templateStringSchema())
		}
		return schema
	}

	wrapped := map[string]any{
		"anyOf": []any{schema, storageRefSchema(), configMapRefSchema(), secretRefSchema(), templateStringSchema()},
	}
	if title, ok := schema["title"]; ok {
		wrapped["title"] = title
	}
	if desc, ok := schema["description"]; ok {
		wrapped["description"] = desc
	}
	return wrapped
}

func schemaSliceHasImplicitRef(schemas []any) bool {
	for _, entry := range schemas {
		if schemaMap, ok := entry.(map[string]any); ok && isImplicitRefSchema(schemaMap) {
			return true
		}
	}
	return false
}

func schemaSliceHasTemplateString(schemas []any) bool {
	for _, entry := range schemas {
		if schemaMap, ok := entry.(map[string]any); ok && isTemplateStringSchema(schemaMap) {
			return true
		}
	}
	return false
}

func isImplicitRefSchema(schema map[string]any) bool {
	return isStorageRefSchema(schema) || isConfigMapRefSchema(schema) || isSecretRefSchema(schema)
}

func isStorageRefSchema(schema map[string]any) bool {
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, hasRef := props[storage.StorageRefKey]
	return hasRef
}

func isConfigMapRefSchema(schema map[string]any) bool {
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, hasRef := props["$bubuConfigMapRef"]
	return hasRef
}

func isSecretRefSchema(schema map[string]any) bool {
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, hasRef := props["$bubuSecretRef"]
	return hasRef
}

func isTemplateStringSchema(schema map[string]any) bool {
	if schema["type"] != "string" {
		return false
	}
	_, hasPattern := schema["pattern"].(string)
	return hasPattern
}

func storageRefSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			storage.StorageRefKey:           map[string]any{"type": "string"},
			storage.StoragePathKey:          map[string]any{"type": "string"},
			storage.StorageContentTypeKey:   map[string]any{"type": "string"},
			storage.StorageSchemaKey:        map[string]any{"type": "string"},
			storage.StorageSchemaVersionKey: map[string]any{"type": "string"},
		},
		"required": []any{storage.StorageRefKey},
	}
}

func configMapRefSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"$bubuConfigMapRef": map[string]any{
				"anyOf": []any{
					map[string]any{"type": "string"},
					map[string]any{
						"type":                 "object",
						"additionalProperties": false,
						"properties": map[string]any{
							"name":      map[string]any{"type": "string"},
							"key":       map[string]any{"type": "string"},
							"namespace": map[string]any{"type": "string"},
							"format": map[string]any{
								"type": "string",
								"enum": []any{"auto", "json", "raw"},
							},
						},
						"required": []any{"name", "key"},
					},
				},
			},
		},
		"required": []any{"$bubuConfigMapRef"},
	}
}

func secretRefSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"$bubuSecretRef": map[string]any{
				"anyOf": []any{
					map[string]any{"type": "string"},
					map[string]any{
						"type":                 "object",
						"additionalProperties": false,
						"properties": map[string]any{
							"name":      map[string]any{"type": "string"},
							"key":       map[string]any{"type": "string"},
							"namespace": map[string]any{"type": "string"},
							"format": map[string]any{
								"type": "string",
								"enum": []any{"auto", "json", "raw"},
							},
						},
						"required": []any{"name", "key"},
					},
				},
			},
		},
		"required": []any{"$bubuSecretRef"},
	}
}

func templateStringSchema() map[string]any {
	return map[string]any{
		"type":    "string",
		"pattern": `^\s*\$?\{\{[\s\S]+\}\}\s*$`,
	}
}

const maxSchemaDefaultDepth = 32

// applySchemaDefaults applies JSON Schema "default" values to the provided input map.
// This mirrors the batch-path behavior in bobrapet/pkg/runs/inputs/defaults.go.
func applySchemaDefaults(schemaRaw []byte, input map[string]any) (map[string]any, error) {
	if len(schemaRaw) == 0 {
		return cloneJSONMap(input)
	}
	var schema map[string]any
	if err := json.Unmarshal(schemaRaw, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}
	resolved, err := cloneJSONMap(input)
	if err != nil {
		return nil, err
	}
	applyDefaultsToObject(schema, resolved, 0)
	return resolved, nil
}

func applyDefaultsToObject(schema map[string]any, obj map[string]any, depth int) bool {
	if schema == nil || obj == nil || depth > maxSchemaDefaultDepth {
		return false
	}
	changed := false
	if allOf, ok := schema["allOf"].([]any); ok {
		for _, entry := range allOf {
			if sub, ok := entry.(map[string]any); ok {
				if applyDefaultsToObject(sub, obj, depth+1) {
					changed = true
				}
			}
		}
	}
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return changed
	}
	for key, raw := range props {
		propSchema, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		val, exists := obj[key]
		if !exists {
			if def, ok := propSchema["default"]; ok {
				obj[key] = cloneJSONValue(def)
				changed = true
				continue
			}
			if schemaHasPropertyDefaults(propSchema, depth+1) {
				child := make(map[string]any)
				if applyDefaultsToObject(propSchema, child, depth+1) {
					obj[key] = child
					changed = true
				}
			}
			continue
		}
		if nested, ok := val.(map[string]any); ok {
			if applyDefaultsToObject(propSchema, nested, depth+1) {
				changed = true
			}
		}
	}
	return changed
}

func schemaHasPropertyDefaults(schema map[string]any, depth int) bool {
	if schema == nil || depth > maxSchemaDefaultDepth {
		return false
	}
	if schema["type"] != nil && schema["type"] != "object" {
		return false
	}
	if allOf, ok := schema["allOf"].([]any); ok {
		for _, entry := range allOf {
			if sub, ok := entry.(map[string]any); ok {
				if schemaHasPropertyDefaults(sub, depth+1) {
					return true
				}
			}
		}
	}
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	for _, raw := range props {
		propSchema, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if _, ok := propSchema["default"]; ok {
			return true
		}
		if schemaHasPropertyDefaults(propSchema, depth+1) {
			return true
		}
	}
	return false
}

func cloneJSONMap(input map[string]any) (map[string]any, error) {
	if input == nil {
		return make(map[string]any), nil
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("failed to clone input map: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("failed to clone input map: %w", err)
	}
	if out == nil {
		out = make(map[string]any)
	}
	return out, nil
}

func cloneJSONValue(value any) any {
	raw, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return value
	}
	return out
}

func liftInlineRequiredFlags(obj map[string]any) map[string]struct{} {
	props, hasProps := obj["properties"].(map[string]any)
	if !hasProps {
		return nil
	}

	requiredSet := map[string]struct{}{}
	for propName, rawChild := range props {
		cleaned := stripBooleanRequired(rawChild, propName, requiredSet)
		props[propName] = normalizeSchemaNode(cleaned)
	}
	if len(requiredSet) == 0 {
		return nil
	}
	return requiredSet
}

func stripBooleanRequired(node any, propName string, requiredSet map[string]struct{}) any {
	childMap, ok := node.(map[string]any)
	if !ok {
		return node
	}
	if raw, has := childMap["required"]; has {
		if b, ok := raw.(bool); ok {
			if b {
				requiredSet[propName] = struct{}{}
			}
			delete(childMap, "required")
		}
	}
	return childMap
}

func mergeRequiredSet(obj map[string]any, requiredSet map[string]struct{}) {
	if len(requiredSet) == 0 {
		return
	}

	existingList := extractExistingRequired(obj)
	seen := make(map[string]struct{}, len(existingList))
	for _, name := range existingList {
		seen[name] = struct{}{}
	}

	for name := range requiredSet {
		if _, already := seen[name]; !already {
			existingList = append(existingList, name)
		}
	}

	out := make([]any, 0, len(existingList))
	for _, name := range existingList {
		out = append(out, name)
	}
	obj["required"] = out
}

func extractExistingRequired(obj map[string]any) []string {
	raw, has := obj["required"]
	if !has {
		return nil
	}

	switch typed := raw.(type) {
	case []any:
		var result []string
		for _, v := range typed {
			if s, ok := v.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return append([]string{}, typed...)
	default:
		return nil
	}
}

func normalizeNestedSchemaLocations(obj map[string]any) {
	normalizeItemsNode(obj)
	normalizeSingleSchemaField(obj, "additionalProperties")
	normalizeMapOfSchemas(obj, "patternProperties")
	normalizeSchemaSlice(obj, "allOf")
	normalizeSchemaSlice(obj, "anyOf")
	normalizeSchemaSlice(obj, "oneOf")
	normalizeMapOfSchemas(obj, "definitions")
	normalizeMapOfSchemas(obj, "$defs")
	normalizeSingleSchemaField(obj, "not")
}

func normalizeItemsNode(obj map[string]any) {
	items, has := obj["items"]
	if !has {
		return
	}
	switch typed := items.(type) {
	case map[string]any, []any:
		obj["items"] = normalizeSchemaNode(typed)
	}
}

func normalizeSingleSchemaField(obj map[string]any, key string) {
	if raw, has := obj[key]; has {
		if schemaMap, ok := raw.(map[string]any); ok {
			obj[key] = normalizeSchemaNode(schemaMap)
		}
	}
}

func normalizeMapOfSchemas(obj map[string]any, key string) {
	raw, has := obj[key].(map[string]any)
	if !has {
		return
	}
	for k, v := range raw {
		raw[k] = normalizeSchemaNode(v)
	}
}

func normalizeSchemaSlice(obj map[string]any, key string) {
	raw, has := obj[key].([]any)
	if !has {
		return
	}
	for i := range raw {
		raw[i] = normalizeSchemaNode(raw[i])
	}
	obj[key] = raw
}
