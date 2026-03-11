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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplySchemaDefaults_EmptySchema(t *testing.T) {
	input := map[string]any{"key": "value"}
	result, err := applySchemaDefaults(nil, input)
	require.NoError(t, err)
	assert.Equal(t, "value", result["key"])
}

func TestApplySchemaDefaults_NilInput(t *testing.T) {
	schema := mustMarshalJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string", "default": "world"},
		},
	})

	result, err := applySchemaDefaults(schema, nil)
	require.NoError(t, err)
	assert.Equal(t, "world", result["name"])
}

func TestApplySchemaDefaults_AppliesMissingFields(t *testing.T) {
	schema := mustMarshalJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"timeout":  map[string]any{"type": "string", "default": "30s"},
			"retries":  map[string]any{"type": "integer", "default": float64(3)},
			"language": map[string]any{"type": "string", "default": "en"},
		},
	})
	input := map[string]any{
		"language": "fr",
	}

	result, err := applySchemaDefaults(schema, input)
	require.NoError(t, err)
	assert.Equal(t, "fr", result["language"], "explicit value should not be overridden")
	assert.Equal(t, "30s", result["timeout"], "missing field should get default")
	assert.Equal(t, float64(3), result["retries"], "missing field should get default")
}

func TestApplySchemaDefaults_NestedObject(t *testing.T) {
	schema := mustMarshalJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"config": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"debug":    map[string]any{"type": "boolean", "default": false},
					"logLevel": map[string]any{"type": "string", "default": "info"},
				},
			},
		},
	})
	input := map[string]any{
		"config": map[string]any{
			"debug": true,
		},
	}

	result, err := applySchemaDefaults(schema, input)
	require.NoError(t, err)

	config, ok := result["config"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, config["debug"], "explicit nested value should be preserved")
	assert.Equal(t, "info", config["logLevel"], "missing nested field should get default")
}

func TestApplySchemaDefaults_DoesNotMutateOriginal(t *testing.T) {
	schema := mustMarshalJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"added": map[string]any{"type": "string", "default": "new"},
		},
	})
	input := map[string]any{"existing": "keep"}

	result, err := applySchemaDefaults(schema, input)
	require.NoError(t, err)
	assert.Equal(t, "new", result["added"])

	// Original should not be modified.
	_, hasAdded := input["added"]
	assert.False(t, hasAdded, "original input map should not be mutated")
}

func TestApplySchemaDefaults_AllOfMerge(t *testing.T) {
	schema := mustMarshalJSON(t, map[string]any{
		"type": "object",
		"allOf": []any{
			map[string]any{
				"properties": map[string]any{
					"a": map[string]any{"type": "string", "default": "alpha"},
				},
			},
			map[string]any{
				"properties": map[string]any{
					"b": map[string]any{"type": "integer", "default": float64(42)},
				},
			},
		},
	})

	result, err := applySchemaDefaults(schema, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "alpha", result["a"])
	assert.Equal(t, float64(42), result["b"])
}

func TestApplySchemaDefaults_NoDefaultsNoChange(t *testing.T) {
	schema := mustMarshalJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
		},
	})
	input := map[string]any{"name": "bob"}

	result, err := applySchemaDefaults(schema, input)
	require.NoError(t, err)
	assert.Equal(t, "bob", result["name"])
	assert.Len(t, result, 1)
}

func TestApplySchemaDefaults_InvalidSchemaJSON(t *testing.T) {
	_, err := applySchemaDefaults([]byte("not json"), map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal schema")
}

func TestApplySchemaDefaults_CreatesNestedObjectFromDefaults(t *testing.T) {
	schema := mustMarshalJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"settings": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"enabled": map[string]any{"type": "boolean", "default": true},
				},
			},
		},
	})

	result, err := applySchemaDefaults(schema, map[string]any{})
	require.NoError(t, err)

	settings, ok := result["settings"].(map[string]any)
	require.True(t, ok, "should create nested object from defaults")
	assert.Equal(t, true, settings["enabled"])
}

func TestValidateEngramOutputs_NilEngram(t *testing.T) {
	s := &Server{}
	err := s.validateEngramOutputs(context.Background(), "step-1", nil, map[string]any{"k": "v"})
	assert.NoError(t, err)
}

func TestValidateEngramOutputs_EmptyOutputs(t *testing.T) {
	s := &Server{}
	err := s.validateEngramOutputs(context.Background(), "step-1", nil, nil)
	assert.NoError(t, err)
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
