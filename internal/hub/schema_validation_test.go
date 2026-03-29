package hub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeSchemaBytes_RejectsExcessiveDepth(t *testing.T) {
	schema := nestedObjectSchema(maxSchemaNormalizationDepth + 2)

	_, err := normalizeSchemaBytes(mustMarshalJSON(t, schema))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "max normalization depth")
}

func nestedObjectSchema(depth int) map[string]any {
	node := map[string]any{"type": "string"}
	for range depth {
		node = map[string]any{
			"type": "object",
			"properties": map[string]any{
				"child": node,
			},
		}
	}
	return node
}

func TestSchemaValidationDeepNesting(t *testing.T) {
	// Create a schema nested to depth 25+ and verify normalization completes within 5 seconds.
	// This tests that we don't have exponential blowup in deep schemas.
	schema := nestedObjectSchema(25)

	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		_, err = normalizeSchemaBytes(mustMarshalJSON(t, schema))
	}()

	select {
	case <-done:
		// Normalization completed (may succeed or fail with depth error, either is acceptable)
		// The important thing is it didn't hang
		if err != nil {
			// Expected: depth exceeded error
			assert.Contains(t, err.Error(), "max normalization depth")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("schema normalization did not complete within 5 seconds - possible infinite loop or exponential blowup")
	}
}

func TestSchemaValidationDeepNestingWithinLimit(t *testing.T) {
	// Create a schema at exactly the max depth limit and verify it completes quickly.
	schema := nestedObjectSchema(maxSchemaNormalizationDepth)

	done := make(chan struct{})
	var result []byte
	var err error
	go func() {
		defer close(done)
		result, err = normalizeSchemaBytes(mustMarshalJSON(t, schema))
	}()

	select {
	case <-done:
		// Should succeed at max depth
		require.NoError(t, err, "schema at max depth should normalize successfully")
		require.NotNil(t, result, "result should not be nil")
	case <-time.After(5 * time.Second):
		t.Fatal("schema normalization at max depth did not complete within 5 seconds")
	}
}
