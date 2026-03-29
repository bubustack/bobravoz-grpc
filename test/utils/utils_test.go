package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNonEmptyLines(t *testing.T) {
	lines := GetNonEmptyLines("\nalpha\n\nbeta\n")

	assert.Equal(t, []string{"alpha", "beta"}, lines)
}

func TestGetNonEmptyLines_EmptyOutput(t *testing.T) {
	lines := GetNonEmptyLines("")

	assert.Empty(t, lines)
}
