package e2e

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2EMetricsCleanupDeletesClusterRoleBinding(t *testing.T) {
	source, err := os.ReadFile("e2e_test.go")
	require.NoError(t, err)

	text := string(source)
	assert.Contains(t, text, `"delete", "clusterrolebinding", metricsRoleBindingName`)
	assert.Contains(t, text, `"--ignore-not-found=true"`)
}

func TestE2ESuiteHasNoDeadCertManagerFlags(t *testing.T) {
	source, err := os.ReadFile("e2e_suite_test.go")
	require.NoError(t, err)

	text := string(source)
	assert.NotContains(t, text, "skipCertManagerInstall")
	assert.NotContains(t, text, "isCertManagerAlreadyInstalled")
}
