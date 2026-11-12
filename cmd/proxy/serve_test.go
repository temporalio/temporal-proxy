package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestServeFailsOnMissingConfigFile proves the --config flag reaches the graph:
// an unreadable file is reported by path rather than blocking in Run.
func TestServeFailsOnMissingConfigFile(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "missing.yaml")
	err := serve().Run(t.Context(), []string{"serve", "--config", missing})
	require.ErrorContains(t, err, missing)
}
