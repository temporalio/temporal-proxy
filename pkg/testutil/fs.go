package testutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// WriteFile writes contents to dir/name with mode 0600 and returns the full
// path. Any IO failure fails the test immediately. Callers typically pass
// [testing.T.TempDir] as dir so cleanup is automatic.
func WriteFile(t *testing.T, dir, name string, contents []byte) string {
	t.Helper()

	path := filepath.Join(dir, name)
	err := os.WriteFile(path, contents, 0o600)
	require.NoError(t, err)

	return path
}
