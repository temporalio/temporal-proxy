package testutil_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

func TestWriteFile(t *testing.T) {
	t.Parallel()

	t.Run("writes contents and returns full path", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := testutil.WriteFile(t, dir, "hello.txt", []byte("world"))

		require.Equal(t, filepath.Join(dir, "hello.txt"), path)

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, []byte("world"), got)
	})

	t.Run("file is created with 0600 perms", func(t *testing.T) {
		t.Parallel()

		path := testutil.WriteFile(t, t.TempDir(), "secret", []byte("x"))

		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	})

	t.Run("empty contents are allowed", func(t *testing.T) {
		t.Parallel()

		path := testutil.WriteFile(t, t.TempDir(), "empty", nil)

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Empty(t, got)
	})
}
