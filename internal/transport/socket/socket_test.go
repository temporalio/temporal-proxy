package socket_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/transport/socket"
)

func TestUnixPath(t *testing.T) {
	t.Parallel()

	t.Run("is deterministic for the same host:port", func(t *testing.T) {
		t.Parallel()
		a, err := socket.UnixPath("localhost:7233")
		require.NoError(t, err)
		b, err := socket.UnixPath("localhost:7233")
		require.NoError(t, err)
		require.Equal(t, a, b)
	})

	t.Run("differs for different host:port", func(t *testing.T) {
		t.Parallel()
		a, err := socket.UnixPath("localhost:7233")
		require.NoError(t, err)
		b, err := socket.UnixPath("localhost:7234")
		require.NoError(t, err)
		require.NotEqual(t, a, b)
	})

	t.Run("is an absolute path under TempDir ending in .sock", func(t *testing.T) {
		t.Parallel()

		got, err := socket.UnixPath("dns:///cloud.example.com:443")
		require.NoError(t, err)
		require.True(t, filepath.IsAbs(got), "expected absolute path, got %q", got)
		require.Equal(t, filepath.Clean(os.TempDir()), filepath.Dir(got))
		require.True(t, strings.HasSuffix(got, ".sock"), "got %q", got)

		// The host:port separators are sanitized in the filename.
		require.NotContains(t, filepath.Base(got), ":")
	})
}

func TestValidatePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{name: "short path", path: "/tmp/proxy.sock"},
		{name: "at the limit", path: "/" + strings.Repeat("d", 102)},
		{name: "one byte over the limit", path: "/" + strings.Repeat("d", 103), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := socket.ValidatePath(tt.path)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, "exceeds limit")
		})
	}
}

func TestUnixPathRejectsOverlongPath(t *testing.T) {
	// Not parallel: mutates TMPDIR, which os.TempDir reads.
	t.Setenv("TMPDIR", "/"+strings.Repeat("d", 200))

	_, err := socket.UnixPath("localhost:7233")
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds limit")
}
