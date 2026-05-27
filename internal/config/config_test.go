package config_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
)

type errReader struct{ err error }

func TestLoad(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		yaml    string
		want    *config.Config
		wantErr bool
	}{
		{
			name: "valid config",
			yaml: "hostPort: :8080\n",
			want: &config.Config{Listen: config.ListenConfig{HostPort: ":8080"}},
		},
		{
			name:    "invalid YAML",
			yaml:    "{unclosed",
			wantErr: true,
		},
		{
			name: "empty hostPort",
			yaml: "hostPort: \"\"\n",
			want: &config.Config{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.Load(strings.NewReader(tc.yaml))
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestLoad_EnvVarExpansion is not parallel because t.Setenv cannot be used in parallel tests.
func TestLoad_EnvVarExpansion(t *testing.T) {
	t.Run("set var is substituted", func(t *testing.T) {
		t.Setenv("CONFIG_TEST_HOST_PORT", ":9090")

		got, err := config.Load(strings.NewReader("hostPort: ${CONFIG_TEST_HOST_PORT}\n"))
		require.NoError(t, err)
		require.Equal(t, ":9090", got.Listen.HostPort)
	})

	t.Run("unset var becomes empty string", func(t *testing.T) {
		require.NoError(t, os.Unsetenv("CONFIG_TEST_UNSET_VAR"))

		got, err := config.Load(strings.NewReader("hostPort: ${CONFIG_TEST_UNSET_VAR}\n"))
		require.NoError(t, err)
		require.Equal(t, "", got.Listen.HostPort)
	})
}

func TestLoad_ReadError(t *testing.T) {
	t.Parallel()

	_, err := config.Load(&errReader{err: errors.New("boom")})
	require.ErrorContains(t, err, "boom")
}

func TestLoadFile(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		content string
		want    *config.Config
		wantErr bool
	}{
		{
			name:    "valid file",
			content: "hostPort: :7233\n",
			want:    &config.Config{Listen: config.ListenConfig{HostPort: ":7233"}},
		},
		{
			name:    "invalid YAML in file",
			content: "{unclosed",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0o600))

			got, err := config.LoadFile(path)
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestLoadFile_MissingFile(t *testing.T) {
	t.Parallel()

	_, err := config.LoadFile(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func (e *errReader) Read(_ []byte) (int, error) { return 0, e.err }
