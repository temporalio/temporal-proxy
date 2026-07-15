package config_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/validation"
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

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	validUpstreams := []config.Upstream{{
		Name:   "primary",
		Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"},
	}}

	tests := []struct {
		name       string
		cfg        *config.Config
		wantTuples [][2]string // (Subject, Field); empty slice means no error expected
	}{
		{
			name: "valid hostPort, no TLS",
			cfg: &config.Config{
				Listen:    config.ListenConfig{HostPort: ":8080"},
				Upstreams: validUpstreams,
			},
		},
		{
			name: "invalid hostPort surfaces from ListenConfig",
			cfg: &config.Config{
				Listen:    config.ListenConfig{HostPort: "localhost"},
				Upstreams: validUpstreams,
			},
			wantTuples: [][2]string{{"", "hostPort"}},
		},
		{
			name: "broken TLS surfaces with tls subject stamped by parent",
			cfg: &config.Config{
				Listen: config.ListenConfig{
					HostPort: ":8080",
					TLS:      &config.TLSConfig{}, // empty -> creds.TLS PEM read failures
				},
				Upstreams: validUpstreams,
			},
			wantTuples: [][2]string{
				{"tls", "cert"},
				{"tls", "key"},
			},
		},
		{
			name: "hostPort and TLS failures aggregate",
			cfg: &config.Config{
				Listen: config.ListenConfig{
					HostPort: "localhost",
					TLS:      &config.TLSConfig{},
				},
				Upstreams: validUpstreams,
			},
			wantTuples: [][2]string{
				{"", "hostPort"},
				{"tls", "cert"},
				{"tls", "key"},
			},
		},
		{
			name: "no upstreams surfaces on the upstreams field",
			cfg: &config.Config{
				Listen: config.ListenConfig{HostPort: ":8080"},
			},
			wantTuples: [][2]string{{"", "upstreams"}},
		},
		{
			name: "missing upstream hostPort surfaces with indexed upstream subject",
			cfg: &config.Config{
				Listen: config.ListenConfig{HostPort: ":8080"},
				Upstreams: []config.Upstream{{
					Name: "primary",
				}},
			},
			wantTuples: [][2]string{{"upstreams[0]", "hostPort"}},
		},
		{
			name: "empty upstream name surfaces with indexed upstream subject",
			cfg: &config.Config{
				Listen: config.ListenConfig{HostPort: ":8080"},
				Upstreams: []config.Upstream{{
					Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"},
				}},
			},
			wantTuples: [][2]string{{"upstreams[0]", "name"}},
		},
		{
			name: "duplicate upstream names surface on the upstreams[name] field",
			cfg: &config.Config{
				Listen: config.ListenConfig{HostPort: ":8080"},
				Upstreams: []config.Upstream{
					{Name: "dup", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
					{Name: "dup", Listen: config.ListenConfig{HostPort: "127.0.0.1:7234"}},
				},
			},
			wantTuples: [][2]string{{"", "upstreams[name]"}},
		},
		{
			name: "templated upstream hostPort is accepted",
			cfg: &config.Config{
				Listen: config.ListenConfig{HostPort: ":8080"},
				Upstreams: []config.Upstream{{
					Name:   "templated",
					Listen: config.ListenConfig{HostPort: "{{ .RemoteNamespace }}.acme-cloud.tmprl.cloud:7233"},
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.wantTuples) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)

			got := make([][2]string, len(errs))
			for i, e := range errs {
				got[i] = [2]string{e.Subject, e.Field}
			}
			require.ElementsMatch(t, tt.wantTuples, got)
		})
	}
}

func TestConfig_Validate_RoutingReferences(t *testing.T) {
	t.Parallel()

	base := func(r config.Routing) *config.Config {
		return &config.Config{
			Listen:  config.ListenConfig{HostPort: ":8080"},
			Routing: r,
			Upstreams: []config.Upstream{
				{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
				{Name: "system", Listen: config.ListenConfig{HostPort: "127.0.0.1:7234"}},
			},
		}
	}

	tests := []struct {
		name       string
		cfg        *config.Config
		wantTuples [][2]string
	}{
		{
			name: "default and system reference known upstreams",
			cfg:  base(config.Routing{DefaultUpstream: "primary", SystemUpstream: "system"}),
		},
		{
			name:       "unknown default upstream",
			cfg:        base(config.Routing{DefaultUpstream: "missing"}),
			wantTuples: [][2]string{{"routing", "default"}},
		},
		{
			name:       "unknown system upstream",
			cfg:        base(config.Routing{SystemUpstream: "missing"}),
			wantTuples: [][2]string{{"routing", "system"}},
		},
		{
			name: "rule references known upstream",
			cfg: base(config.Routing{Rules: []config.RoutingRule{
				{Upstream: "primary", Match: config.RoutingMatch{Namespace: "payments"}},
			}}),
		},
		{
			name: "rule references unknown upstream",
			cfg: base(config.Routing{Rules: []config.RoutingRule{
				{Upstream: "missing", Match: config.RoutingMatch{Namespace: "payments"}},
			}}),
			wantTuples: [][2]string{{"routing.rules[0]", "upstream"}},
		},
		{
			name: "rule with empty upstream is required, not a bad reference",
			cfg: base(config.Routing{Rules: []config.RoutingRule{
				{Match: config.RoutingMatch{Namespace: "payments"}},
			}}),
			wantTuples: [][2]string{{"routing.rules[0]", "upstream"}},
		},
		{
			name: "unknown default, system, and rule aggregate",
			cfg: base(config.Routing{
				DefaultUpstream: "missing",
				SystemUpstream:  "gone",
				Rules: []config.RoutingRule{
					{Upstream: "nope", Match: config.RoutingMatch{Namespace: "payments"}},
				},
			}),
			wantTuples: [][2]string{
				{"routing", "default"},
				{"routing", "system"},
				{"routing.rules[0]", "upstream"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertTuples(t, tt.cfg.Validate(), tt.wantTuples)
		})
	}
}

func TestConfig_PrimaryUpstream(t *testing.T) {
	t.Parallel()

	t.Run("returns the first configured upstream", func(t *testing.T) {
		t.Parallel()

		cfg := &config.Config{Upstreams: []config.Upstream{
			{Name: "first", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
			{Name: "second", Listen: config.ListenConfig{HostPort: "127.0.0.1:7234"}},
		}}

		up, err := cfg.PrimaryUpstream()
		require.NoError(t, err)
		require.Equal(t, "first", up.Name)
	})

	t.Run("errors when no upstreams are configured", func(t *testing.T) {
		t.Parallel()

		_, err := (&config.Config{}).PrimaryUpstream()
		require.Error(t, err)
	})
}

func TestConfig_ValidateRejectsDuplicateHostPorts(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		Listen: config.ListenConfig{HostPort: "127.0.0.1:8443"},
		Upstreams: []config.Upstream{
			{Name: "a", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
			{Name: "b", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	require.ErrorContains(t, err, "upstreams[hostPort]")
}

func TestUpstream_IsTemplated(t *testing.T) {
	t.Parallel()

	require.True(t, (&config.Upstream{Listen: config.ListenConfig{HostPort: "{{ .LocalNamespace }}.acme.cloud:7233"}}).IsTemplated())
	require.False(t, (&config.Upstream{Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}).IsTemplated())
}

func (e *errReader) Read(_ []byte) (int, error) { return 0, e.err }
