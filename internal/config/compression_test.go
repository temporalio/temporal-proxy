package config_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/config"
)

func TestCompressionValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     config.Compression
		wantErr string // substring; "" means valid
	}{
		{
			name: "absent block",
			cfg:  config.Compression{},
		},
		{
			name: "enabled with an algorithm",
			cfg:  config.Compression{Enabled: true, Default: &config.CompressionPolicy{Algorithm: config.CompressionZstd}},
		},
		{
			name:    "enabled without default",
			cfg:     config.Compression{Enabled: true},
			wantErr: "default: is required",
		},
		{
			name:    "enabled without an algorithm",
			cfg:     config.Compression{Enabled: true, Default: &config.CompressionPolicy{}},
			wantErr: "default: algorithm: required when compression is enabled",
		},
		{
			name:    "unknown algorithm, even when disabled",
			cfg:     config.Compression{Default: &config.CompressionPolicy{Algorithm: "gzip"}},
			wantErr: `default: algorithm: unknown algorithm "gzip", expected one of lz4, snappy, zstd`,
		},
		{
			name: "level and minBytes at their bounds",
			cfg:  config.Compression{Default: &config.CompressionPolicy{Level: new(22), MinBytes: new(0)}},
		},
		{
			name:    "level below range",
			cfg:     config.Compression{Default: &config.CompressionPolicy{Level: new(0)}},
			wantErr: "default: level: must be 1-22",
		},
		{
			name:    "level above range",
			cfg:     config.Compression{Default: &config.CompressionPolicy{Level: new(23)}},
			wantErr: "default: level: must be 1-22",
		},
		{
			name:    "negative minBytes",
			cfg:     config.Compression{Default: &config.CompressionPolicy{MinBytes: new(-1)}},
			wantErr: "default: minBytes",
		},
		{
			// The top-level flag is what default's namespaces follow, so a second
			// switch here could only disagree with it.
			name:    "enabled inside default",
			cfg:     config.Compression{Default: &config.CompressionPolicy{Enabled: new(true)}},
			wantErr: "default: enabled: not allowed here; set compression.enabled instead",
		},
		{
			name: "override inherits the default algorithm",
			cfg: config.Compression{
				Enabled:   true,
				Default:   &config.CompressionPolicy{Algorithm: config.CompressionZstd},
				Overrides: map[string]config.CompressionPolicy{"staging": {MinBytes: new(512)}},
			},
		},
		{
			name: "override opts one namespace in while compression is off",
			cfg: config.Compression{
				Overrides: map[string]config.CompressionPolicy{
					"staging": {Enabled: new(true), Algorithm: config.CompressionLZ4},
				},
			},
		},
		{
			name: "override opts in and inherits a default algorithm",
			cfg: config.Compression{
				Default:   &config.CompressionPolicy{Algorithm: config.CompressionSnappy},
				Overrides: map[string]config.CompressionPolicy{"staging": {Enabled: new(true)}},
			},
		},
		{
			name: "override opts in with no algorithm anywhere",
			cfg: config.Compression{
				Overrides: map[string]config.CompressionPolicy{"staging": {Enabled: new(true)}},
			},
			wantErr: "overrides[staging]: algorithm: required when compression is enabled; set it here or in default",
		},
		{
			name: "override opts out without an algorithm",
			cfg: config.Compression{
				Enabled:   true,
				Default:   &config.CompressionPolicy{Algorithm: config.CompressionZstd},
				Overrides: map[string]config.CompressionPolicy{"precompressed": {Enabled: new(false)}},
			},
		},
		{
			name: "invalid override",
			cfg: config.Compression{
				Overrides: map[string]config.CompressionPolicy{"staging": {Level: new(0)}},
			},
			wantErr: "overrides[staging]: level: must be 1-22",
		},
		{
			name: "blank override namespace",
			cfg: config.Compression{
				Overrides: map[string]config.CompressionPolicy{"": {}},
			},
			wantErr: "overrides[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestCompressionSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  config.Compression
		ns   string
		want config.CompressionSettings
	}{
		{
			name: "absent block",
			ns:   "orders",
			want: config.CompressionSettings{Level: 3, MinBytes: 1024},
		},
		{
			name: "default fills in level and minBytes",
			cfg:  config.Compression{Enabled: true, Default: &config.CompressionPolicy{Algorithm: config.CompressionZstd}},
			ns:   "orders",
			want: config.CompressionSettings{Enabled: true, Algorithm: config.CompressionZstd, Level: 3, MinBytes: 1024},
		},
		{
			name: "namespace without an override uses default",
			cfg: config.Compression{
				Enabled:   true,
				Default:   &config.CompressionPolicy{Algorithm: config.CompressionZstd, Level: new(9), MinBytes: new(512)},
				Overrides: map[string]config.CompressionPolicy{"staging": {Algorithm: config.CompressionLZ4}},
			},
			ns:   "orders",
			want: config.CompressionSettings{Enabled: true, Algorithm: config.CompressionZstd, Level: 9, MinBytes: 512},
		},
		{
			name: "override inherits what it leaves unset",
			cfg: config.Compression{
				Enabled:   true,
				Default:   &config.CompressionPolicy{Algorithm: config.CompressionZstd, Level: new(9), MinBytes: new(512)},
				Overrides: map[string]config.CompressionPolicy{"staging": {Algorithm: config.CompressionLZ4}},
			},
			ns:   "staging",
			want: config.CompressionSettings{Enabled: true, Algorithm: config.CompressionLZ4, Level: 9, MinBytes: 512},
		},
		{
			// An explicit zero is a setting, not an omission.
			name: "override sets minBytes to zero",
			cfg: config.Compression{
				Enabled:   true,
				Default:   &config.CompressionPolicy{Algorithm: config.CompressionZstd, MinBytes: new(512)},
				Overrides: map[string]config.CompressionPolicy{"staging": {MinBytes: new(0)}},
			},
			ns:   "staging",
			want: config.CompressionSettings{Enabled: true, Algorithm: config.CompressionZstd, Level: 3, MinBytes: 0},
		},
		{
			name: "override opts out",
			cfg: config.Compression{
				Enabled:   true,
				Default:   &config.CompressionPolicy{Algorithm: config.CompressionZstd},
				Overrides: map[string]config.CompressionPolicy{"precompressed": {Enabled: new(false)}},
			},
			ns:   "precompressed",
			want: config.CompressionSettings{Algorithm: config.CompressionZstd, Level: 3, MinBytes: 1024},
		},
		{
			name: "override opts in while compression is off",
			cfg: config.Compression{
				Overrides: map[string]config.CompressionPolicy{
					"staging": {Enabled: new(true), Algorithm: config.CompressionSnappy},
				},
			},
			ns:   "staging",
			want: config.CompressionSettings{Enabled: true, Algorithm: config.CompressionSnappy, Level: 3, MinBytes: 1024},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := minimalConfig()
			cfg.Compression = tt.cfg
			require.NoError(t, cfg.Prepare())
			require.Equal(t, tt.want, cfg.Compression.Settings(tt.ns))
		})
	}
}

// TestLoadCompression reads the RFC's example block. Misspelling a yaml tag, so
// a key is silently dropped, fails it.
func TestLoadCompression(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(strings.NewReader("hostPort: :8080\n" + upstreamYAML + `
compression:
  enabled: true
  default:
    algorithm: zstd
    level: 3
    minBytes: 256
  overrides:
    staging:
      algorithm: lz4
    archive:
      level: 9
    precompressed:
      enabled: false
`))
	require.NoError(t, err)

	c := &cfg.Compression
	require.Equal(t,
		config.CompressionSettings{Enabled: true, Algorithm: config.CompressionZstd, Level: 3, MinBytes: 256},
		c.Settings("orders"))
	require.Equal(t,
		config.CompressionSettings{Enabled: true, Algorithm: config.CompressionLZ4, Level: 3, MinBytes: 256},
		c.Settings("staging"))
	require.Equal(t,
		config.CompressionSettings{Enabled: true, Algorithm: config.CompressionZstd, Level: 9, MinBytes: 256},
		c.Settings("archive"))
	require.False(t, c.Settings("precompressed").Enabled)
}

func TestLoadRejectsInvalidCompression(t *testing.T) {
	t.Parallel()

	_, err := config.Load(strings.NewReader("hostPort: :8080\n" + upstreamYAML + `
compression:
  enabled: true
`))
	require.ErrorContains(t, err, "compression: default: is required")
}

func TestCompressionSettingsPanicsBeforePrepare(t *testing.T) {
	t.Parallel()

	c := &config.Compression{Enabled: true, Default: &config.CompressionPolicy{Algorithm: config.CompressionZstd}}
	require.Panics(t, func() { c.Settings("orders") })
}

// TestCompressionSettingsRecompile shows Prepare picks up a config changed
// after an earlier Prepare. Keeping compiled settings across calls, by skipping
// the reset and compiling only when nothing is cached, fails it.
func TestCompressionSettingsRecompile(t *testing.T) {
	t.Parallel()

	cfg := minimalConfig()
	cfg.Compression = config.Compression{Enabled: true, Default: &config.CompressionPolicy{Algorithm: config.CompressionZstd}}
	require.NoError(t, cfg.Prepare())

	cfg.Compression.Default.Algorithm = config.CompressionLZ4
	require.NoError(t, cfg.Prepare())
	require.Equal(t, config.CompressionLZ4, cfg.Compression.Settings("orders").Algorithm)
}

func TestCompressionSettingsClearedByFailedPrepare(t *testing.T) {
	t.Parallel()

	cfg := minimalConfig()
	require.NoError(t, cfg.Prepare())

	cfg.Listen.HostPort = "not a host port"
	require.Error(t, cfg.Prepare())
	require.Panics(t, func() { cfg.Compression.Settings("orders") })
}

// minimalConfig returns the smallest config that prepares.
func minimalConfig() *config.Config {
	return &config.Config{
		Listen: config.ListenConfig{HostPort: ":8080"},
		Upstreams: []config.Upstream{{
			Name:   "primary",
			Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"},
		}},
	}
}
