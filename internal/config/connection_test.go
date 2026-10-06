package config_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestConnection_Defaults(t *testing.T) {
	t.Parallel()

	const upstream = "hostPort: 127.0.0.1:7233\nupstreams:\n  - name: local\n    hostPort: 127.0.0.1:7234\n"

	tests := []struct {
		name        string
		yaml        string
		wantSize    config.ByteSize
		wantTime    time.Duration
		wantTimeout time.Duration
		wantPool    int
	}{
		{
			name:        "absent block takes every default",
			yaml:        upstream,
			wantSize:    128 << 20,
			wantTime:    30 * time.Second,
			wantTimeout: 15 * time.Second,
			wantPool:    32,
		},
		{
			name:        "explicit values are preserved",
			yaml:        upstream + "    connection:\n      maxResponseSize: 64MiB\n      maxConnections: 4\n      keepAlive:\n        time: 10s\n        timeout: 5s\n",
			wantSize:    64 << 20,
			wantTime:    10 * time.Second,
			wantTimeout: 5 * time.Second,
			wantPool:    4,
		},
		{
			name:        "a bare integer size is bytes",
			yaml:        upstream + "    connection:\n      maxResponseSize: 8388608\n",
			wantSize:    8 << 20,
			wantTime:    30 * time.Second,
			wantTimeout: 15 * time.Second,
			wantPool:    32,
		},
		{
			name:        "each keepalive duration defaults on its own",
			yaml:        upstream + "    connection:\n      keepAlive:\n        time: 1m\n",
			wantSize:    128 << 20,
			wantTime:    time.Minute,
			wantTimeout: 15 * time.Second,
			wantPool:    32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := config.Load(strings.NewReader(tt.yaml))
			require.NoError(t, err)

			conn := cfg.Upstreams[0].Connection
			require.Equal(t, tt.wantSize, conn.ResponseLimit())
			require.Equal(t, tt.wantTime, conn.KeepAlive.PingTime())
			require.Equal(t, tt.wantTimeout, conn.KeepAlive.PingTimeout())
			require.Equal(t, tt.wantPool, conn.PoolSize(cfg.Upstreams[0].IsTemplated()))
		})
	}
}

func TestConnection_PoolSize(t *testing.T) {
	t.Parallel()

	const (
		static    = "hostPort: 127.0.0.1:7233\nupstreams:\n  - name: up\n    hostPort: 127.0.0.1:7234\n"
		srv       = "hostPort: 127.0.0.1:7233\nupstreams:\n  - name: up\n    hostPort: srv:///_grpc._tcp.temporal.example\n"
		templated = "hostPort: 127.0.0.1:7233\nupstreams:\n  - name: up\n    hostPort: \"{{ .RemoteNamespace }}.example:7233\"\n"
		explicit  = "    connection:\n      maxConnections: 8\n"
	)

	tests := []struct {
		name     string
		yaml     string
		wantPool int
	}{
		{name: "a static upstream defaults to 32", yaml: static, wantPool: 32},
		{name: "an SRV upstream defaults to 32", yaml: srv, wantPool: 32},
		{name: "a templated upstream defaults to 4", yaml: templated, wantPool: 4},
		{name: "an explicit count overrides the static default", yaml: static + explicit, wantPool: 8},
		{name: "an explicit count overrides the templated default", yaml: templated + explicit, wantPool: 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := config.Load(strings.NewReader(tt.yaml))
			require.NoError(t, err)

			up := cfg.Upstreams[0]
			require.Equal(t, tt.wantPool, up.Connection.PoolSize(up.IsTemplated()))
		})
	}
}

func TestConnection_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      config.ConnectionConfig
		wantErrs []validation.Error
	}{
		{
			name: "a zero block is valid, since every field defaults",
			cfg:  config.ConnectionConfig{},
		},
		{
			name: "the largest size gRPC can represent is valid",
			cfg:  config.ConnectionConfig{MaxResponseSize: 2<<30 - 1},
		},
		{
			name:     "a size of 2GiB or more is rejected",
			cfg:      config.ConnectionConfig{MaxResponseSize: 2 << 30},
			wantErrs: []validation.Error{{Field: "maxResponseSize", Message: "not less than 2GiB"}},
		},
		{
			name:     "a negative size is rejected",
			cfg:      config.ConnectionConfig{MaxResponseSize: -1},
			wantErrs: []validation.Error{{Field: "maxResponseSize", Message: "not greater than 0B"}},
		},
		{
			name: "the largest pool is valid",
			cfg:  config.ConnectionConfig{MaxConnections: 64},
		},
		{
			name:     "a pool over 64 connections is rejected",
			cfg:      config.ConnectionConfig{MaxConnections: 65},
			wantErrs: []validation.Error{{Field: "maxConnections", Message: "not less than 65"}},
		},
		{
			name:     "a negative pool is rejected",
			cfg:      config.ConnectionConfig{MaxConnections: -1},
			wantErrs: []validation.Error{{Field: "maxConnections", Message: "not greater than 0"}},
		},
		{
			name: "the gRPC minimum ping time is valid",
			cfg:  config.ConnectionConfig{KeepAlive: config.KeepAliveConfig{Time: 10 * time.Second}},
		},
		{
			name: "a ping time under the gRPC minimum is rejected",
			cfg:  config.ConnectionConfig{KeepAlive: config.KeepAliveConfig{Time: 5 * time.Second}},
			wantErrs: []validation.Error{
				{Subject: "keepAlive", Field: "time", Message: "not greater than or equal to 10s"},
			},
		},
		{
			name: "a negative ping timeout is rejected",
			cfg:  config.ConnectionConfig{KeepAlive: config.KeepAliveConfig{Timeout: -time.Second}},
			wantErrs: []validation.Error{
				{Subject: "keepAlive", Field: "timeout", Message: "not greater than 0s"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.wantErrs) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)
			require.ElementsMatch(t, tt.wantErrs, []validation.Error(errs))
		})
	}
}
