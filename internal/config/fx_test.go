package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/services"
)

func TestModule_ProvidesConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("hostPort: :7233\n"+upstreamYAML), 0o600))

	var got *config.Config
	app := fx.New(
		fx.Supply(fx.Annotate(path, config.ConfigFileTag)),
		config.Module,
		fx.Populate(&got),
		fx.NopLogger,
	)

	require.NoError(t, app.Err())

	want := &config.Config{
		Listen:          config.ListenConfig{HostPort: ":7233"},
		Upstreams:       []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}},
		AllowedServices: config.Services(services.Default()),
		Metrics:         defaultMetrics(),
	}
	require.NoError(t, want.Prepare())
	require.Equal(t, want, got)
}

func TestModule_ProvidesAllowlist(t *testing.T) {
	t.Parallel()

	// The forwarding hops depend on this provider and nothing else supplies it, so
	// without it the binary fails at construction with "missing type:
	// services.Allowlist" while every other test still passes.
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("hostPort: :7233\n"+upstreamYAML+"allowedServices: [\""+services.Reflection+"\"]\n"), 0o600))

	var got services.Allowlist
	app := fx.New(
		fx.Supply(fx.Annotate(path, config.ConfigFileTag)),
		config.Module,
		fx.Populate(&got),
		fx.NopLogger,
	)

	require.NoError(t, app.Err())
	require.True(t, got.Allows(services.Reflection))
	require.True(t, got.Allows(services.ReflectionV1Alpha), "expected the compatibility alias to be admitted")
	require.False(t, got.Allows(services.WorkflowService), "expected a service the config omits to be denied")
}

func TestModule_AllowlistDefaultsWhenConfigNamesNone(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("hostPort: :7233\n"+upstreamYAML), 0o600))

	var got services.Allowlist
	app := fx.New(
		fx.Supply(fx.Annotate(path, config.ConfigFileTag)),
		config.Module,
		fx.Populate(&got),
		fx.NopLogger,
	)

	require.NoError(t, app.Err())
	for _, svc := range services.Default() {
		require.True(t, got.Allows(svc), "expected the default set to be admitted")
	}
}

func TestModule_ErrorPropagates(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")

	app := fx.New(
		fx.Supply(fx.Annotate(missing, config.ConfigFileTag)),
		config.Module,
		fx.Invoke(func(*config.Config) {}),
		fx.NopLogger,
	)

	require.ErrorIs(t, app.Err(), os.ErrNotExist)
}

func TestSupply(t *testing.T) {
	t.Parallel()

	t.Run("provides the config prepared", func(t *testing.T) {
		t.Parallel()

		cfg := &config.Config{
			Listen:    config.ListenConfig{HostPort: ":8080"},
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}},
		}

		var got *config.Config
		app := fx.New(fx.NopLogger, config.Supply(cfg), fx.Populate(&got))
		require.NoError(t, app.Err())
		require.Equal(t, ":9090", got.Metrics.HostPort)
	})

	t.Run("fails the graph on an invalid config", func(t *testing.T) {
		t.Parallel()

		app := fx.New(fx.NopLogger, config.Supply(&config.Config{}), fx.Invoke(func(*config.Config) {}))
		require.ErrorContains(t, app.Err(), "at least one upstream is required")
	})
}

func TestModuleRejectsInvalidFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "proxy.yaml")
	require.NoError(t, os.WriteFile(path, []byte("hostPort: \":8080\"\n"), 0o600))

	app := fx.New(
		fx.NopLogger,
		fx.Supply(fx.Annotate(path, config.ConfigFileTag)),
		config.Module,
		fx.Invoke(func(*config.Config) {}),
	)
	require.ErrorContains(t, app.Err(), "at least one upstream is required")
}
