package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestModule_ProvidesConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("hostPort: :7233\n"), 0o600))

	var got *config.Config
	app := fx.New(
		fx.Supply(fx.Annotate(path, config.ConfigFileTag)),
		config.Module,
		fx.Populate(&got),
		fx.NopLogger,
	)

	require.NoError(t, app.Err())
	require.Equal(t, &config.Config{Listen: config.ListenConfig{HostPort: ":7233"}}, got)
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
