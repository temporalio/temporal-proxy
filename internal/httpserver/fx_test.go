package httpserver_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/httpserver"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

func TestModuleMountsEveryGroupsRoutes(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		HTTP: config.HTTP{Listen: config.ListenConfig{HostPort: "127.0.0.1:0", Insecure: true}},
	}

	var svr *httpserver.Server

	app := fx.New(
		fx.Supply(cfg),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		fx.Provide(httpserver.AsRoutes(func() []httpserver.Route {
			return []httpserver.Route{{Pattern: "/", Handler: body("root")}}
		})),
		fx.Provide(httpserver.AsRoutes(func() ([]httpserver.Route, error) {
			return []httpserver.Route{{Pattern: "/other/", Handler: body("other")}}, nil
		})),
		httpserver.Module,
		fx.Populate(&svr),
		fx.NopLogger,
	)
	require.NoError(t, app.Err())
	require.NoError(t, app.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, app.Stop(context.Background())) })

	base := "http://" + svr.Addr().String()
	require.Equal(t, "root", get(t, base+"/decode"))
	require.Equal(t, "other", get(t, base+"/other/thing"))
}

func TestModuleIsInertWithNoRoutes(t *testing.T) {
	t.Parallel()

	// With no group enabled nothing may bind, so the module must produce no
	// Server at all rather than one that happens not to have started. Starting
	// the app also exercises the lifecycle hook itself: with no Server, it must
	// never be appended, so there is nothing for OnStart to call on a nil
	// receiver.
	var svr *httpserver.Server

	app := fx.New(
		fx.Supply(&config.Config{}),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		fx.Provide(httpserver.AsRoutes(func() []httpserver.Route { return nil })),
		httpserver.Module,
		fx.Populate(&svr),
		fx.NopLogger,
	)
	require.NoError(t, app.Err())
	require.Nil(t, svr)

	require.NoError(t, app.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, app.Stop(context.Background())) })
}

func body(s string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, s) })
}

func get(t *testing.T, url string) string {
	t.Helper()

	res, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()

	require.Equal(t, http.StatusOK, res.StatusCode)

	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	return string(b)
}
