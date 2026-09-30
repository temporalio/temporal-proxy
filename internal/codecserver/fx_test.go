package codecserver_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/codecserver"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// Real time and a real listener rather than testing/synctest: the bubble's
// clock only advances once every goroutine in it is durably blocked, and a
// served connection sits on real network reads, which never qualify. The
// server's timeouts are not asserted here for the same reason.
func TestServerStartsAndReportsItsAddress(t *testing.T) {
	t.Parallel()

	svr := codecserver.NewServer(
		"127.0.0.1:0",
		codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, testReporter(t)),
		nil,
		logger.NewNoopLogger(),
		nil,
	)

	require.Nil(t, svr.Addr())
	require.NoError(t, svr.Start(t.Context()))
	require.NotNil(t, svr.Addr())

	// t.Context() is already cancelled by the time cleanups run, so the drain
	// needs its own context.
	t.Cleanup(func() { require.NoError(t, svr.Stop(context.Background())) })

	res, err := http.Post(
		"http://"+svr.Addr().String()+"/decode",
		"application/json",
		strings.NewReader(`{"payloads":[]}`),
	)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()

	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestModuleIsInertWhenDisabled(t *testing.T) {
	t.Parallel()

	// A disabled block must not bind anything, so the module must produce no
	// Server at all rather than one that happens not to have started. Starting
	// the app also exercises the lifecycle hook itself: with no Server, it must
	// never be appended, so there is nothing for OnStart to call on a nil
	// receiver.
	cfg := &config.Config{}
	require.False(t, cfg.CodecServer.Enabled)

	var svr *codecserver.Server

	app := fx.New(
		fx.Supply(cfg),
		fx.Provide(func() (*proxy.Codecs, error) { return proxy.NewCodecs(proxy.CodecOptions{}) }),
		fx.Provide(func() api.Connections { return api.Connections{} }),
		fx.Provide(func() *metrics.Factory {
			return metrics.New("test", promauto.With(prometheus.NewRegistry()))
		}),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		codecserver.Module,
		fx.Populate(&svr),
	)
	require.NoError(t, app.Err())
	require.Nil(t, svr)

	require.NoError(t, app.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, app.Stop(context.Background())) })
}
