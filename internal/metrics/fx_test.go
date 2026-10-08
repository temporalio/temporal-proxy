package metrics_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	goprom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("serves prometheus metrics over the lifecycle", func(t *testing.T) {
		t.Parallel()

		var (
			factory *metrics.Factory
			svr     *metrics.Server
		)
		app := newTestApp(t, "127.0.0.1:0", fx.Populate(&factory, &svr))
		require.NoError(t, app.Err())

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		t.Cleanup(cancel)
		require.NoError(t, app.Start(ctx))

		factory.NewCounter(goprom.CounterOpts{
			Name: "answer_total",
			Help: "smoke-test counter",
		}, nil).WithLabelValues().Inc()

		// Start binds before it returns, so the address is live immediately.
		url := "http://" + svr.Addr().String() + "/metrics"
		body, ok := scrape(url)
		require.True(t, ok)
		require.Contains(t, body, "tmprl_proxy_answer_total")

		require.NoError(t, app.Stop(ctx))

		_, ok = scrape(url)
		require.False(t, ok, "the stop hook must close the listener")
	})

	t.Run("fails to start when the port is taken", func(t *testing.T) {
		t.Parallel()

		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = l.Close() })

		app := newTestApp(t, l.Addr().String())
		require.NoError(t, app.Err())

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		t.Cleanup(cancel)
		require.Error(t, app.Start(ctx))
	})

	t.Run("requires the metrics address", func(t *testing.T) {
		t.Parallel()

		// Every other dependency is supplied, so the empty hostPort is the only
		// thing that can fail the app, and the message proves it was the guard
		// rather than an unsatisfied constructor.
		app := newTestApp(t, "")
		require.ErrorContains(t, app.Err(), "metrics addr not set")
	})
}

func newTestApp(t *testing.T, addr string, opts ...fx.Option) *fx.App {
	t.Helper()

	reg := goprom.NewRegistry()

	base := []fx.Option{
		fx.Supply(
			&config.Config{Metrics: config.Metrics{HostPort: addr, Namespace: "tmprl_proxy"}},
			fx.Annotate(reg, fx.As(new(goprom.Registerer))),
			fx.Annotate(reg, fx.As(new(goprom.Gatherer))),
			fx.Annotate(logger.NewNoopLogger(), fx.As(new(logger.Logger))),
		),
		metrics.Module,
		fx.NopLogger,
	}

	return fx.New(append(base, opts...)...)
}

func scrape(url string) (string, bool) {
	resp, err := http.Get(url) //nolint:noctx // short-lived test scrape
	if err != nil {
		return "", false
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", false
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false
	}

	return string(b), true
}

func freeAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}
