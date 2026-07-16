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

	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("serves prometheus metrics over the lifecycle", func(t *testing.T) {
		t.Parallel()

		addr := freeAddr(t)

		var factory *metrics.Factory
		app := newTestApp(t, addr, fx.Populate(&factory))
		require.NoError(t, app.Err())

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		t.Cleanup(cancel)
		require.NoError(t, app.Start(ctx))

		// Emit a counter via the provided Factory so the scrape has something
		// proxy-specific to assert on. newTestApp supplies the "tmprl_proxy"
		// namespace, so the series is tmprl_proxy_answer_total.
		factory.NewCounter(goprom.CounterOpts{
			Name: "answer_total",
			Help: "smoke-test counter",
		}, nil).WithLabelValues().Inc()

		url := "http://" + addr + "/metrics"

		// OnStart launches ListenAndServe in a goroutine, so the listener may
		// not be bound the instant Start returns. Retry until it answers.
		var body string
		require.Eventually(t, func() bool {
			b, ok := scrape(url)
			if ok {
				body = b
			}
			return ok
		}, 5*time.Second, 20*time.Millisecond)

		require.Contains(t, body, "tmprl_proxy_answer_total")

		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
		t.Cleanup(cancel)
		require.NoError(t, app.Stop(ctx))

		// OnStop shuts the server down, so the endpoint should stop answering.
		// This proves the stop hook ran rather than just returning nil.
		require.Eventually(t, func() bool {
			_, ok := scrape(url)
			return !ok
		}, 5*time.Second, 20*time.Millisecond)
	})

	t.Run("shuts the app down when the listener fails", func(t *testing.T) {
		t.Parallel()

		// Hold the port so the server's ListenAndServe fails to bind, which
		// should drive the OnStart goroutine to shut the whole app down.
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = l.Close() })

		app := newTestApp(t, l.Addr().String())
		require.NoError(t, app.Err())

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		t.Cleanup(cancel)
		require.NoError(t, app.Start(ctx))

		select {
		case sig := <-app.Wait():
			require.Equal(t, 1, sig.ExitCode)
		case <-ctx.Done():
			t.Fatal("app did not shut down after listener failure")
		}
	})

	t.Run("requires the metrics address", func(t *testing.T) {
		t.Parallel()

		reg := goprom.NewRegistry()
		app := fx.New(
			fx.Supply(
				fx.Annotate(reg, fx.As(new(goprom.Registerer))),
				fx.Annotate(reg, fx.As(new(goprom.Gatherer))),
			),
			metrics.Module,
			fx.NopLogger,
		)
		require.Error(t, app.Err())
	})
}

func newTestApp(t *testing.T, addr string, opts ...fx.Option) *fx.App {
	t.Helper()

	reg := goprom.NewRegistry()

	base := []fx.Option{
		fx.Supply(
			fx.Annotate(addr, metrics.AddrTag),
			fx.Annotate("tmprl_proxy", metrics.NamespaceTag),
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
