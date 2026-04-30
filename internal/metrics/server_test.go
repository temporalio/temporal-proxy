package metrics_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

func TestServer_ServesMetricsEndpoint(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
	h := p.RegisterGauge("srv_test_gauge", "help", "label")
	h.Set(42, "label", "foo")

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svr := metrics.NewServer(metrics.Gatherer(reg))
	svr.Start(lis)
	defer func() { require.NoError(t, svr.Stop(context.Background())) }()

	resp, err := http.Get("http://" + lis.Addr().String() + "/metrics")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "srv_test_gauge")
	require.Contains(t, string(body), "42")
}

func TestServer_Stop(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
	_ = p // registered against reg; gather via reg

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svr := metrics.NewServer(metrics.Gatherer(reg))
	svr.Start(lis)

	require.NoError(t, svr.Stop(context.Background()))

	// After stop, connections should be refused
	_, err = http.Get("http://" + lis.Addr().String() + "/metrics")
	require.Error(t, err)
}
