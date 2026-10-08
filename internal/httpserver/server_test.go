package httpserver_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/httpserver"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// Real time and a real listener rather than testing/synctest: the bubble's
// clock only advances once every goroutine in it is durably blocked, and a
// served connection sits on real network reads, which never qualify. The
// server's timeouts are not asserted here for the same reason.
func TestServerStartsAndReportsItsAddress(t *testing.T) {
	t.Parallel()

	svr := httpserver.NewServer(
		"127.0.0.1:0",
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }),
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

	res, err := http.Get("http://" + svr.Addr().String() + "/")
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()

	require.Equal(t, http.StatusTeapot, res.StatusCode)
}
