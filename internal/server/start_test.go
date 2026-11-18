package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/server"
	"go.temporal.io/proxy/pkg/logger"
)

func TestStartResolvesPortZero(t *testing.T) {
	t.Parallel()

	svr := startServer(t, server.WithHostPort("127.0.0.1:0"))

	addr, ok := svr.Addr().(*net.TCPAddr)
	require.True(t, ok)
	require.NotZero(t, addr.Port, "Addr reports the port the kernel picked")

	conn, err := net.Dial("tcp", addr.String())
	require.NoError(t, err, "the server must be accepting once Start returns")
	require.NoError(t, conn.Close())
}

func TestStartRequiresAHostPort(t *testing.T) {
	t.Parallel()

	svr, err := server.New()
	require.NoError(t, err)

	require.ErrorContains(t, svr.Start(t.Context()), "WithHostPort")
	require.Nil(t, svr.Addr())
}

func TestStartFailsWhenThePortIsTaken(t *testing.T) {
	t.Parallel()

	first := startServer(t, server.WithHostPort("127.0.0.1:0"))

	second, err := server.New(server.WithHostPort(first.Addr().String()))
	require.NoError(t, err)

	require.ErrorContains(t, second.Start(t.Context()), "failed to create listener")
	require.Nil(t, second.Addr())
}

// TestStartServesPastItsContext shows the context given to Start bounds the bind
// only: cancelling it afterwards leaves the server accepting.
func TestStartServesPastItsContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	svr := startServerCtx(t, ctx, server.WithHostPort("127.0.0.1:0"))
	cancel()

	conn, err := net.Dial("tcp", svr.Addr().String())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

// TestAbortFiresWhenServingStopsUnexpectedly closes the listener out from under
// the serving goroutine, which is the only way into the unexpected-exit path:
// Stop is a clean shutdown, and a taken port fails before serving starts.
func TestAbortFiresWhenServingStopsUnexpectedly(t *testing.T) {
	t.Parallel()

	aborts := make(chan error, 1)
	svr := startServer(t,
		server.WithHostPort("127.0.0.1:0"),
		server.WithAbort(func(err error) { aborts <- err }),
	)

	require.NoError(t, svr.Listener().Close())

	select {
	case err := <-aborts:
		require.ErrorContains(t, err, "stopped serving")
	case <-time.After(10 * time.Second):
		t.Fatal("Abort was not called after serving stopped")
	}
}

func TestAbortIsOptional(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	svr := startServer(t, server.WithHostPort("127.0.0.1:0"), server.WithLogger(log))

	// Without an abort function the notification is dropped rather than
	// panicking; the log entry is then the only record.
	require.NoError(t, svr.Listener().Close())

	require.Eventually(t, func() bool {
		return log.Contains("Server stopped serving")
	}, 10*time.Second, 10*time.Millisecond)
}

func TestStopDoesNotAbort(t *testing.T) {
	t.Parallel()

	aborts := make(chan error, 1)
	svr, err := server.New(
		server.WithHostPort("127.0.0.1:0"),
		server.WithAbort(func(err error) { aborts <- err }),
	)
	require.NoError(t, err)
	require.NoError(t, svr.Start(t.Context()))

	addr := svr.Addr().String()
	require.NoError(t, svr.Stop(context.WithoutCancel(t.Context())))

	select {
	case err := <-aborts:
		t.Fatalf("Abort fired during a clean shutdown: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	conn, err := net.Dial("tcp", addr)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("the server is still accepting on %s", addr)
	}
}

func startServer(t *testing.T, opts ...server.Option) *server.Server {
	t.Helper()

	return startServerCtx(t, t.Context(), opts...)
}

func startServerCtx(t *testing.T, ctx context.Context, opts ...server.Option) *server.Server {
	t.Helper()

	svr, err := server.New(opts...)
	require.NoError(t, err)
	require.NoError(t, svr.Start(ctx))

	// t.Context is already cancelled by the time cleanups run, and a shutdown
	// must not inherit that.
	t.Cleanup(func() { _ = svr.Stop(context.WithoutCancel(t.Context())) })

	return svr
}
