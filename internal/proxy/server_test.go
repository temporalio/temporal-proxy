package proxy_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("returns a server with default options", func(t *testing.T) {
		t.Parallel()

		svr, err := proxy.New("127.0.0.1:7233", upstreamConn(t, "127.0.0.1:7233"))
		require.NoError(t, err)
		require.NotNil(t, svr)
	})
}

func TestServerStartAndStop(t *testing.T) {
	t.Parallel()

	// A unique upstream host gives this test its own socket path so it can run in
	// parallel with the others. The upstream is never dialed: the health service
	// the proxy serves locally answers the Check below.
	const upstream = "127.0.0.1:17233"

	log := logger.NewTestLogger()
	svr, err := proxy.New(upstream, upstreamConn(t, upstream), proxy.WithLogger(log))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lis, err := svr.Listen(ctx)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(ctx, lis) }()

	conn := dialUnix(t, upstream)
	defer func() { _ = conn.Close() }()

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(
		t.Context(),
		&grpc_health_v1.HealthCheckRequest{},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

	// The supplied logger reaches the underlying server.
	require.True(t, log.Contains("Starting the server"), "expected the injected logger to be used")

	require.NoError(t, svr.Stop(t.Context()))

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("server did not stop after shutdown")
	}
}

func TestStartRemovesStaleSocket(t *testing.T) {
	t.Parallel()

	const upstream = "127.0.0.1:27233"

	path, err := socket.UnixPath(upstream)
	require.NoError(t, err)

	// Leave a file behind where the listener wants to bind. Without removal the
	// bind would fail with "address already in use" and the Check never succeeds.
	require.NoError(t, os.WriteFile(path, []byte("stale"), 0o600))
	t.Cleanup(func() { _ = os.Remove(path) })

	svr, err := proxy.New(upstream, upstreamConn(t, upstream))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lis, err := svr.Listen(ctx)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(ctx, lis) }()

	conn := dialUnix(t, upstream)
	defer func() { _ = conn.Close() }()

	_, err = grpc_health_v1.NewHealthClient(conn).Check(
		t.Context(),
		&grpc_health_v1.HealthCheckRequest{},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	require.NoError(t, svr.Stop(t.Context()))
	require.NoError(t, <-errCh)
}

func TestListenReturnsErrorWhenStaleSocketCannotBeRemoved(t *testing.T) {
	t.Parallel()

	const upstream = "127.0.0.1:37233"

	path, err := socket.UnixPath(upstream)
	require.NoError(t, err)

	// A non-empty directory at the socket path makes os.Remove fail, so Listen
	// returns before it ever binds.
	require.NoError(t, os.Mkdir(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "child"), nil, 0o600))
	t.Cleanup(func() { _ = os.RemoveAll(path) })

	svr, err := proxy.New(upstream, upstreamConn(t, upstream))
	require.NoError(t, err)

	_, err = svr.Listen(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to remove stale socket")
}

// upstreamConn returns a grpc.ClientConnInterface for New's cc argument. gRPC
// dials lazily, so this never opens a socket to upstream; the tests in this
// file never make an outbound RPC through it (they only exercise the local
// unix listener), so a plain client conn stands in for the pool-backed
// resolvingConn used in production.
func upstreamConn(t *testing.T, upstream string) grpc.ClientConnInterface {
	t.Helper()

	conn, err := grpc.NewClient(upstream, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// dialUnix returns a client connection to the proxy's unix socket for the given
// upstream host. The socket path matches what proxy.Listen binds.
func dialUnix(t *testing.T, upstream string) *grpc.ClientConn {
	t.Helper()

	path, err := socket.UnixPath(upstream)
	require.NoError(t, err)

	conn, err := grpc.NewClient(
		"unix://"+path,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	return conn
}
