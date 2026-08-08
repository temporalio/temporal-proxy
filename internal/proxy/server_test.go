package proxy_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("returns a server with default options", func(t *testing.T) {
		t.Parallel()

		svr, err := proxy.New("127.0.0.1:7233", forwarder(t, "127.0.0.1:7233"))
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
	svr, err := proxy.New(upstream, forwarder(t, upstream), proxy.WithLogger(log))
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

	// An ephemeral port gives this test its own socket path. A fixed one is shared
	// with every past run on the machine, so a socket left behind by a run that was
	// killed before it could shut down would occupy the path and make planting the
	// stale socket below fail with "operation not supported on socket".
	upstream := deadUpstream(t)

	path, err := socket.UnixPath(upstream)
	require.NoError(t, err)

	// Clear the path first: a run killed before its cleanup leaves its own socket
	// or directory behind, and planting over either fails.
	require.NoError(t, os.RemoveAll(path))

	// Leave a real socket behind, which is what a killed process leaves. Without
	// removal the bind would fail with "address already in use" and the Check never
	// succeeds.
	stale, err := net.Listen("unix", path)
	require.NoError(t, err)
	unix, ok := stale.(*net.UnixListener)
	require.True(t, ok)
	unix.SetUnlinkOnClose(false)
	require.NoError(t, unix.Close())
	t.Cleanup(func() { _ = os.Remove(path) })
	require.FileExists(t, path, "expected a stale socket to be planted")

	svr, err := proxy.New(upstream, forwarder(t, upstream))
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

func TestNewWithSocketPathOverridesDerivedPath(t *testing.T) {
	t.Parallel()

	// A directory under os.TempDir() rather than t.TempDir() keeps the path
	// short: t.TempDir() embeds the full test name, which here would push the
	// socket path past the sun_path limit socket.UnixPath enforces.
	dir, err := os.MkdirTemp("", "socket-override")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	want := filepath.Join(dir, "override.sock")

	svr, err := proxy.New("127.0.0.1:7233", forwarder(t, "127.0.0.1:7233"), proxy.WithSocketPath(want))
	require.NoError(t, err)

	lis, err := svr.Listen(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	require.Equal(t, want, lis.Addr().String())
}

func TestNewRejectsOverlongSocketPath(t *testing.T) {
	t.Parallel()

	path := filepath.Join(os.TempDir(), strings.Repeat("d", 120)+".sock")

	_, err := proxy.New("127.0.0.1:7233", forwarder(t, "127.0.0.1:7233"), proxy.WithSocketPath(path))
	require.ErrorContains(t, err, "invalid socket path")
	require.ErrorContains(t, err, "exceeds limit")
}

func TestListenReturnsErrorWhenStaleSocketCannotBeRemoved(t *testing.T) {
	t.Parallel()

	// As above, an ephemeral port keeps the path this test's own, so a leftover
	// socket cannot make the Mkdir below fail with "file exists".
	upstream := deadUpstream(t)

	path, err := socket.UnixPath(upstream)
	require.NoError(t, err)

	// Clear the path first, as above.
	require.NoError(t, os.RemoveAll(path))

	// A non-empty directory at the socket path makes os.Remove fail, so Listen
	// returns before it ever binds.
	require.NoError(t, os.Mkdir(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "child"), nil, 0o600))
	t.Cleanup(func() { _ = os.RemoveAll(path) })

	svr, err := proxy.New(upstream, forwarder(t, upstream))
	require.NoError(t, err)

	_, err = svr.Listen(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to remove stale socket")
}

// forwarder returns the Forwarder for New's fw argument, allowing the services
// named or the default set when none are. gRPC dials lazily, so the underlying
// client conn opens no socket to upstream until a request needs it, which lets
// the tests that only exercise the local unix listener pass an upstream that was
// never started. A plain client conn stands in for the pool-backed resolvingConn
// used in production.
func forwarder(t *testing.T, upstream string, allowed ...string) *proxy.Forwarder {
	t.Helper()

	if len(allowed) == 0 {
		allowed = services.Default()
	}

	conn, err := grpc.NewClient(upstream, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	fw, err := proxy.NewForwarder(conn, services.NewAllowlist(allowed))
	require.NoError(t, err)

	return fw
}

// startProxy runs a proxy forwarding the named services to upstream and returns a
// client connection to its local unix socket. Stop takes a fresh context because
// the test's own is already cancelled by the time cleanups run.
func startProxy(t *testing.T, upstream string, allowed ...string) *grpc.ClientConn {
	t.Helper()

	svr, err := proxy.New(upstream, forwarder(t, upstream, allowed...))
	require.NoError(t, err)

	ctx := t.Context()
	lis, err := svr.Listen(ctx)
	require.NoError(t, err)

	go func() { _ = svr.Start(ctx, lis) }()
	t.Cleanup(func() { _ = svr.Stop(context.Background()) })

	conn := dialUnix(t, upstream)
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
