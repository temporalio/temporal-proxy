package proxy_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

type failingCredentials struct {
	err error
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("returns a server with default options", func(t *testing.T) {
		t.Parallel()

		svr, err := proxy.New("127.0.0.1:7233")
		require.NoError(t, err)
		require.NotNil(t, svr)
	})

	t.Run("propagates credential errors", func(t *testing.T) {
		t.Parallel()

		svr, err := proxy.New(
			"127.0.0.1:7233",
			proxy.WithCredentials(failingCredentials{err: errors.New("boom")}),
		)
		require.Error(t, err)
		require.Nil(t, svr)
		require.ErrorContains(t, err, "outbound credentials")
		require.ErrorContains(t, err, "boom")
	})
}

func TestNewWithDialOptions(t *testing.T) {
	t.Parallel()

	cp, err := auth.NewStaticCredentialProvider("k", "", "")
	require.NoError(t, err)

	svr, err := proxy.New(
		"127.0.0.1:7233",
		proxy.WithCredentials(creds.NewClientTLS()),
		proxy.WithDialOptions(auth.DialOptions(cp)...),
	)
	require.NoError(t, err)
	require.NotNil(t, svr)
}

func TestServerStartAndStop(t *testing.T) {
	t.Parallel()

	// A unique upstream host gives this test its own socket path so it can run in
	// parallel with the others. The upstream is never dialed: the health service
	// the proxy serves locally answers the Check below.
	const upstream = "127.0.0.1:17233"

	log := logger.NewTestLogger()
	svr, err := proxy.New(upstream, proxy.WithLogger(log))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(ctx) }()

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

	svr, err := proxy.New(upstream)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(ctx) }()

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

func TestStartReturnsErrorWhenStaleSocketCannotBeRemoved(t *testing.T) {
	t.Parallel()

	const upstream = "127.0.0.1:37233"

	path, err := socket.UnixPath(upstream)
	require.NoError(t, err)

	// A non-empty directory at the socket path makes os.Remove fail, so Start
	// returns before it ever binds.
	require.NoError(t, os.Mkdir(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "child"), nil, 0o600))
	t.Cleanup(func() { _ = os.RemoveAll(path) })

	svr, err := proxy.New(upstream)
	require.NoError(t, err)

	err = svr.Start(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to remove stale socket")
}

func (f failingCredentials) DialOption() (grpc.DialOption, error) {
	return nil, f.err
}

// dialUnix returns a client connection to the proxy's unix socket for the given
// upstream host. The socket path matches what proxy.Start binds.
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
