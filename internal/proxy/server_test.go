package proxy_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/proxy"
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

		svr, err := proxy.New(freeUpstream(t))
		require.NoError(t, err)
		require.NotNil(t, svr)
	})

	t.Run("propagates credential errors", func(t *testing.T) {
		t.Parallel()

		svr, err := proxy.New(
			freeUpstream(t),
			proxy.WithCredentials(failingCredentials{err: errors.New("boom")}),
		)
		require.Error(t, err)
		require.Nil(t, svr)
		require.ErrorContains(t, err, "outbound credentials")
		require.ErrorContains(t, err, "boom")
	})
}

func TestServerStartAndStop(t *testing.T) {
	t.Parallel()

	// A free ephemeral port gives this test its own socket path so it can run in
	// parallel with the others. The upstream is never dialed: the health service
	// the proxy serves locally answers the Check below.
	upstream := freeUpstream(t)

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

	upstream := freeUpstream(t)

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

	upstream := freeUpstream(t)

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

func TestWithUnaryInterceptor(t *testing.T) {
	t.Parallel()

	upstream := freeUpstream(t)

	// first records and forwards; second records and short-circuits with an error
	// so the never-dialed upstream is never actually contacted. Supplying them via
	// two separate options proves the interceptors accumulate and chain in order.
	fired := make(chan string, 8)
	first := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		fired <- "first:" + method
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	second := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ grpc.UnaryInvoker, _ ...grpc.CallOption) error {
		fired <- "second"
		return status.Error(codes.Unavailable, "short-circuit")
	}

	svr, err := proxy.New(upstream, proxy.WithUnaryInterceptor(first), proxy.WithUnaryInterceptor(second))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(ctx) }()

	conn := dialUnix(t, upstream)
	defer func() { _ = conn.Close() }()

	// The forwarded call reaches the upstream client interceptors and is
	// short-circuited before any upstream dial, so it returns an error.
	_, callErr := workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
		t.Context(),
		&workflowservice.GetSystemInfoRequest{},
		grpc.WaitForReady(true),
	)
	require.Error(t, callErr)

	require.NoError(t, svr.Stop(t.Context()))
	require.NoError(t, <-errCh)

	// Both interceptors fire synchronously before the call returns, so read
	// without blocking: a regression that stops them firing fails loudly here
	// instead of hanging on an empty channel.
	require.Equal(t, "first:/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo", recvOr(t, fired))
	require.Equal(t, "second", recvOr(t, fired))
}

func TestWithStreamInterceptor(t *testing.T) {
	t.Parallel()

	// WorkflowService exposes no streaming RPCs, so a stream interceptor cannot be
	// exercised end-to-end through the proxy. This confirms the option is accepted
	// and chained onto the dial options without error at construction.
	streamIn := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(ctx, desc, cc, method, opts...)
	}

	svr, err := proxy.New(freeUpstream(t), proxy.WithStreamInterceptor(streamIn))
	require.NoError(t, err)
	require.NotNil(t, svr)
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

// recvOr returns a value already buffered on ch, or fails the test if none is
// present. Use it for values a synchronous call is expected to have produced by
// the time it returns, so a missing value fails loudly instead of blocking.
func recvOr[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	default:
		t.Fatal("expected a value on the channel, but none was ready")
		panic("unreachable")
	}
}
