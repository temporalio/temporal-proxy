package dataplane

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// fakeHealthClient implements only Watch. The embedded interface leaves every
// other method nil, so a call to one panics rather than quietly returning a
// zero value.
type fakeHealthClient struct {
	grpc_health_v1.HealthClient

	watchErr error
	recvErr  error

	calls  atomic.Int64
	gotCtx atomic.Value // context.Context, as handed to Watch
}

func (f *fakeHealthClient) Watch(
	ctx context.Context,
	_ *grpc_health_v1.HealthCheckRequest,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[grpc_health_v1.HealthCheckResponse], error) {
	f.calls.Add(1)
	f.gotCtx.Store(ctx)

	if f.watchErr != nil {
		return nil, f.watchErr
	}

	return &fakeHealthStream{err: f.recvErr}, nil
}

// fakeHealthStream implements only Recv, for the same reason.
type fakeHealthStream struct {
	grpc.ServerStreamingClient[grpc_health_v1.HealthCheckResponse]

	err error
}

func (f *fakeHealthStream) Recv() (*grpc_health_v1.HealthCheckResponse, error) {
	if f.err != nil {
		return nil, f.err
	}

	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

func TestStatusFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want grpc_health_v1.HealthCheckResponse_ServingStatus
	}{
		{
			name: "a message arrives",
			err:  nil,
			want: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		{
			name: "a rejection is still an answer",
			err:  status.Error(codes.Unauthenticated, "no credential"),
			want: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		{
			name: "any other status is an answer too",
			err:  status.Error(codes.Internal, "something went wrong"),
			want: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		{
			name: "a deadline means the chain never answered",
			err:  status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			want: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
		{
			name: "a bare context deadline means the same",
			err:  context.DeadlineExceeded,
			want: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
		{
			name: "an unreachable listener is not serving",
			err:  status.Error(codes.Unavailable, "connection refused"),
			want: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
		{
			name: "a transport failure carries no status at all",
			err:  errors.New("connection reset by peer"),
			want: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, statusFor(tt.err))
		})
	}
}

// TestWatchOnceCancelsTheStream covers the reason the exchange takes one
// message and hangs up: health.Server registers a watcher per open Watch, so a
// stream left open would leak an entry every interval. Nothing outside the
// process can observe that, which is why watchOnce takes a client.
func TestWatchOnceCancelsTheStream(t *testing.T) {
	t.Parallel()

	client := &fakeHealthClient{}

	require.NoError(t, watchOnce(t.Context(), client))

	ctx, ok := client.gotCtx.Load().(context.Context)
	require.True(t, ok, "Watch must be handed a context")

	select {
	case <-ctx.Done():
	default:
		t.Fatal("the stream context must be cancelled before watchOnce returns, or the watcher leaks")
	}
}

func TestWatchOnceReturnsTheExchangeOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		client *fakeHealthClient
		want   error
	}{
		{
			name:   "opening the stream failed",
			client: &fakeHealthClient{watchErr: status.Error(codes.Unavailable, "connection refused")},
			want:   status.Error(codes.Unavailable, "connection refused"),
		},
		{
			name:   "the first read failed",
			client: &fakeHealthClient{recvErr: status.Error(codes.DeadlineExceeded, "deadline")},
			want:   status.Error(codes.DeadlineExceeded, "deadline"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want.Error(), watchOnce(t.Context(), tt.client).Error())
		})
	}
}

// TestLoopbackCheckGuardsSkipTheNetwork points the check at an address nothing
// is listening on. A check that dialled would report NOT_SERVING, so SERVING is
// what proves the guard ran instead.
func TestLoopbackCheckGuardsSkipTheNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *config.Config
		addr bool
	}{
		{
			name: "nothing is bound yet",
			cfg:  &config.Config{},
			addr: false,
		},
		{
			name: "the gateway requires client certificates",
			cfg: &config.Config{Listen: config.ListenConfig{
				TLS: &config.TLSConfig{CA: "ca.crt", Cert: "server.crt", Key: "server.key"},
			}},
			addr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			check := newLoopbackCheck(tt.cfg, logger.NewNoopLogger())
			if tt.addr {
				check.setAddr(closedAddr(t))
			}

			require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, check.Status(t.Context()))
		})
	}
}

func TestLoopbackCheckServerTLSStaysEnabled(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Listen: config.ListenConfig{
		TLS: &config.TLSConfig{Cert: "server.crt", Key: "server.key"},
	}}

	check := newLoopbackCheck(cfg, logger.NewNoopLogger())

	require.True(t, check.enabled, "a gateway presenting a certificate is still dialled")
}

// TestLoopbackCheckDialsAWildcardBind covers the address the check is handed in
// production. A gateway configured with ":8443" binds the wildcard, so
// lis.Addr() is "[::]:8443" rather than a concrete host, and the dial works only
// because Go treats the unspecified address as loopback. Nothing else pins that.
func TestLoopbackCheckDialsAWildcardBind(t *testing.T) {
	t.Parallel()

	lis, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	require.Contains(t, lis.Addr().String(), "[::]", "this test is pointless unless the bind is a wildcard")

	check := newLoopbackCheck(&config.Config{}, logger.NewNoopLogger())
	check.setAddr(lis.Addr())

	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, check.Status(t.Context()))
}

// TestLoopbackCheckTimesOutOnASilentGateway is the wedge, against a real
// socket: the listener accepts and then says nothing, which is what a blocked
// interceptor chain looks like from outside.
func TestLoopbackCheckTimesOutOnASilentGateway(t *testing.T) {
	t.Parallel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			// Held open deliberately, and never answered.
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()

	cfg := &config.Config{Health: config.Health{Interval: time.Minute, Timeout: 300 * time.Millisecond}}
	check := newLoopbackCheck(cfg, logger.NewNoopLogger())
	check.setAddr(lis.Addr())

	start := time.Now()
	got := check.Status(t.Context())

	require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, got)
	require.Less(t, time.Since(start), 10*time.Second, "the check must be bounded by its timeout, not its interval")
}

func TestLoopbackCheckInterval(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Health: config.Health{Interval: 2 * time.Second, Timeout: time.Second}}
	check := newLoopbackCheck(cfg, logger.NewNoopLogger())

	require.Equal(t, 2*time.Second, check.Interval())
}

// closedAddr is an address that was bound and then released, so a dial to it is
// refused rather than hanging.
func closedAddr(t *testing.T) net.Addr {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := lis.Addr()
	require.NoError(t, lis.Close())

	return addr
}
