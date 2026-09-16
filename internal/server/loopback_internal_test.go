package server

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
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
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
			name: "a listener that is not accepting is not serving",
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

// TestLoopbackHealthCheckIsOptIn pins that a server nobody asked for one on
// keeps the stub and creates no in-process listener. The per-upstream proxies
// bind sockets nothing probes, and a check per upstream would be a goroutine and
// a connection every interval for a status no one reads.
func TestLoopbackHealthCheckIsOptIn(t *testing.T) {
	t.Parallel()

	svr, err := New(WithLogger(logger.NewNoopLogger()))
	require.NoError(t, err)

	require.Nil(t, svr.loopbackLis)
	require.NotNil(t, svr.healthCheck)
	require.IsType(t, &healthCheckFn{}, svr.healthCheck)
}

// TestLoopbackHealthCheckWinsOverAnExplicitCheck pins the precedence the option
// documents, in both orders, since an option set resolved by last-write would
// otherwise make it depend on the caller.
func TestLoopbackHealthCheckWinsOverAnExplicitCheck(t *testing.T) {
	t.Parallel()

	stub := HealthCheckFunc(time.Minute, func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
		return grpc_health_v1.HealthCheckResponse_SERVING
	})

	tests := []struct {
		name string
		opts []Option
	}{
		{name: "loopback last", opts: []Option{WithHealthCheck(stub), WithLoopbackHealthCheck(time.Minute, time.Second)}},
		{name: "loopback first", opts: []Option{WithLoopbackHealthCheck(time.Minute, time.Second), WithHealthCheck(stub)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svr, err := New(append(tt.opts, WithLogger(logger.NewNoopLogger()))...)
			require.NoError(t, err)

			require.NotNil(t, svr.loopbackLis)
			require.IsType(t, &loopbackCheck{}, svr.healthCheck)
		})
	}
}

// TestLoopbackCheckAnswersOverMutualTLS is the regression test for the gap this
// check used to leave. The server demands a client certificate on the listener
// it accepts traffic on, which is a configuration the check cannot dial without
// issuing itself one; going over the in-process listener instead means the
// status is a real answer rather than an assumed SERVING.
func TestLoopbackCheckAnswersOverMutualTLS(t *testing.T) {
	t.Parallel()

	ca, cert, key := testutil.GenerateMTLSCerts(t)

	svr := startLoopbackServer(t,
		WithCredentials(creds.NewListener(creds.WithCA(ca), creds.WithCertificate(cert, key))),
		WithLoopbackHealthCheck(time.Minute, 5*time.Second),
	)

	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, svr.healthCheck.Status(t.Context()))
}

// TestLoopbackCheckReportsAWedgedChain is the wedge the check exists for: a
// stream interceptor that never returns leaves the unary Check answering
// SERVING, because the gateway carries no unary interceptors, while nothing
// streamed can complete.
func TestLoopbackCheckReportsAWedgedChain(t *testing.T) {
	t.Parallel()

	wedge := func(_ any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
		<-ss.Context().Done()

		return ss.Context().Err()
	}

	svr := startLoopbackServer(t,
		WithStreamInterceptor(wedge),
		WithLoopbackHealthCheck(time.Minute, 300*time.Millisecond),
	)

	start := time.Now()
	got := svr.healthCheck.Status(t.Context())

	require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, got)
	require.Less(t, time.Since(start), 10*time.Second, "the check must be bounded by its timeout, not its interval")
}

// TestLoopbackCheckRunsTheStreamChain proves the call is not shortcut around the
// interceptors: an interceptor that rejects every stream is still an answer, so
// the status stays SERVING and the interceptor records that it ran.
func TestLoopbackCheckRunsTheStreamChain(t *testing.T) {
	t.Parallel()

	var seen atomic.Int64

	reject := func(_ any, _ grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
		seen.Add(1)

		return status.Error(codes.Unauthenticated, "no credential")
	}

	svr := startLoopbackServer(t,
		WithStreamInterceptor(reject),
		WithLoopbackHealthCheck(time.Minute, 5*time.Second),
	)

	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, svr.healthCheck.Status(t.Context()))
	require.Equal(t, int64(1), seen.Load(), "the check must travel the stream interceptor chain")
}

// startLoopbackServer builds a server from opts and serves it, returning once
// Start is under way. The real listener is a loopback socket nothing dials: the
// point of every caller is what happens over the in-process one.
func startLoopbackServer(t *testing.T, opts ...Option) *Server {
	t.Helper()

	svr, err := New(append(opts, WithLogger(logger.NewNoopLogger()))...)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = svr.Start(t.Context(), lis) }()
	t.Cleanup(func() { require.NoError(t, svr.Stop(context.WithoutCancel(t.Context()))) })

	return svr
}
