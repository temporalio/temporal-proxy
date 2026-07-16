package server_test

import (
	"strings"
	"testing"
	"time"

	goprom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/server"
)

func TestReporterObserve(t *testing.T) {
	t.Parallel()

	t.Run("counts requests by method and code", func(t *testing.T) {
		t.Parallel()

		r, reg := newTestReporter(t)

		r.Observe("/svc/Method", codes.OK, 750*time.Millisecond)
		r.Observe("/svc/Method", codes.OK, 250*time.Millisecond)
		r.Observe("/svc/Other", codes.NotFound, 2*time.Second)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP tmprl_proxy_server_requests_total Total RPCs served, labeled by method and gRPC status code.
# TYPE tmprl_proxy_server_requests_total counter
tmprl_proxy_server_requests_total{code="OK",method="/svc/Method"} 2
tmprl_proxy_server_requests_total{code="NotFound",method="/svc/Other"} 1
`), "tmprl_proxy_server_requests_total"))
	})

	t.Run("buckets durations by method", func(t *testing.T) {
		t.Parallel()

		r, reg := newTestReporter(t)

		r.Observe("/svc/Method", codes.OK, 750*time.Millisecond)
		r.Observe("/svc/Method", codes.OK, 250*time.Millisecond)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP tmprl_proxy_server_request_duration_seconds Time spent serving an RPC end to end, labeled by method.
# TYPE tmprl_proxy_server_request_duration_seconds histogram
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="0.005"} 0
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="0.01"} 0
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="0.025"} 0
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="0.05"} 0
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="0.1"} 0
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="0.25"} 1
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="0.5"} 1
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="1"} 2
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="2.5"} 2
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="5"} 2
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="10"} 2
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="30"} 2
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="60"} 2
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="120"} 2
tmprl_proxy_server_request_duration_seconds_bucket{method="/svc/Method",le="+Inf"} 2
tmprl_proxy_server_request_duration_seconds_sum{method="/svc/Method"} 1
tmprl_proxy_server_request_duration_seconds_count{method="/svc/Method"} 2
`), "tmprl_proxy_server_request_duration_seconds"))
	})
}

func TestReporterStreamInterceptor(t *testing.T) {
	t.Parallel()

	t.Run("records the returned status code", func(t *testing.T) {
		t.Parallel()

		r, reg := newTestReporter(t)
		interceptor := r.StreamInterceptor()

		info := &grpc.StreamServerInfo{FullMethod: "/svc/Fail"}
		handler := func(any, grpc.ServerStream) error {
			return status.Error(codes.PermissionDenied, "nope")
		}

		err := interceptor(nil, nil, info, handler)
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP tmprl_proxy_server_requests_total Total RPCs served, labeled by method and gRPC status code.
# TYPE tmprl_proxy_server_requests_total counter
tmprl_proxy_server_requests_total{code="PermissionDenied",method="/svc/Fail"} 1
`), "tmprl_proxy_server_requests_total"))
	})

	t.Run("maps a nil error to OK", func(t *testing.T) {
		t.Parallel()

		r, reg := newTestReporter(t)
		interceptor := r.StreamInterceptor()

		info := &grpc.StreamServerInfo{FullMethod: "/svc/Ok"}
		handler := func(any, grpc.ServerStream) error { return nil }

		require.NoError(t, interceptor(nil, nil, info, handler))

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP tmprl_proxy_server_requests_total Total RPCs served, labeled by method and gRPC status code.
# TYPE tmprl_proxy_server_requests_total counter
tmprl_proxy_server_requests_total{code="OK",method="/svc/Ok"} 1
`), "tmprl_proxy_server_requests_total"))
	})
}

func TestReporterInterceptorMetersForwardedTraffic(t *testing.T) {
	t.Parallel()

	r, reg := newTestReporter(t)

	handler := func(_ any, _ grpc.ServerStream) error {
		return status.Error(codes.Unimplemented, "unknown")
	}

	svr, err := server.New(
		server.WithStreamInterceptor(r.StreamInterceptor()),
		server.WithUnknownServiceHandler(handler),
	)
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	// A "unary" call to an unregistered method is served as a stream through the
	// unknown-service handler, so the stream interceptor meters it.
	err = conn.Invoke(
		t.Context(),
		"/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
		&grpc_health_v1.HealthCheckRequest{},
		&grpc_health_v1.HealthCheckResponse{},
	)
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))

	require.NoError(t, svr.Stop(t.Context()))
	require.NoError(t, <-errCh)

	n, err := testutil.GatherAndCount(reg, "tmprl_proxy_server_requests_total")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP tmprl_proxy_server_requests_total Total RPCs served, labeled by method and gRPC status code.
# TYPE tmprl_proxy_server_requests_total counter
tmprl_proxy_server_requests_total{code="Unimplemented",method="/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"} 1
`), "tmprl_proxy_server_requests_total"))
}

func newTestReporter(t *testing.T) (*server.Reporter, *goprom.Registry) {
	t.Helper()
	f, reg := newTestFactory(t)
	return server.NewReporter(f.ForSubsystem("server")), reg
}

// newTestFactory builds a namespace-scoped metrics Factory backed by a fresh,
// isolated registry, returning both so callers can gather what the Factory's
// collectors record.
func newTestFactory(t *testing.T) (*metrics.Factory, *goprom.Registry) {
	t.Helper()
	reg := goprom.NewRegistry()
	return metrics.New("tmprl_proxy", promauto.With(reg)), reg
}
