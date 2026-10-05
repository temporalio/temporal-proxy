package server_test

import (
	"fmt"
	"strings"
	"testing"

	goprom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/rpc"
	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

const (
	panicMethod  = "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"
	panicMessage = "recovered panic serving RPC"
	panicValue   = "secret request contents"
)

func TestRecoveryInterceptorRecoversHandlerPanic(t *testing.T) {
	t.Parallel()

	r, reg := newTestReporter(t, metrics.MetadataLabels{})
	log := logger.NewTestLogger()

	err := r.RecoveryInterceptor(log)(nil, &fakeStream{ctx: t.Context()}, &grpc.StreamServerInfo{FullMethod: panicMethod},
		func(any, grpc.ServerStream) error { panic(panicValue) },
	)

	requireGenericInternal(t, err)
	requirePanicLogged(t, log)
	requirePanicCount(t, reg, 1)
}

// A Pump direction cannot panic onto the handler's goroutine, so it hands the
// panic back as an error, and the interceptor must treat that the same way.
func TestRecoveryInterceptorReportsPanicReturnedByThePump(t *testing.T) {
	t.Parallel()

	r, reg := newTestReporter(t, metrics.MetadataLabels{})
	log := logger.NewTestLogger()

	err := r.RecoveryInterceptor(log)(nil, &fakeStream{ctx: t.Context()}, &grpc.StreamServerInfo{FullMethod: panicMethod},
		func(any, grpc.ServerStream) error { return fmt.Errorf("forwarding: %w", panicErr()) },
	)

	requireGenericInternal(t, err)
	requirePanicLogged(t, log)
	requirePanicCount(t, reg, 1)
}

func TestRecoveryInterceptorPassesThroughWithoutPanic(t *testing.T) {
	t.Parallel()

	r, reg := newTestReporter(t, metrics.MetadataLabels{})
	log := logger.NewTestLogger()
	want := status.Error(codes.NotFound, "no such workflow")

	err := r.RecoveryInterceptor(log)(nil, &fakeStream{ctx: t.Context()}, &grpc.StreamServerInfo{FullMethod: panicMethod},
		func(any, grpc.ServerStream) error { return want },
	)

	require.Same(t, want, err, "an error with no panic must pass through untouched")
	require.False(t, log.Contains(panicMessage))
	requirePanicCount(t, reg, 0)
}

// TestRecoveryInterceptorAfterReporterRecordsInternal runs the two interceptors
// in the order the gateway installs them, over a real server, so a panicked RPC
// is seen by the caller as Internal and recorded as one in requests_total.
func TestRecoveryInterceptorAfterReporterRecordsInternal(t *testing.T) {
	t.Parallel()

	r, reg := newTestReporter(t, metrics.MetadataLabels{})
	log := logger.NewTestLogger()

	svr, err := server.New(
		server.WithStreamInterceptor(r.StreamInterceptor(), r.RecoveryInterceptor(log)),
		server.WithUnknownServiceHandler(func(any, grpc.ServerStream) error { panic(panicValue) }),
	)
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	err = conn.Invoke(t.Context(), panicMethod, &grpc_health_v1.HealthCheckRequest{}, &grpc_health_v1.HealthCheckResponse{})
	requireGenericInternal(t, err)

	require.NoError(t, svr.Stop(t.Context()))
	require.NoError(t, <-errCh)

	requirePanicLogged(t, log)
	requirePanicCount(t, reg, 1)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP tmprl_proxy_server_requests_total Total RPCs served, labeled by method and gRPC status code.
# TYPE tmprl_proxy_server_requests_total counter
tmprl_proxy_server_requests_total{code="Internal",method="`+panicMethod+`"} 1
`), "tmprl_proxy_server_requests_total"))
}

// panicErr recovers a panic the way a Pump direction does.
func panicErr() (err error) {
	defer func() { err = rpc.NewPanicError(recover()) }()
	panic(panicValue)
}

// requireGenericInternal asserts err is the Internal status a recovered panic
// returns, and that the panic value did not reach the caller.
func requireGenericInternal(t *testing.T, err error) {
	t.Helper()

	st, ok := status.FromError(err)
	require.True(t, ok, "want a status error, got %v", err)
	require.Equal(t, codes.Internal, st.Code())
	require.Equal(t, "internal error", st.Message())
}

// requirePanicLogged asserts the panic was logged at error level with its method,
// its value, and a stack captured mid-panic: runtime's panic frame is on the
// stack only until it unwinds, so finding it means the frame that panicked is
// there too.
func requirePanicLogged(t *testing.T, log *logger.TestLogger) {
	t.Helper()

	tags := log.TagsOf(logger.LevelError, panicMessage)
	require.NotNil(t, tags, "no %q entry was logged", panicMessage)
	require.Equal(t, panicMethod, tags["method"])
	require.Equal(t, panicValue, tags["panic"])
	require.Contains(t, tags["stack"], "runtime/panic.go")
}

// requirePanicCount asserts want panics were counted, all under panicMethod.
func requirePanicCount(t *testing.T, reg *goprom.Registry, want int) {
	t.Helper()

	if want == 0 {
		n, err := testutil.GatherAndCount(reg, "tmprl_proxy_server_panics_total")
		require.NoError(t, err)
		require.Zero(t, n)

		return
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
# HELP tmprl_proxy_server_panics_total Total panics recovered while serving an RPC, labeled by method.
# TYPE tmprl_proxy_server_panics_total counter
tmprl_proxy_server_panics_total{method=%q} %d
`, panicMethod, want)), "tmprl_proxy_server_panics_total"))
}
