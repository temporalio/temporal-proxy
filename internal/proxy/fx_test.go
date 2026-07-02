package proxy_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/interceptor"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("wires defaults and runs the lifecycle", func(t *testing.T) {
		t.Parallel()

		upstream := freeUpstream(t)

		app := newProxyApp(t, &config.Config{
			Upstream: config.Upstream{Listen: config.ListenConfig{HostPort: upstream}},
		})
		require.NoError(t, app.Err())

		startServeStop(t, app, upstream)
	})

	t.Run("uses the supplied logger", func(t *testing.T) {
		t.Parallel()

		upstream := freeUpstream(t)

		log := logger.NewTestLogger()
		app := newProxyApp(
			t,
			&config.Config{Upstream: config.Upstream{Listen: config.ListenConfig{HostPort: upstream}}},
			fx.Provide(func() logger.Logger { return log }),
		)
		require.NoError(t, app.Err())

		startServeStop(t, app, upstream)

		require.True(t, log.Contains("Starting the server"), "expected the injected logger to be used")
	})

	t.Run("chains provided unary interceptors onto the upstream connection", func(t *testing.T) {
		t.Parallel()

		upstream := freeUpstream(t)

		fired := make(chan string, 4)
		in := grpc.UnaryClientInterceptor(func(_ context.Context, method string, _, _ any, _ *grpc.ClientConn, _ grpc.UnaryInvoker, _ ...grpc.CallOption) error {
			fired <- method
			return status.Error(codes.Unavailable, "short-circuit")
		})

		app := newProxyApp(
			t,
			&config.Config{Upstream: config.Upstream{Listen: config.ListenConfig{HostPort: upstream}}},
			fx.Provide(fx.Annotate(
				func() []grpc.UnaryClientInterceptor { return []grpc.UnaryClientInterceptor{in} },
				proxy.UnaryInterceptorsTag,
			)),
		)
		require.NoError(t, app.Err())

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Start(startCtx))

		conn := dialUnix(t, upstream)
		defer func() { _ = conn.Close() }()

		// The interceptor short-circuits the forwarded call, so it errors, but only
		// after fx has passed it through the value group and it has run on the
		// upstream connection. It fires synchronously before the call returns.
		_, err := workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
			startCtx,
			&workflowservice.GetSystemInfoRequest{},
			grpc.WaitForReady(true),
		)
		require.Error(t, err)

		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))

		// Non-blocking: if the interceptor was not wired through, fail loudly here
		// rather than block forever on an empty channel.
		select {
		case method := <-fired:
			require.Equal(t, "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo", method)
		default:
			t.Fatal("expected the supplied interceptor to run on the forwarded call")
		}
	})

	t.Run("runs the payload interceptor contributed by interceptor.Module", func(t *testing.T) {
		t.Parallel()

		upstream := freeUpstream(t)

		encoded := make(chan struct{}, 1)
		app := newProxyApp(
			t,
			&config.Config{Upstream: config.Upstream{Listen: config.ListenConfig{HostPort: upstream}}},
			interceptor.Module,
			fx.Supply([]interceptor.PayloadCodec{recordingCodec{encoded: encoded}}),
		)
		require.NoError(t, app.Err())

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Start(startCtx))

		conn := dialUnix(t, upstream)
		defer func() { _ = conn.Close() }()

		// StartWorkflowExecution carries an input payload. Forwarding it upstream
		// runs the payload interceptor's outbound codec chain before the (down)
		// upstream is dialed, so the call errors but the codec has already fired.
		_, err := workflowservice.NewWorkflowServiceClient(conn).StartWorkflowExecution(
			startCtx,
			&workflowservice.StartWorkflowExecutionRequest{
				Input: &common.Payloads{Payloads: []*common.Payload{{Data: []byte("hi")}}},
			},
			grpc.WaitForReady(true),
		)
		require.Error(t, err)

		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))

		select {
		case <-encoded:
		default:
			t.Fatal("expected the interceptor.Module codec to encode the forwarded payload")
		}
	})

	t.Run("rejects invalid upstream configuration before construction", func(t *testing.T) {
		t.Parallel()

		app := newProxyApp(t, &config.Config{
			Upstream: config.Upstream{Listen: config.ListenConfig{HostPort: "not-a-host-port"}},
		})

		require.Error(t, app.Err())
		require.ErrorContains(t, app.Err(), "invalid upstream configuration")

		var errs validation.Errors
		require.ErrorAs(t, app.Err(), &errs, "expected validation.Errors in chain")
		require.NotEmpty(t, errs)
	})
}

// recordingCodec is an interceptor.PayloadCodec that signals encoded when Encode
// runs and otherwise leaves payloads unchanged. The non-blocking send keeps it
// safe for the concurrent visits the interceptor performs.
type recordingCodec struct {
	encoded chan<- struct{}
}

func (c recordingCodec) Encode(_ context.Context, p *common.Payload) (*common.Payload, error) {
	select {
	case c.encoded <- struct{}{}:
	default:
	}

	return p, nil
}

func (c recordingCodec) Decode(_ context.Context, p *common.Payload) (*common.Payload, error) {
	return p, nil
}

// freeUpstream returns a loopback host:port backed by a free ephemeral port.
// The proxy never binds or connects to it in these tests; a fresh port simply
// gives each test a unique derived socket path so they run in parallel without
// colliding.
func freeUpstream(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, lis.Close())

	return lis.Addr().String()
}

func newProxyApp(t *testing.T, cfg *config.Config, opts ...fx.Option) *fx.App {
	t.Helper()

	base := []fx.Option{
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		proxy.Module,
		fx.NopLogger,
	}

	return fx.New(append(base, opts...)...)
}

// startServeStop starts the app, confirms the proxy serves on its unix socket
// via the local health service, then stops the app.
func startServeStop(t *testing.T, app *fx.App, upstream string) {
	t.Helper()

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	conn := dialUnix(t, upstream)
	defer func() { _ = conn.Close() }()

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(
		startCtx,
		&grpc_health_v1.HealthCheckRequest{},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

	stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, app.Stop(stopCtx))
}
