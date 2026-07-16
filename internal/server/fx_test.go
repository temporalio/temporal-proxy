package server_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("wires defaults and runs the lifecycle", func(t *testing.T) {
		t.Parallel()

		app := newTestApp(
			t,
			fx.Supply(&config.Config{
				Listen: config.ListenConfig{HostPort: "127.0.0.1:0"},
				Upstreams: []config.Upstream{
					{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
				},
			}),
		)

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Start(startCtx))

		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))
	})

	t.Run("honours every optional dependency", func(t *testing.T) {
		t.Parallel()

		hc := server.HealthCheckFunc(time.Hour, func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
			return grpc_health_v1.HealthCheckResponse_SERVING
		})

		app := newTestApp(
			t,
			fx.Supply(
				&config.Config{
					Listen: config.ListenConfig{HostPort: "127.0.0.1:0"},
					Upstreams: []config.Upstream{
						{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
					},
				},
				fx.Annotate(hc, fx.As(new(server.HealthCheck))),
			),
			fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		)

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Start(startCtx))

		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))
	})

	t.Run("rejects invalid configuration before construction", func(t *testing.T) {
		t.Parallel()

		// TLS paths that don't exist trip creds.TLS.Validate at config-validation
		// time, before any server is constructed or listener bound. The fx
		// Invoke wraps the validation.Errors with "invalid configuration: %w".
		missing := filepath.Join(t.TempDir(), "missing.pem")

		app := newTestApp(t, fx.Supply(&config.Config{
			Listen: config.ListenConfig{
				HostPort: "127.0.0.1:0",
				TLS: &config.TLSConfig{
					Cert: missing,
					Key:  missing,
				},
			},
		}))

		require.Error(t, app.Err())
		require.ErrorContains(t, app.Err(), "invalid configuration")

		var errs validation.Errors
		require.ErrorAs(t, app.Err(), &errs, "expected validation.Errors in chain")
		require.NotEmpty(t, errs)
	})

	t.Run("surfaces listener creation errors at start", func(t *testing.T) {
		t.Parallel()

		// 1.2.3.4:0 is a well-formed host:port (passes IsHostPort and so
		// passes config validation) but isn't a local interface, so the
		// runtime bind fails with EADDRNOTAVAIL. This exercises the
		// listener-failure path that validation alone can't catch.
		app := newTestApp(
			t,
			fx.Supply(&config.Config{
				Listen: config.ListenConfig{HostPort: "1.2.3.4:0"},
				Upstreams: []config.Upstream{
					{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
				},
			}),
		)

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		err := app.Start(startCtx)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to create listener")
	})
}

func TestModuleWiresInjectedCodecAndHandler(t *testing.T) {
	t.Parallel()

	// The module forces the injected codec on the server and installs the
	// injected handler as the unknown-service handler. A recording codec proves
	// the former (the locally hosted health service exercises it); a sentinel
	// handler proves the latter (an unregistered method reaches it).
	rec := &recordingCodec{delegate: encoding.GetCodecV2("proto")}
	handler := grpc.StreamHandler(func(any, grpc.ServerStream) error {
		return status.Error(codes.Unimplemented, "injected-handler-reached")
	})

	f, _ := newTestFactory(t)
	addr := freeTCPAddr(t)
	app := fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(&config.Config{
			Listen:    config.ListenConfig{HostPort: addr},
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}},
		}),
		fx.Supply(f),
		fx.Provide(
			func() encoding.CodecV2 { return rec },
			func() grpc.StreamHandler { return handler },
		),
		server.Module,
		fx.NopLogger,
	)

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	callCtx, callCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer callCancel()

	// Local health is answered by the server itself and runs through the forced
	// codec. WaitForReady rides out the window before the serve goroutine begins
	// accepting.
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(
		callCtx,
		&grpc_health_v1.HealthCheckRequest{},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
	require.Positive(t, rec.calls.Load(), "the injected codec should be forced on the server")

	// An unregistered method routes to the injected handler.
	err = conn.Invoke(
		callCtx,
		"/unknown.Service/Method",
		&grpc_health_v1.HealthCheckRequest{},
		new(grpc_health_v1.HealthCheckResponse),
	)
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.ErrorContains(t, err, "injected-handler-reached")

	stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, app.Stop(stopCtx))
}

func TestModuleWiresMetricsInterceptor(t *testing.T) {
	t.Parallel()

	factory, reg := newTestFactory(t)

	handler := grpc.StreamHandler(func(any, grpc.ServerStream) error {
		return status.Error(codes.Unimplemented, "stub")
	})

	addr := freeTCPAddr(t)
	app := fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(&config.Config{
			Listen:    config.ListenConfig{HostPort: addr},
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}},
		}),
		fx.Supply(factory),
		fx.Provide(
			func() encoding.CodecV2 { return encoding.GetCodecV2("proto") },
			func() grpc.StreamHandler { return handler },
		),
		server.Module,
		fx.NopLogger,
	)

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	callCtx, callCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer callCancel()

	err = conn.Invoke(
		callCtx,
		"/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
		&grpc_health_v1.HealthCheckRequest{},
		new(grpc_health_v1.HealthCheckResponse),
		grpc.WaitForReady(true),
	)
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))

	stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, app.Stop(stopCtx))

	// This test proves Module installs the interceptor, so one recorded series is
	// enough; the exact labels are asserted by the reporter tests that own that
	// check.
	n, err := testutil.GatherAndCount(reg, "tmprl_proxy_server_requests_total")
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func newTestApp(t *testing.T, opts ...fx.Option) *fx.App {
	t.Helper()

	f, _ := newTestFactory(t)
	base := []fx.Option{
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(f),
		// Stand-in transparent-forwarding dependencies; the router module
		// provides the real ones in production.
		fx.Provide(
			func() encoding.CodecV2 { return encoding.GetCodecV2("proto") },
			func() grpc.StreamHandler {
				return func(any, grpc.ServerStream) error {
					return status.Error(codes.Unimplemented, "stub handler")
				}
			},
		),
		server.Module,
		fx.NopLogger,
	}

	return fx.New(append(base, opts...)...)
}

// freeTCPAddr reserves an ephemeral localhost TCP port and returns its address.
// The listener is closed before returning so the caller can bind it; the small
// race window is acceptable in tests.
func freeTCPAddr(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}
