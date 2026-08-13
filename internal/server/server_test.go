package server_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

const (
	insecureMessage = "Running with insecure credentials. Configure TLS for production use."
	forcedMessage   = "Drain ended with calls in flight. Dropping them"
	cleanMessage    = "Server stopped cleanly"
)

type (
	failingCredentials struct {
		err error
	}

	stubCredentials struct {
		secure bool
	}

	recordingCodec struct {
		delegate encoding.CodecV2
		calls    atomic.Int32
	}
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("returns a server with default options", func(t *testing.T) {
		t.Parallel()

		svr, err := server.New()
		require.NoError(t, err)
		require.NotNil(t, svr)
	})

	t.Run("propagates credential errors", func(t *testing.T) {
		t.Parallel()

		svr, err := server.New(server.WithCredentials(failingCredentials{err: errors.New("boom")}))
		require.Error(t, err)
		require.Nil(t, svr)
		require.ErrorContains(t, err, "boom")
	})

	t.Run("uses the supplied logger for lifecycle events", func(t *testing.T) {
		t.Parallel()

		log := logger.NewTestLogger()
		hc := server.HealthCheckFunc(time.Hour, func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
			return grpc_health_v1.HealthCheckResponse_SERVING
		})

		svr, err := server.New(
			server.WithLogger(log),
			server.WithHealthCheck(hc),
		)
		require.NoError(t, err)

		lis := bufconn.Listen(1024)
		defer func() { _ = lis.Close() }()

		errCh := make(chan error, 1)
		go func() { errCh <- svr.Start(t.Context(), lis) }()

		require.Eventually(t, func() bool {
			return log.Contains("Starting the server")
		}, time.Second, 10*time.Millisecond)

		require.NoError(t, svr.Stop(t.Context()))
		<-errCh

		require.True(t, log.Contains("Shutting down"), "expected shutdown to be logged")
	})
}

func TestServerInsecureWarning(t *testing.T) {
	t.Parallel()

	t.Run("warns when credentials are insecure", func(t *testing.T) {
		t.Parallel()

		log := logger.NewTestLogger()
		svr, err := server.New(
			server.WithLogger(log),
			server.WithCredentials(stubCredentials{secure: false}),
		)
		require.NoError(t, err)

		lis := bufconn.Listen(1024)
		defer func() { _ = lis.Close() }()

		errCh := make(chan error, 1)
		go func() { errCh <- svr.Start(t.Context(), lis) }()

		require.Eventually(t, func() bool {
			return log.Contains(insecureMessage)
		}, time.Second, 10*time.Millisecond)

		require.NoError(t, svr.Stop(t.Context()))
		<-errCh
	})

	t.Run("does not warn when credentials are secure", func(t *testing.T) {
		t.Parallel()

		log := logger.NewTestLogger()
		svr, err := server.New(
			server.WithLogger(log),
			server.WithCredentials(stubCredentials{secure: true}),
		)
		require.NoError(t, err)

		lis := bufconn.Listen(1024)
		defer func() { _ = lis.Close() }()

		errCh := make(chan error, 1)
		go func() { errCh <- svr.Start(t.Context(), lis) }()

		require.Eventually(t, func() bool {
			return log.Contains("Starting the server")
		}, time.Second, 10*time.Millisecond)

		require.NoError(t, svr.Stop(t.Context()))
		<-errCh

		require.False(t, log.Contains(insecureMessage))
	})
}

func TestWithUnaryInterceptor(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var calls []string

	record := func(name string) grpc.UnaryServerInterceptor {
		return func(
			ctx context.Context,
			req any,
			_ *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (any, error) {
			mu.Lock()
			calls = append(calls, name)
			mu.Unlock()
			return handler(ctx, req)
		}
	}

	svr, err := server.New(
		server.WithUnaryInterceptor(record("first"), record("second")),
	)
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	client := grpc_health_v1.NewHealthClient(conn)
	_, err = client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)

	require.NoError(t, svr.Stop(t.Context()))
	<-errCh

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"first", "second"}, calls)
}

func TestWithStreamInterceptor(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	interceptor := func(
		srv any,
		ss grpc.ServerStream,
		_ *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		called.Store(true)
		return handler(srv, ss)
	}

	svr, err := server.New(server.WithStreamInterceptor(interceptor))
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	client := grpc_health_v1.NewHealthClient(conn)

	ctx, cancel := context.WithCancel(t.Context())
	stream, err := client.Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)

	// Recv once to ensure the server-side handler (and thus the interceptor) ran.
	_, err = stream.Recv()
	require.NoError(t, err)
	cancel()

	require.True(t, called.Load())

	require.NoError(t, svr.Stop(t.Context()))
	<-errCh
}

func TestWithService(t *testing.T) {
	t.Parallel()

	echoDesc := grpc.ServiceDesc{
		ServiceName: "test.v1.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Ping",
				Handler: func(
					_ any,
					ctx context.Context,
					dec func(any) error,
					_ grpc.UnaryServerInterceptor,
				) (any, error) {
					in := new(grpc_health_v1.HealthCheckRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					return &grpc_health_v1.HealthCheckResponse{
						Status: grpc_health_v1.HealthCheckResponse_SERVING,
					}, nil
				},
			},
		},
	}

	var registered bool
	svr, err := server.New(server.WithService(func(r grpc.ServiceRegistrar) {
		registered = true
		r.RegisterService(&echoDesc, nil)
	}))
	require.NoError(t, err)
	require.True(t, registered, "service registration callback should be invoked")

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	resp := new(grpc_health_v1.HealthCheckResponse)
	err = conn.Invoke(
		t.Context(),
		"/test.v1.Echo/Ping",
		&grpc_health_v1.HealthCheckRequest{},
		resp,
	)
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

	require.NoError(t, svr.Stop(t.Context()))
	<-errCh
}

func TestWithUnknownServiceHandler(t *testing.T) {
	t.Parallel()

	handler := func(_ any, stream grpc.ServerStream) error {
		return status.Error(codes.Unimplemented, "reached-unknown-handler")
	}

	svr, err := server.New(server.WithUnknownServiceHandler(handler))
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	err = conn.Invoke(
		t.Context(),
		"/not.registered.Service/Method",
		&grpc_health_v1.HealthCheckRequest{},
		&grpc_health_v1.HealthCheckResponse{},
	)
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.ErrorContains(t, err, "reached-unknown-handler")

	require.NoError(t, svr.Stop(t.Context()))
	<-errCh
}

func TestWithServerCodec(t *testing.T) {
	t.Parallel()

	rec := &recordingCodec{delegate: encoding.GetCodecV2("proto")}

	svr, err := server.New(server.WithServerCodec(rec))
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	client := grpc_health_v1.NewHealthClient(conn)
	resp, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

	require.NoError(t, svr.Stop(t.Context()))
	<-errCh

	require.Positive(t, rec.calls.Load(), "forced server codec should be exercised")
}

func TestWithHealthServices(t *testing.T) {
	t.Parallel()

	// Literal names rather than the services package's constants: the server is
	// generic and only publishes what it is handed.
	const (
		workflow = "temporal.api.workflowservice.v1.WorkflowService"
		operator = "temporal.api.operatorservice.v1.OperatorService"
	)

	var serving atomic.Int32
	serving.Store(int32(grpc_health_v1.HealthCheckResponse_SERVING))
	hc := server.HealthCheckFunc(10*time.Millisecond, func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
		return grpc_health_v1.HealthCheckResponse_ServingStatus(serving.Load())
	})

	svr, err := server.New(
		server.WithHealthCheck(hc),
		server.WithHealthServices(workflow, operator),
	)
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	client := grpc_health_v1.NewHealthClient(conn)
	check := func(service string) (*grpc_health_v1.HealthCheckResponse, error) {
		return client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{Service: service})
	}

	// Asserted without Eventually: every entry exists from construction, so a
	// client that connects the instant the server starts cannot see NOT_FOUND for
	// a service the server advertises.
	for _, service := range []string{"", workflow, operator} {
		resp, err := check(service)
		require.NoError(t, err, "Check(%q)", service)
		require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus(), "Check(%q)", service)
	}

	// A service that was not advertised stays unknown, so a client cannot read a
	// status for something this server does not answer for.
	_, err = check("temporal.api.adminservice.v1.AdminService")
	require.Equal(t, codes.NotFound, status.Code(err))

	// The periodic check drives every entry, not just the unnamed one.
	serving.Store(int32(grpc_health_v1.HealthCheckResponse_NOT_SERVING))
	for _, service := range []string{"", workflow, operator} {
		require.Eventually(t, func() bool {
			resp, err := check(service)
			return err == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_NOT_SERVING
		}, time.Second, 10*time.Millisecond, "Check(%q)", service)
	}

	require.NoError(t, svr.Stop(t.Context()))
	<-errCh
}

func TestServerStartAndStop(t *testing.T) {
	t.Parallel()

	var status atomic.Int32
	status.Store(int32(grpc_health_v1.HealthCheckResponse_SERVING))
	hc := server.HealthCheckFunc(10*time.Millisecond, func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
		return grpc_health_v1.HealthCheckResponse_ServingStatus(status.Load())
	})

	svr, err := server.New(server.WithHealthCheck(hc))
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- svr.Start(ctx, lis)
	}()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	client := grpc_health_v1.NewHealthClient(conn)

	require.Eventually(t, func() bool {
		resp, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
		return err == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING
	}, time.Second, 10*time.Millisecond)

	// Flip the reported status and confirm the periodic health check propagates
	// the change to the gRPC health service.
	status.Store(int32(grpc_health_v1.HealthCheckResponse_NOT_SERVING))

	require.Eventually(t, func() bool {
		resp, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
		return err == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, svr.Stop(t.Context()))

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("server did not stop after shutdown")
	}
}

func TestStopForcesShutdownWhenHandlerHangs(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	svr, callErr := wedgedServer(t,
		server.WithLogger(log),
		server.WithShutdownTimeout(100*time.Millisecond),
	)

	start := time.Now()
	require.NoError(t, svr.Stop(t.Context()), "a forced shutdown is still a shutdown")
	require.Less(t, time.Since(start), 5*time.Second, "Stop waited on the wedged handler")

	require.True(t, log.Contains(forcedMessage), "expected the forced drain to be logged")
	require.False(t, log.Contains(cleanMessage))
	// Serve scopes the logger to the bound address, so that tag leads every entry.
	require.True(
		t,
		log.ContainsEntry(
			logger.LevelWarn,
			forcedMessage,
			tag.String("addr", "bufconn"),
			tag.String("cause", "shutdown timeout"),
			tag.String("timeout", "100ms"),
		),
		"the budget expiring should be reported as the cause",
	)

	// The point of forcing: the call this drain gave up on is dropped, so its
	// caller finds out now instead of waiting on its own timeout.
	select {
	case err := <-callErr:
		require.Error(t, err, "the abandoned call should have been dropped")
	case <-time.After(5 * time.Second):
		t.Fatal("the abandoned call was left hanging")
	}
}

// Real time rather than synctest: the drain is bounded by a timer, but what it
// waits on is gRPC's own connection bookkeeping across a bufconn pipe, which
// synctest cannot see as idle.
func TestWithShutdownTimeoutClampsToFloor(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	svr, _ := wedgedServer(t,
		server.WithLogger(log),
		server.WithShutdownTimeout(0),
	)

	start := time.Now()
	require.NoError(t, svr.Stop(t.Context()))

	// Clamped, so an already-answered call still gets a moment to flush instead of
	// the drain becoming an immediate kill.
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	require.True(t, log.Contains(forcedMessage))
}

func TestStopHonoursContextDeadline(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	svr, _ := wedgedServer(t,
		server.WithLogger(log),
		// Far longer than the Context allows, so only the deadline can end this.
		server.WithShutdownTimeout(10*time.Second),
	)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	require.NoError(t, svr.Stop(ctx))
	require.Less(t, time.Since(start), 5*time.Second, "Stop ignored the Context deadline")

	// The budget never elapsed, so blaming it would send an operator to raise a
	// setting that was not binding.
	require.True(
		t,
		log.ContainsEntry(
			logger.LevelWarn,
			forcedMessage,
			tag.String("addr", "bufconn"),
			tag.String("cause", "stop context: context deadline exceeded"),
			tag.String("timeout", "10s"),
		),
		"the Context deadline should be reported as the cause, not the budget",
	)
}

func TestStopWithCancelledContextForcesImmediately(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	svr, _ := wedgedServer(t,
		server.WithLogger(log),
		server.WithShutdownTimeout(10*time.Second),
	)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.NoError(t, svr.Stop(ctx))
	require.True(t, log.Contains(forcedMessage), "a cancelled Context should force the drain")
	require.True(
		t,
		log.ContainsEntry(
			logger.LevelWarn,
			forcedMessage,
			tag.String("addr", "bufconn"),
			tag.String("cause", "stop context: context canceled"),
			tag.String("timeout", "10s"),
		),
		"a cancelled Context should be distinguishable from an expired budget",
	)
}

func TestStopDrainsCleanlyWhenNothingIsInFlight(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	svr, err := server.New(server.WithLogger(log))
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	require.Eventually(t, func() bool {
		return log.Contains("Starting the server")
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, svr.Stop(t.Context()))
	<-errCh

	// Guards against the forced path becoming unconditional: with nothing to wait
	// on, the drain must finish on its own.
	require.True(t, log.Contains(cleanMessage))
	require.False(t, log.Contains(forcedMessage))
}

func TestStopMarksNotServingBeforeDraining(t *testing.T) {
	t.Parallel()

	// A watcher keeps GracefulStop from ever finishing, so this exercises the
	// forced path by design; the point is that the status flip still lands first.
	svr, err := server.New(server.WithShutdownTimeout(100 * time.Millisecond))
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	go func() { _ = svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	stream, err := grpc_health_v1.NewHealthClient(conn).Watch(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)

	first, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, first.GetStatus())

	go func() { _ = svr.Stop(t.Context()) }()

	next, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, next.GetStatus())
}

func TestStopBeforeStart(t *testing.T) {
	t.Parallel()

	// The rollback path: a Start that failed before reaching Serve still gets
	// stopped, so Stop has to tolerate never having been started.
	svr, err := server.New()
	require.NoError(t, err)

	require.NoError(t, svr.Stop(t.Context()))
}

func (f failingCredentials) ServerOption() (grpc.ServerOption, error) {
	return nil, f.err
}

func (f failingCredentials) Encrypted() bool { return false }

func (c stubCredentials) ServerOption() (grpc.ServerOption, error) {
	return grpc.Creds(insecure.NewCredentials()), nil
}

func (c stubCredentials) Encrypted() bool { return c.secure }

func (c *recordingCodec) Marshal(v any) (mem.BufferSlice, error) {
	c.calls.Add(1)
	return c.delegate.Marshal(v)
}

func (c *recordingCodec) Unmarshal(data mem.BufferSlice, v any) error {
	c.calls.Add(1)
	return c.delegate.Unmarshal(data, v)
}

func (c *recordingCodec) Name() string { return c.delegate.Name() }

// wedgedServer starts a server whose unknown-service handler parks forever,
// standing in for one blocked on a backend that never answers, and returns once
// that handler has actually been entered: only then is there something for the
// drain to wait on. The handler is released during cleanup so it does not
// outlive the test.
//
// The returned channel carries the parked call's outcome. Start's return is not
// offered because a forced drain never produces one: GracefulStop waits for
// handlers holding the server's lock, so a handler that never returns wedges it,
// the forced Stop that needs the same lock, and Serve waiting on either to
// finish. Stop returning promptly is the guarantee, not teardown.
func wedgedServer(t *testing.T, sopts ...server.Option) (*server.Server, <-chan error) {
	t.Helper()

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	var once sync.Once
	handler := func(_ any, _ grpc.ServerStream) error {
		once.Do(func() { close(entered) })
		<-release

		return nil
	}

	svr, err := server.New(append(sopts, server.WithUnknownServiceHandler(handler))...)
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	go func() { _ = svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	t.Cleanup(func() { _ = conn.Close() })

	// Invoked from its own goroutine because it never returns until the drain
	// drops it.
	callErr := make(chan error, 1)
	go func() {
		callErr <- conn.Invoke(
			t.Context(),
			"/not.registered.Service/Method",
			&grpc_health_v1.HealthCheckRequest{},
			&grpc_health_v1.HealthCheckResponse{},
		)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was never entered")
	}

	return svr, callErr
}

func newBufConnClient(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	require.NoError(t, err)

	conn.Connect()
	return conn
}
