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
)

const insecureMessage = "Running with insecure credentials. Configure TLS for production use."

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
