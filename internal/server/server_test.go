package server_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
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

func TestServerStartAndStop(t *testing.T) {
	t.Parallel()

	var statusCalls atomic.Int32
	hc := server.HealthCheckFunc(10*time.Millisecond, func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
		statusCalls.Add(1)
		return grpc_health_v1.HealthCheckResponse_NOT_SERVING
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

	require.Eventually(t, func() bool {
		resp, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
		return err == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return statusCalls.Load() > 0
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
