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
)

type failingCredentials struct {
	err error
}

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

func (f failingCredentials) Server() (grpc.ServerOption, error) {
	return nil, f.err
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
