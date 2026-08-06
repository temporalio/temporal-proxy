package server_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/internal/services"
)

func TestHealthCheckFunc(t *testing.T) {
	t.Parallel()

	t.Run("returns the configured interval", func(t *testing.T) {
		t.Parallel()

		hc := server.HealthCheckFunc(5*time.Second, func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
			return grpc_health_v1.HealthCheckResponse_SERVING
		})

		require.Equal(t, 5*time.Second, hc.Interval())
	})

	t.Run("delegates status checks to the provided function", func(t *testing.T) {
		t.Parallel()

		type contextKey string

		var called bool
		ctx := context.WithValue(context.Background(), contextKey("probe"), "value")
		hc := server.HealthCheckFunc(250*time.Millisecond, func(got context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
			called = true
			require.Equal(t, "value", got.Value(contextKey("probe")))

			return grpc_health_v1.HealthCheckResponse_NOT_SERVING
		})

		status := hc.Status(ctx)
		require.True(t, called)
		require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, status)
	})
}

func TestServerReportsNamedServiceHealth(t *testing.T) {
	t.Parallel()

	svr, err := server.New(
		server.WithHealthServices([]string{services.WorkflowService, services.OperatorService}),
	)
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	client := grpc_health_v1.NewHealthClient(conn)

	tests := []struct {
		name    string
		service string
		want    grpc_health_v1.HealthCheckResponse_ServingStatus
		wantErr codes.Code
	}{
		{name: "overall", service: "", want: grpc_health_v1.HealthCheckResponse_SERVING},
		{
			name:    "workflow service, what the SDK asks for",
			service: services.WorkflowService,
			want:    grpc_health_v1.HealthCheckResponse_SERVING,
		},
		{
			name:    "operator service",
			service: services.OperatorService,
			want:    grpc_health_v1.HealthCheckResponse_SERVING,
		},
		{
			name:    "a service the proxy does not expose",
			service: "temporal.server.api.adminservice.v1.AdminService",
			wantErr: codes.NotFound,
		},
	}

	// Deliberately not t.Parallel() here: these subtests share the one running
	// server above, and svr.Stop() below runs synchronously right after the
	// loop. A parallel subtest would be deferred past that Stop and race its
	// RPC against shutdown.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{Service: tt.service})
			if tt.wantErr != codes.OK {
				require.Equal(t, tt.wantErr, status.Code(err))
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, resp.GetStatus())
		})
	}

	require.NoError(t, svr.Stop(t.Context()))
	<-errCh
}

// TestServerNamedServiceHealthTracksPeriodicCheck pins the requirement that
// named statuses are re-read on every tick of the periodic health check
// rather than set once at construction. Hoisting the healthServices loop out
// of runHealthCheck's for{} body, so it only ran before the first tick, would
// still pass every other health test here (the default check reports SERVING
// throughout) but would fail this one, since the named status would never
// observe the flip to NOT_SERVING that happens after the first successful
// check.
func TestServerNamedServiceHealthTracksPeriodicCheck(t *testing.T) {
	t.Parallel()

	var notServing atomic.Bool
	hc := server.HealthCheckFunc(10*time.Millisecond, func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
		if notServing.Load() {
			return grpc_health_v1.HealthCheckResponse_NOT_SERVING
		}
		return grpc_health_v1.HealthCheckResponse_SERVING
	})

	svr, err := server.New(
		server.WithHealthCheck(hc),
		server.WithHealthServices([]string{services.WorkflowService}),
	)
	require.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)
	defer func() { _ = lis.Close() }()

	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), lis) }()

	conn := newBufConnClient(t, lis)
	defer func() { _ = conn.Close() }()

	client := grpc_health_v1.NewHealthClient(conn)

	require.Eventually(t, func() bool {
		resp, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{Service: services.WorkflowService})
		return err == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING
	}, time.Second, 10*time.Millisecond)

	// Flip what the check reports only after the named service has already
	// been observed as SERVING once, so passing this next assertion can only
	// be explained by a later tick re-setting the named status, not by a
	// value fixed at startup.
	notServing.Store(true)

	require.Eventually(t, func() bool {
		resp, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{Service: services.WorkflowService})
		return err == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, svr.Stop(t.Context()))
	<-errCh
}
