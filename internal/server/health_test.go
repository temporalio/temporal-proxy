package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/server"
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
