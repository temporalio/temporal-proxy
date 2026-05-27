package server

import (
	"context"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"
)

type (
	// HealthCheck reports the server's serving status on a fixed cadence.
	// Interval controls how often Status is invoked to refresh the status
	// exposed via the gRPC health service.
	HealthCheck interface {
		Interval() time.Duration
		Status(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus
	}

	healthCheckFn struct {
		interval time.Duration
		statusFn func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus
	}
)

// HealthCheckFunc adapts a function into a [HealthCheck] that polls at the
// given interval.
func HealthCheckFunc(
	d time.Duration,
	fn func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus,
) HealthCheck {
	return &healthCheckFn{
		interval: d,
		statusFn: fn,
	}
}

func (f *healthCheckFn) Interval() time.Duration {
	return f.interval
}

func (f *healthCheckFn) Status(ctx context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
	return f.statusFn(ctx)
}

func defaultHealthCheck() HealthCheck {
	return HealthCheckFunc(30*time.Second, func(ctx context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
		return grpc_health_v1.HealthCheckResponse_SERVING
	})
}
