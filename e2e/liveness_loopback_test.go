package e2e

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

// wedge is an Authenticator that admits every caller until it is armed, after
// which it blocks until the stream it is deciding on is cancelled. The gateway
// runs it from a stream interceptor, so armed it stands in for any wedge in
// that chain: a deadlocked interceptor, or an extension server that stopped
// answering.
//
// The gateway carries no unary interceptors, so the unary Check these tests
// read a status with never reaches this at all. That asymmetry is the blind
// spot the loopback check closes, and it is what lets a test read a status from
// a gateway it has wedged.
type wedge struct {
	armed atomic.Bool
}

func (w *wedge) Authenticate(ctx context.Context, _ meta.Target, _ metadata.MD) error {
	if !w.armed.Load() {
		return nil
	}

	<-ctx.Done()

	return ctx.Err()
}

func (w *wedge) SecureHeaders() []string { return nil }

// TestEndToEndLivenessFlipsWhenTheStreamChainWedges drives the liveness check
// through the whole stack: a gateway that answers its own Health/Watch keeps
// reporting SERVING, one whose stream interceptor chain stops answering flips
// to NOT_SERVING, and it flips back once the chain frees up, since the check
// applies no hysteresis of its own.
//
// It is the regression test for the gap the check exists to close. Before it,
// the status was a transport heartbeat: the gateway below reports SERVING
// indefinitely while every forwarded request hangs.
func TestEndToEndLivenessFlipsWhenTheStreamChainWedges(t *testing.T) {
	t.Parallel()

	cfg := dataplanetest.Config(dataplanetest.NewUpstream(t))
	cfg.Health = config.Health{Interval: 100 * time.Millisecond, Timeout: 50 * time.Millisecond}

	blocker := &wedge{}
	f := dataplanetest.Start(t, cfg, dataplanetest.WithAuth(blocker))
	status := servingStatus(t, f)

	// Several refreshes with nothing wrong: a check that could not reach its own
	// listener would have flipped the status by now.
	require.Never(t, func() bool {
		return status() != grpc_health_v1.HealthCheckResponse_SERVING
	}, 500*time.Millisecond, 50*time.Millisecond, "a healthy gateway must keep reporting SERVING")

	blocker.armed.Store(true)
	require.Eventually(t, func() bool {
		return status() == grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}, 5*time.Second, 50*time.Millisecond, "a wedged stream chain must be reported as NOT_SERVING")

	blocker.armed.Store(false)
	require.Eventually(t, func() bool {
		return status() == grpc_health_v1.HealthCheckResponse_SERVING
	}, 5*time.Second, 50*time.Millisecond, "the status must recover once the chain answers again")
}

// servingStatus reads the gateway's process-wide serving status the way a
// Kubernetes grpc probe does, with the unary Check the health service answers
// without consulting the interceptor chain.
//
// A failed probe reports SERVICE_UNKNOWN rather than failing the test, because
// this is polled from testify's own goroutine: a poll still in flight as the
// test ends would otherwise report the shutdown it raced as a failure. Neither
// caller accepts SERVICE_UNKNOWN, so a probe that genuinely stops answering
// still fails, through the assertion that was waiting on it.
func servingStatus(t *testing.T, f *dataplanetest.Fixture) func() grpc_health_v1.HealthCheckResponse_ServingStatus {
	t.Helper()

	client := grpc_health_v1.NewHealthClient(f.Conn())

	return func() grpc_health_v1.HealthCheckResponse_ServingStatus {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true))
		if err != nil {
			return grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN
		}

		return resp.GetStatus()
	}
}
