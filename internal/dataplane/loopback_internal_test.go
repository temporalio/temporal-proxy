package dataplane

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// fakeWatch records how often the loopback was dialled and returns err from
// each call, standing in for the Watch exchange the check would otherwise run
// against its own listener.
type fakeWatch struct {
	calls    atomic.Int64
	deadline atomic.Value // time.Time, the deadline the check bounded the call with
	err      error
}

func (f *fakeWatch) watch(ctx context.Context, _ string) error {
	f.calls.Add(1)
	if dl, ok := ctx.Deadline(); ok {
		f.deadline.Store(dl)
	}

	return f.err
}

func TestLoopbackCheckStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want grpc_health_v1.HealthCheckResponse_ServingStatus
	}{
		{
			name: "a message arrives",
			err:  nil,
			want: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		{
			name: "a rejection is still an answer",
			err:  status.Error(codes.Unauthenticated, "no credential"),
			want: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		{
			name: "any other status is an answer too",
			err:  status.Error(codes.Internal, "something went wrong"),
			want: grpc_health_v1.HealthCheckResponse_SERVING,
		},
		{
			name: "a deadline means the chain never answered",
			err:  status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			want: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
		{
			name: "a bare context deadline means the same",
			err:  context.DeadlineExceeded,
			want: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
		{
			name: "an unreachable listener is not serving",
			err:  status.Error(codes.Unavailable, "connection refused"),
			want: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
		{
			name: "a transport failure carries no status at all",
			err:  errors.New("connection reset by peer"),
			want: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeWatch{err: tt.err}
			check := newLoopbackCheck(&config.Config{}, logger.NewNoopLogger())
			check.watch = fake.watch
			check.setAddr(testAddr("127.0.0.1:7233"))

			require.Equal(t, tt.want, check.Status(t.Context()))
			require.Equal(t, int64(1), fake.calls.Load(), "the check must dial its own listener")
		})
	}
}

func TestLoopbackCheckReportsServingBeforeItHasAnAddress(t *testing.T) {
	t.Parallel()

	fake := &fakeWatch{err: status.Error(codes.Unavailable, "connection refused")}
	check := newLoopbackCheck(&config.Config{}, logger.NewNoopLogger())
	check.watch = fake.watch

	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, check.Status(t.Context()))
	require.Zero(t, fake.calls.Load(), "nothing is bound yet, so there is nothing to dial")
}

func TestLoopbackCheckDisabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *config.Config
	}{
		{
			name: "an operator turned it off",
			cfg:  &config.Config{Health: config.Health{Enabled: new(false)}},
		},
		{
			name: "the gateway requires client certificates",
			cfg: &config.Config{Listen: config.ListenConfig{
				TLS: &config.TLSConfig{CA: "ca.crt", Cert: "server.crt", Key: "server.key"},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeWatch{err: status.Error(codes.Unavailable, "connection refused")}
			check := newLoopbackCheck(tt.cfg, logger.NewNoopLogger())
			check.watch = fake.watch
			check.setAddr(testAddr("127.0.0.1:7233"))

			require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, check.Status(t.Context()))
			require.Zero(t, fake.calls.Load(), "a disabled check must not dial")
		})
	}
}

func TestLoopbackCheckServerTLSStaysEnabled(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Listen: config.ListenConfig{
		TLS: &config.TLSConfig{Cert: "server.crt", Key: "server.key"},
	}}

	fake := &fakeWatch{}
	check := newLoopbackCheck(cfg, logger.NewNoopLogger())
	check.watch = fake.watch
	check.setAddr(testAddr("127.0.0.1:7233"))

	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, check.Status(t.Context()))
	require.Equal(t, int64(1), fake.calls.Load(), "a gateway presenting a certificate is still dialled")
}

func TestLoopbackCheckIntervalAndTimeout(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Health: config.Health{Interval: 2 * time.Second, Timeout: time.Second}}

	fake := &fakeWatch{}
	check := newLoopbackCheck(cfg, logger.NewNoopLogger())
	check.watch = fake.watch
	check.setAddr(testAddr("127.0.0.1:7233"))

	require.Equal(t, 2*time.Second, check.Interval())

	before := time.Now()
	check.Status(t.Context())

	deadline, ok := fake.deadline.Load().(time.Time)
	require.True(t, ok, "the check must bound the call with its timeout")
	require.WithinDuration(t, before.Add(time.Second), deadline, 250*time.Millisecond)
}

// testAddr is a stand-in net.Addr, so a check can be handed an address without
// binding anything.
type testAddr string

func (a testAddr) Network() string { return "tcp" }
func (a testAddr) String() string  { return string(a) }

var _ net.Addr = testAddr("")
