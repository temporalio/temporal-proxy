package server

import (
	"context"
	"errors"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

const (
	// loopbackBuffer sizes each half of one in-process connection. A health
	// exchange is a handful of small frames, so this only has to clear HTTP/2's
	// default window; the buffers are allocated per run and released with it.
	loopbackBuffer = 64 * 1024

	// loopbackName is what the check dials. The connection comes from the
	// listener's own dialer rather than from resolving a name, and passthrough is
	// what keeps grpc from handing this one to DNS.
	loopbackName = "passthrough:///loopback"
)

type (
	// loopbackCheck is a server's liveness check. Each interval it calls
	// grpc.health.v1.Health/Watch on the server it belongs to, reads one message,
	// and reports whether the exchange was answered at all.
	//
	// It must use Watch, not Check: the server carries stream interceptors and
	// no unary ones, so only a streaming call runs the chain every forwarded
	// request goes through. The call goes over an in-process listener served by
	// [newLoopbackServer], with no handshake or credential, so it behaves the
	// same whether the real server is plaintext, TLS, or mutual TLS. It does not
	// cover accepting on the real listener or the handshake; those belong to an
	// external probe.
	loopbackCheck struct {
		interval time.Duration
		timeout  time.Duration
		lis      *bufconn.Listener
		logger   logger.Logger
	}
)

// newLoopbackServer builds the server behind the in-process listener, from the
// same options as the real one so that it runs the same interceptor chain, and
// registering the same health service so both answer from one status. It is a
// separate server because transport credentials belong to a [grpc.Server], and
// this one requires none; nothing outside the process holds its listener, so
// only the check can reach it.
func newLoopbackServer(o *options, hc *health.Server) *grpc.Server {
	svr := grpc.NewServer(o.serverOptions(grpc.Creds(insecure.NewCredentials()))...)
	grpc_health_v1.RegisterHealthServer(svr, hc)

	return svr
}

// newLoopbackCheck builds the check that dials lis. Both durations are taken as
// given: [Server] is not the owner of their defaults, and the caller has already
// resolved them.
func newLoopbackCheck(lis *bufconn.Listener, interval, timeout time.Duration, log logger.Logger) *loopbackCheck {
	return &loopbackCheck{
		interval: interval,
		timeout:  timeout,
		lis:      lis,
		logger:   log,
	}
}

// Interval is how often the server refreshes its serving status, and so how
// often Status runs.
func (c *loopbackCheck) Interval() time.Duration { return c.interval }

// Status runs one exchange and maps its outcome. Any answer, including a
// rejection, means the chain ran end to end and reports SERVING, so the check
// never needs to authenticate. A deadline, a listener that is not accepting, or
// a failure carrying no status at all reports NOT_SERVING. No hysteresis is
// applied; debouncing is left to the probe's failureThreshold.
func (c *loopbackCheck) Status(ctx context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	err := c.probe(ctx)

	switch {
	case err == nil:
	case answered(err):
		c.logger.Debug("The liveness check was answered with an error, which still proves the request path is live",
			tag.Error(err))
	default:
		c.logger.Warn("The server did not answer its own liveness check", tag.Error(err))
	}

	return statusFor(err)
}

// probe opens a connection over the in-process listener and runs one exchange
// across it. The connection is built and closed per run so that each run also
// covers accepting; a reused connection would not notice a stopped Serve
// goroutine.
func (c *loopbackCheck) probe(ctx context.Context) error {
	conn, err := grpc.NewClient(
		loopbackName,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return c.lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	return watchOnce(ctx, grpc_health_v1.NewHealthClient(conn))
}

// watchOnce opens a Watch stream, reads one message, and returns what the
// exchange ended with. The stream is cancelled as soon as that message arrives:
// health.Server registers a watcher per Watch stream, so one left open would
// leak an entry every interval.
func watchOnce(ctx context.Context, client grpc_health_v1.HealthClient) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err
	}

	_, err = stream.Recv()

	return err
}

// statusFor maps what an exchange ended with onto a serving status. Any answer,
// including a rejection, means the chain ran end to end: that is what keeps the
// check free of credentials, since it never needs to authenticate, only to be
// answered. Silence is the failure.
func statusFor(err error) grpc_health_v1.HealthCheckResponse_ServingStatus {
	if err == nil || answered(err) {
		return grpc_health_v1.HealthCheckResponse_SERVING
	}

	return grpc_health_v1.HealthCheckResponse_NOT_SERVING
}

// answered reports whether err is an answer from the server rather than a
// failure to reach it. A deadline is the wedge this check exists for, an
// unavailable listener is the server not accepting, and an error carrying no
// gRPC status at all never came from a handler.
func answered(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.DeadlineExceeded, codes.Unavailable:
		return false
	default:
		return true
	}
}
