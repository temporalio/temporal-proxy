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
	// Watch rather than Check, and that is the whole point: a server assembled
	// here carries stream interceptors and no unary ones, so the unary Check a
	// probe runs
	// reaches the health handler without traversing the chain every forwarded
	// request goes through. Watch is a locally registered streaming method, so a
	// call to it runs that chain, and a wedge in it stops being invisible.
	//
	// The call never leaves the process. It goes over an in-process listener
	// served by [newLoopbackServer]'s twin of the real server: the same
	// interceptor chain and the same health service, but with no address to
	// learn, no handshake to complete and no credential to present. That is what
	// lets the check behave the same whether the server is plaintext, TLS, or
	// mutual TLS, which last would otherwise leave it dialling a listener that
	// demands a client certificate it has no way to hold.
	//
	// What it does not cover is anything below the chain: accepting on the real
	// listener, and the handshake above that. Those belong to whatever probes the
	// server from outside, over a tcpSocket or grpc handler, rather than to
	// this.
	loopbackCheck struct {
		interval time.Duration
		timeout  time.Duration
		lis      *bufconn.Listener
		logger   logger.Logger
	}
)

// newLoopbackServer builds the server behind the in-process listener, from the
// same options as the real one so that it runs the same interceptor chain, and
// registering the same health service so both answer from one status.
//
// It is a second server rather than a second listener on the first because
// transport credentials belong to a [grpc.Server]: one server cannot accept on a
// listener that requires client certificates and on another that requires
// nothing. Nothing outside the process holds the listener, so the credential-free
// half is reachable only by the check.
func newLoopbackServer(o *options, hc *health.Server) *grpc.Server {
	svr := grpc.NewServer(o.serverOptions(grpc.Creds(insecure.NewCredentials()))...)
	grpc_health_v1.RegisterHealthServer(svr, hc)

	for _, register := range o.services {
		register(svr)
	}

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
// rejection, means the chain ran end to end and reports SERVING: that is what
// keeps the check free of credentials, since it never needs to authenticate,
// only to be answered. A deadline means the chain did not answer, and a listener
// that is not accepting or a failure carrying no status at all means the server
// is not answering either; both report NOT_SERVING.
//
// No hysteresis is applied here. A probe's failureThreshold already debounces,
// and a second threshold underneath it makes the real detection latency hard to
// reason about.
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
// across it. Dialling is the only thing it adds over watchOnce, which is what
// keeps watchOnce drivable from a test with no server behind it.
//
// The connection is built and closed per run rather than held open. A check runs
// every 30 seconds by default, and an in-process connection costs far less than
// one kept correct across the server's whole lifetime; dialling afresh also
// covers accepting, which a reused connection would skip, so a Serve goroutine
// that had stopped would otherwise go unnoticed.
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
// exchange ended with.
//
// The stream is cancelled as soon as that message arrives: health.Server
// registers a watcher per Watch stream, so one left open would leak an entry
// every interval. It takes the client rather than a listener so a test can drive
// it with a fake and assert that cancellation, which is otherwise unobservable
// from outside the process.
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
