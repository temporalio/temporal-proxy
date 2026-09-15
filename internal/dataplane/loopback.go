package dataplane

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

type (
	// loopbackCheck is the gateway's liveness check. Each interval it calls
	// grpc.health.v1.Health/Watch on its own listener, reads one message, and
	// reports whether the exchange was answered at all.
	//
	// Watch rather than Check, and that is the whole point: the gateway carries
	// stream interceptors and no unary ones, so the unary Check a probe runs
	// reaches the health handler without traversing the chain every forwarded
	// request goes through. Watch is a locally registered streaming method, so a
	// call to it runs that chain, and a wedge in it stops being invisible.
	//
	// It lives here rather than in internal/server, which is a general gRPC
	// server package and should not learn the dataplane's address or its routing.
	loopbackCheck struct {
		interval time.Duration
		timeout  time.Duration
		enabled  bool
		creds    credentials.TransportCredentials
		logger   logger.Logger

		// watch runs one exchange against the address given. It is a field so a
		// test can map an outcome to a status without a listener; production sets
		// watchOnce.
		watch func(ctx context.Context, target string) error

		// mu guards addr, which Start writes once the listener binds while the
		// health loop reads it from its own goroutine.
		mu   sync.Mutex
		addr net.Addr
	}
)

// newLoopbackCheck builds the check for cfg's gateway. It is constructed before
// anything is bound, because the gateway is built during New and the listener is
// created during Start, so the address arrives later via setAddr.
//
// The check turns itself off for a gateway that requires client certificates.
// That is read from the configuration rather than discovered by failing a
// handshake: the certificate the gateway presents is a server certificate and
// generally will not carry the client usage a handshake would need, so dialling
// anyway would report a wedge on every interval that was only ever the check's
// own configuration.
func newLoopbackCheck(cfg *config.Config, log logger.Logger) *loopbackCheck {
	if log == nil {
		log = logger.Default()
	}

	c := &loopbackCheck{
		interval: cfg.Health.CheckInterval(),
		timeout:  cfg.Health.CheckTimeout(),
		enabled:  cfg.Health.CheckEnabled(),
		creds:    loopbackCredentials(&cfg.Listen),
		logger:   log,
	}
	c.watch = c.watchOnce

	if c.enabled && mutualTLS(&cfg.Listen) {
		c.enabled = false
		c.logger.Info(
			"The gateway requires client certificates, so the liveness check reports SERVING without dialling it",
		)
	}

	return c
}

// Interval is how often the gateway refreshes its serving status, and so how
// often Status runs.
func (c *loopbackCheck) Interval() time.Duration { return c.interval }

// Status runs one exchange and maps its outcome. Any answer, including a
// rejection, means the chain ran end to end and reports SERVING: that is what
// keeps the check free of credentials, since it never needs to authenticate,
// only to be answered. A deadline means the chain did not answer, and an
// unreachable listener or a failure carrying no status at all means the gateway
// is not answering either; both report NOT_SERVING.
//
// A check with no address yet reports SERVING, so the wait for the upstreams
// during startup is never a false positive on a signal whose action is a
// restart. A disabled check does the same, leaving the gateway reporting SERVING
// on its usual cadence.
//
// No hysteresis is applied here. A probe's failureThreshold already debounces,
// and a second threshold underneath it makes the real detection latency hard to
// reason about.
func (c *loopbackCheck) Status(ctx context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
	if !c.enabled {
		return grpc_health_v1.HealthCheckResponse_SERVING
	}

	addr := c.target()
	if addr == "" {
		return grpc_health_v1.HealthCheckResponse_SERVING
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	err := c.watch(ctx, addr)
	if err == nil {
		return grpc_health_v1.HealthCheckResponse_SERVING
	}

	if answered(err) {
		c.logger.Debug("The liveness check was answered with an error, which still proves the request path is live",
			tag.Error(err))

		return grpc_health_v1.HealthCheckResponse_SERVING
	}

	c.logger.Warn("The gateway did not answer its own liveness check", tag.Error(err))

	return grpc_health_v1.HealthCheckResponse_NOT_SERVING
}

// setAddr hands the check the address the gateway is accepting on, which Start
// knows only once the listener binds.
func (c *loopbackCheck) setAddr(addr net.Addr) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.addr = addr
}

// target is the address to dial, empty until the gateway has bound one.
func (c *loopbackCheck) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.addr == nil {
		return ""
	}

	return c.addr.String()
}

// watchOnce opens a Watch stream against target, reads one message, and returns
// what the exchange ended with.
//
// The stream is cancelled as soon as that message arrives: health.Server
// registers a watcher per Watch stream, so one left open would leak an entry
// every interval. The connection is built and closed per run for the same
// reason, since a check runs every 30 seconds by default and one handshake on
// the loopback costs far less than a connection to keep correct across the
// gateway's whole lifetime.
func (c *loopbackCheck) watchOnce(ctx context.Context, target string) error {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(c.creds))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := grpc_health_v1.NewHealthClient(conn).Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err
	}

	_, err = stream.Recv()

	return err
}

// answered reports whether err is an answer from the gateway rather than a
// failure to reach it. A deadline is the wedge this check exists for, an
// unavailable listener is the gateway not accepting, and an error carrying no
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

// loopbackCredentials resolves what the check dials its own listener with.
//
// Certificate verification is skipped deliberately: the connection never leaves
// the process's own address, so verifying it establishes nothing.
func loopbackCredentials(listen *config.ListenConfig) credentials.TransportCredentials {
	if listen.Insecure || listen.TLS == nil {
		return insecure.NewCredentials()
	}

	return credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // dialling our own listener; see the doc comment.
		MinVersion:         tls.VersionTLS12,
	})
}

// mutualTLS reports whether the gateway requires each client to present a
// certificate, which is a listener's tls.ca being set.
func mutualTLS(listen *config.ListenConfig) bool {
	return !listen.Insecure && listen.TLS != nil && listen.TLS.CA != ""
}
