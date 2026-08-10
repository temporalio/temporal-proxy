package router

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

type (
	// Director selects the upstream for a request. Resolve receives the full
	// method, the namespace peeked from the first request message (empty when the
	// client sent no message), and the incoming metadata, and returns the Target to
	// forward over. A non-nil error aborts the stream and is returned to the caller
	// verbatim, so implementations should return a gRPC status error.
	Director interface {
		Resolve(ctx context.Context, method, namespace string, md map[string][]string) (Target, error)
	}

	// director maps the upstream name the Mux chooses to that upstream's
	// connection.
	director struct {
		conns    map[string]grpc.ClientConnInterface
		mux      *Mux
		reporter *Reporter
		logger   logger.Logger
	}
)

// NewDirector returns the Director that routes a request with mux and looks the
// chosen upstream up in conns. A nil log falls back to the default logger.
func NewDirector(
	mux *Mux,
	conns map[string]grpc.ClientConnInterface,
	rep *Reporter,
	log logger.Logger,
) Director {
	if log == nil {
		log = logger.Default()
	}

	return &director{
		conns:    conns,
		mux:      mux,
		reporter: rep,
		logger:   log.With(tag.Component("router")),
	}
}

// Resolve routes a request by matching it against the Mux and returning the
// Target for the resulting upstream. It fails with FailedPrecondition when
// no upstream matches (and no default is configured) and with Unavailable when
// the matched upstream has no connection. It records a decision metric on
// every call, plus a no_connection forwarding-error metric when the chosen
// upstream has no connection.
func (d *director) Resolve(
	ctx context.Context,
	method, namespace string,
	md map[string][]string,
) (Target, error) {
	upstream, outcome := d.mux.Switch(namespace, md)
	if outcome == OutcomeUnroutable {
		d.reporter.Decision(upstreamUnknown, OutcomeUnroutable)
		return Target{}, status.Error(codes.FailedPrecondition, "no upstream matched the request and no default is configured")
	}

	d.reporter.Decision(upstream, outcome)

	cc, ok := d.conns[upstream]
	if !ok {
		d.reporter.ForwardingError(upstream, reasonNoConnection)
		return Target{}, status.Errorf(codes.Unavailable, "router: no connection for upstream %q", upstream)
	}

	d.logger.Debug(
		"routing request",
		tag.String("method", method),
		tag.String("namespace", namespace),
		tag.String("upstream", upstream),
		tag.Stringer("outcome", outcome),
	)

	return Target{Upstream: upstream, Conn: cc}, nil
}
