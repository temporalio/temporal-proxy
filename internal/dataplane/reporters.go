package dataplane

import (
	"errors"
	"fmt"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
)

// reporters holds the metric reporters, each built once. A per-upstream build
// would register the same Prometheus collectors twice and panic.
type reporters struct {
	router     *router.Reporter
	server     *server.Reporter
	encryption *proxy.Reporter
}

// newReporters builds every reporter against f. Prometheus panics rather than
// erring on a duplicate registration, so recover and return an error: two
// dataplanes over one registry is a caller mistake, not a reason to die.
func newReporters(f *metrics.Factory, c *config.Config, encryption bool) (r *reporters, err error) {
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}

		// MustRegister panics with the error Register returned, so wrap it and
		// leave the caller an [prometheus.AlreadyRegisteredError] to match on.
		// Any other panic value can only be reported as it arrived, and names a
		// cause other than the ones guessed at below.
		cause, ok := rec.(error)
		if !ok {
			cause = errors.New(fmt.Sprint(rec))
		}

		r, err = nil, fmt.Errorf(
			"dataplane: registering metrics panicked, is another dataplane using this "+
				"registry, or a configured metrics tag label colliding with a collector's own: %w", cause,
		)
	}()

	names := make([]string, 0, len(c.Upstreams))
	for i := range c.Upstreams {
		names = append(names, c.Upstreams[i].Name)
	}

	// Only the request-scoped reporters take the tags; internal/kms builds its
	// own from the same factory and emits off a request, where no metadata is in
	// scope to read them from.
	tags := metrics.NewTags(c.Metrics.Tags)

	out := &reporters{
		router: router.NewReporter(f.ForSubsystem("router"), names, tags),
		server: server.NewReporter(f.ForSubsystem("server"), tags),
	}

	if encryption {
		out.encryption = proxy.NewReporter(f.ForSubsystem("encryption"), tags)
	}

	return out, nil
}
