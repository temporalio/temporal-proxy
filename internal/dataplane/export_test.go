package dataplane

import (
	"net"
	"slices"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
)

// Listeners is the set of listeners Start bound, exported for tests that break
// serving out from under a running plane. Nothing a caller can reach does that:
// Stop and cancelling the serving context are both clean shutdowns, and a taken
// port fails before serving starts, so closing these is the only way into the
// unexpected-exit path.
func (d *Dataplane) Listeners() []net.Listener {
	d.mu.Lock()
	defer d.mu.Unlock()

	return slices.Clone(d.listeners)
}

// Reporters exposes the metric reporters built for one dataplane so tests can
// pin the exact metric surface they register.
type Reporters struct {
	Router     *router.Reporter
	Server     *server.Reporter
	Encryption *proxy.Reporter
}

// NewReporters builds the reporters for c against f, exported so tests can
// assert the metric names and labels a wired dataplane emits.
func NewReporters(f *metrics.Factory, c *config.Config, encryption bool) (*Reporters, error) {
	r, err := newReporters(f, c, encryption)
	if err != nil {
		return nil, err
	}

	return &Reporters{Router: r.router, Server: r.server, Encryption: r.encryption}, nil
}
