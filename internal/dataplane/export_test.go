package dataplane

import (
	"net"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
)

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

// Listener is the gateway listener Start bound, exported for tests that break
// it to drive an unexpected stop.
func (d *Dataplane) Listener() net.Listener {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.lis
}
