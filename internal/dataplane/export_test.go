package dataplane

import (
	"net"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/internal/router"
	"go.temporal.io/proxy/internal/server"
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
