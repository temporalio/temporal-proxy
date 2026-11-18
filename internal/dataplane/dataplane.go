package dataplane

import (
	"context"
	"net"
	"sync"

	"go.temporal.io/proxy/internal/server"
	"go.temporal.io/proxy/internal/transport/connect"
	"go.temporal.io/proxy/pkg/logger"
)

type (
	// Dataplane is the assembled request path. It is single-use: not restartable
	// after Stop.
	Dataplane struct {
		ctx       context.Context
		gateway   *server.Server
		hostPort  string
		ready     []*connect.Conn
		abort     Abort
		abortOnce sync.Once
		logger    logger.Logger

		mu       sync.Mutex
		addr     net.Addr
		lis      net.Listener
		stopping bool
	}

	// Abort is called at most once, from the goroutine that was serving, when the
	// gateway stops for a reason other than Stop. It must not block and must not
	// call back into the Dataplane. [Module] provides one that shuts the
	// application down; replace it with fx.Decorate to observe the stop instead.
	// A nil Abort drops the notification, leaving the log entry as the only record.
	Abort func(error)
)

// newDataplane binds the gateway to the upstream connections it has to open
// before serving. p.Context is long lived and drives the gateway's health check;
// the context passed to Start bounds startup only. Neither stops serving, which
// only Stop does.
func newDataplane(p params) *Dataplane {
	return &Dataplane{
		ctx:      p.Context,
		gateway:  p.Gateway,
		hostPort: p.Config.Listen.HostPort,
		ready:    p.Upstreams.ready,
		abort:    p.Abort,
		logger:   p.Logger,
	}
}

// Addr is the address the gateway is accepting on, nil before Start.
func (d *Dataplane) Addr() net.Addr {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.addr
}
