package dataplane

import (
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/router"
	"go.temporal.io/proxy/pkg/logger"
)

// newMux compiles the routing rules into the mux that picks an upstream for a
// request from its namespace and metadata. Anything that routes as the gateway
// does takes this one instance, so the two cannot disagree.
func newMux(cfg *config.Config) *router.Mux {
	return router.MuxFor(cfg.Routing)
}

// newDirector routes each stream to its upstream's forwarder, choosing the
// upstream with mux, and records every routing decision.
func newDirector(mux *router.Mux, ups *upstreams, rep *router.Reporter, log logger.Logger) router.Director {
	return router.NewDirector(mux, ups.handlers, rep, log)
}
