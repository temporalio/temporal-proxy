package certreload

import (
	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/pkg/logger"
)

// Module provides the [creds.Reloader] that TLS sites register their material
// with, reporting through [NewReporter]. The reloader loads every registered
// credential when the app starts, so unreadable or invalid material fails
// startup, then polls for changes until the app stops.
var Module = fx.Options(fx.Provide(newReloader))

// Params holds the fx-injected dependencies of the reloader.
type Params struct {
	fx.In
	Lifecycle fx.Lifecycle

	Logger  logger.Logger
	Metrics *metrics.Factory
}

func newReloader(p Params) *creds.Reloader {
	r := creds.NewReloader(
		creds.WithReloaderLogger(p.Logger),
		creds.WithReporter(NewReporter(p.Metrics)),
	)

	p.Lifecycle.Append(fx.Hook{OnStart: r.Start, OnStop: r.Stop})

	return r
}
