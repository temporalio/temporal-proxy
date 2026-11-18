package dataplane

import (
	"context"

	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/server"
	"go.temporal.io/proxy/pkg/crypto"
	"go.temporal.io/proxy/pkg/logger"
)

// Module provides a [Dataplane] from the assembled application and binds Start
// and Stop to the fx lifecycle. The Dataplane serves the gateway [server.Server]
// and opens its upstreams' connections; the module builds both, along with what
// it shares with the rest of the application:
//
//   - the codec chain, which the codec server applies too;
//   - the decode limiter that bounds decoded bytes across both;
//   - the request-path metric reporters, registered once per registry;
//   - the [Abort] the Dataplane reports an unexpected stop through, which
//     brings the application down, since lingering in a non-serving state is
//     worse than exiting non-zero.
var Module = fx.Options(
	fx.Provide(
		newDecodeLimiter,
		func(f *metrics.Factory, c *config.Config, v *crypto.Vault) (reporters, error) {
			return newReporters(f, c, v != nil)
		},
		newCodecs,
		newUpstreams,
		newGateway,
		newAbort,
		newDataplane,
	),
	fx.Invoke(func(lc fx.Lifecycle, d *Dataplane) {
		lc.Append(fx.Hook{OnStart: d.Start, OnStop: d.Stop})
	}),
)

// params collects the fx-provided dependencies a [Dataplane] is built from.
type params struct {
	fx.In

	Context   context.Context
	Config    *config.Config
	Gateway   *server.Server
	Upstreams *upstreams
	Logger    logger.Logger
	Abort     Abort
}

// newAbort shuts the application down with a non-zero exit code.
func newAbort(s fx.Shutdowner) Abort {
	return func(error) { _ = s.Shutdown(fx.ExitCode(1)) }
}
