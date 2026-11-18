package dataplane

import (
	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/server"
	"go.temporal.io/proxy/pkg/crypto"
)

// Module assembles the request path and binds it to the fx lifecycle. On start,
// every static upstream's connection is opened before the gateway binds, so an
// unreachable upstream fails startup instead of surfacing as request errors
// once the gateway is already serving. On stop, the gateway drains.
//
// Alongside the gateway [server.Server] it provides what the request path
// shares with the rest of the application:
//
//   - the codec chain, which the codec server applies too;
//   - the decode limiter that bounds decoded bytes across both;
//   - the request-path metric reporters, registered once per registry;
//   - the [Abort] the gateway reports an unexpected stop through, which brings
//     the application down, since lingering in a non-serving state is worse
//     than exiting non-zero.
var Module = fx.Options(
	fx.Provide(
		newDecodeLimiter,
		func(f *metrics.Factory, c *config.Config, v *crypto.Vault) (reporters, error) {
			return newReporters(f, c, v != nil)
		},
		newCodecs,
		newUpstreams,
		newMux,
		newDirector,
		newGateway,
		newAbort,
	),
	// Nothing else depends on the gateway, so force its construction.
	fx.Invoke(func(*server.Server) {}),
)

// Abort is called at most once, from the goroutine that was serving, when the
// gateway stops for a reason other than Stop. It must not block and must not
// call back into the gateway. [Module] provides one that shuts the application
// down; replace it with fx.Decorate to observe the stop instead. A nil Abort
// drops the notification, leaving the log entry as the only record.
type Abort func(error)

// newAbort shuts the application down with a non-zero exit code.
func newAbort(s fx.Shutdowner) Abort {
	return func(error) { _ = s.Shutdown(fx.ExitCode(1)) }
}
