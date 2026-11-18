package dataplane

import (
	"context"

	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/auth"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/protoutil"
	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/internal/services"
	"go.temporal.io/proxy/internal/transport/connect"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/pkg/crypto"
	"go.temporal.io/proxy/pkg/logger"
)

// Module provides a [Dataplane] and its [proxy.Codecs] from the assembled
// application and binds Start and Stop to the fx lifecycle. It also provides
// the [Abort] the Dataplane reports an unexpected stop through, which brings
// the application down, since lingering in a non-serving state is worse than
// exiting non-zero.
var Module = fx.Options(
	fx.Provide(newAbort, newDataplane),
	fx.Provide(func(d *Dataplane) *proxy.Codecs { return d.Codecs() }),
	fx.Invoke(func(lc fx.Lifecycle, d *Dataplane) {
		lc.Append(fx.Hook{OnStart: d.Start, OnStop: d.Stop})
	}),
)

// Params collects the fx-provided dependencies a [Dataplane] is built from.
// Vault is nil when no encryption keys are configured, and Reloader may be nil,
// which leaves every credential with the material it loaded first.
type Params struct {
	fx.In

	Context    context.Context
	Config     *config.Config
	Extractor  *protoutil.Extractor
	Translator *protoutil.Translator
	Types      protoutil.Types
	Pool       *connect.Pool
	Metrics    *metrics.Factory
	Allowlist  services.Allowlist
	Auth       auth.Authenticator
	Vault      *crypto.Vault
	Reloader   *creds.Reloader
	Logger     logger.Logger
	Abort      Abort
}

// newAbort shuts the application down with a non-zero exit code.
func newAbort(s fx.Shutdowner) Abort {
	return func(error) { _ = s.Shutdown(fx.ExitCode(1)) }
}
