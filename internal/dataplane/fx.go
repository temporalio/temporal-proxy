package dataplane

import (
	"context"

	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// Module provides a [Dataplane] from the assembled application and binds
// Start and Stop to the fx lifecycle. It replaces the router, proxy, and
// server modules: this is the only place in the graph that owns the
// gateway/proxy topology.
var Module = fx.Options(
	fx.Provide(newFromParams),
	fx.Invoke(func(lc fx.Lifecycle, d *Dataplane) {
		lc.Append(fx.Hook{OnStart: d.Start, OnStop: d.Stop})
	}),
)

// Params collects the fx-provided dependencies [New] needs. Types and Logger
// are optional: New falls back to the global proto registry and the default
// logger when neither is supplied.
type Params struct {
	fx.In
	Shutdowner fx.Shutdowner

	Context    context.Context
	Config     *config.Config
	Extractor  *protoutil.Extractor
	Translator *protoutil.Translator
	Pool       *connect.Pool
	Metrics    *metrics.Factory
	Allowlist  services.Allowlist
	Auth       auth.Authenticator
	Vault      *crypto.Vault

	Types  protoutil.Types `optional:"true"`
	Logger logger.Logger   `optional:"true"`
}

// newFromParams adapts fx-provided Params into options and constructs a
// Dataplane. Abort brings the application down on an unexpected stop, since
// lingering in a non-serving state is worse than exiting non-zero. An absent
// optional dependency arrives as nil, which the matching option ignores in
// favour of its default.
func newFromParams(p Params) (*Dataplane, error) {
	return New(p.Context, p.Config,
		WithExtractor(p.Extractor),
		WithTranslator(p.Translator),
		WithProtoTypes(p.Types),
		WithPool(p.Pool),
		WithMetrics(p.Metrics),
		WithAllowlist(p.Allowlist),
		WithAuth(p.Auth),
		WithVault(p.Vault),
		WithLogger(p.Logger),
		WithAbort(func(error) {
			_ = p.Shutdowner.Shutdown(fx.ExitCode(1))
		}),
	)
}
