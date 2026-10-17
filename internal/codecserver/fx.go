package codecserver

import (
	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/api"
	"go.temporal.io/proxy/internal/auth"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/httpserver"
	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
)

// Module contributes the codec server routes to the shared HTTP server in
// [httpserver.Module]. Include it unconditionally: a disabled codecServer
// block contributes no routes, so the module is inert.
var Module = fx.Provide(httpserver.AsRoutes(newFromParams))

// Params collects the fx-provided dependencies the codec server needs. Every
// field is required. Conns is used only to resolve an extension-server
// authenticator and may be empty otherwise.
type Params struct {
	fx.In

	Config  *config.Config
	Codecs  *proxy.Codecs
	Metrics *metrics.Factory
	Conns   api.Connections
	Logger  logger.Logger
}

// newFromParams builds the routes the configuration describes, mounted at the
// root, or returns none when the codec server is disabled. It warns rather than
// fails for the two configurations that are legal but probably unintended: no
// authentication, which config only permits on a loopback bind, and no
// encryption keys, which makes both routes identity transforms. Under a strict
// namespace binding both routes require the request to name a namespace.
// Returns an error when the namespace override mapping is ambiguous or when the
// authenticator cannot be built.
func newFromParams(p Params) ([]httpserver.Route, error) {
	cfg := &p.Config.HTTP.CodecServer
	if !cfg.Enabled {
		return nil, nil
	}

	log := p.Logger.With(tag.Component("codecserver"))

	overrides, err := NewOverrideMap(p.Config)
	if err != nil {
		return nil, err
	}

	strict := p.Config.Encryption.NamespaceBinding == config.NamespaceBindingStrict

	opts := []Option{
		WithCORS(cfg.CORS.Origins, cfg.CORS.Credentials),
		WithLogger(log),
		// A per-namespace key policy means a payload that cannot be attributed to
		// a namespace must not be sealed at all: it would silently take the
		// default policy rather than the one the operator wrote for it. Under a
		// strict binding neither route can act without one: a payload sealed
		// unnamed is refused everywhere, and an opened one has nothing to be
		// compared with.
		WithNamespaceRequired(len(p.Config.Encryption.Overrides) > 0 || strict),
		WithDecodeNamespaceRequired(strict),
	}

	if cfg.Auth != nil {
		authn, err := auth.For(cfg.Auth, p.Conns)
		if err != nil {
			return nil, err
		}

		opts = append(opts, WithAuth(authn))
	} else {
		// Configuration only permits this on a loopback bind.
		log.Warn("Codec server is running without authentication, which is only allowed on a loopback bind")
	}

	// An enabled codec server with no keys is an identity transform on both
	// routes. That is honest but almost certainly not what was meant.
	if p.Config.Encryption.Default == nil {
		log.Warn("Codec server is enabled but no encryption keys are configured, so payloads pass through unchanged")
	}

	handler := Handler(
		p.Codecs,
		overrides,
		NewReporter(p.Metrics.ForSubsystem("codec_server")),
		opts...,
	)

	return []httpserver.Route{{Pattern: "/", Handler: handler}}, nil
}
