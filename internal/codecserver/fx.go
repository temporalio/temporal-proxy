package codecserver

import (
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// Module provides the codec server and binds it to the fx lifecycle. Include it
// unconditionally: a disabled codecServer block yields a nil [Server] and the
// lifecycle hook skips one, so the module is inert rather than something the
// call site has to gate.
//
// It depends on the dataplane's payload codec chain rather than building its
// own, which is both what makes a payload transform identically on either path
// and a necessity, since the encryption collectors register once per registry.
var Module = fx.Options(
	fx.Provide(newFromParams),
	fx.Invoke(func(lc fx.Lifecycle, s *Server) {
		if s == nil {
			return
		}

		lc.Append(fx.Hook{OnStart: s.Start, OnStop: s.Stop})
	}),
)

// Params collects the fx-provided dependencies the codec server needs. Every
// field is required; the codec server has no optional dependency, because a
// missing one would leave it either unauthenticated or unreported.
//
// Codecs comes from the dataplane rather than being built here, so the chain
// has one construction site. Conns is needed only to resolve an
// extension-server authenticator, and is harmlessly empty otherwise.
type Params struct {
	fx.In
	Shutdowner fx.Shutdowner

	Config  *config.Config
	Codecs  *proxy.Codecs
	Metrics *metrics.Factory
	Conns   api.Connections
	Logger  logger.Logger
}

// newFromParams builds the Server the configuration describes, or nil when the
// codec server is disabled. It warns rather than fails for the two
// configurations that are legal but probably unintended: no authentication,
// which config only permits on a loopback bind, and no encryption keys, which
// makes both routes identity transforms.
//
// Returns an error when the namespace override mapping is ambiguous, when the
// authenticator cannot be built, or when the TLS material will not load. Each
// is a startup failure rather than a runtime one.
func newFromParams(p Params) (*Server, error) {
	cfg := &p.Config.CodecServer
	if !cfg.Enabled {
		return nil, nil
	}

	log := p.Logger.With(tag.Component("codecserver"))

	overrides, err := NewOverrideMap(p.Config)
	if err != nil {
		return nil, err
	}

	opts := []Option{
		WithCORS(cfg.CORS.Origins, cfg.CORS.Credentials),
		// A per-namespace key policy means a payload that cannot be attributed to
		// a namespace must not be sealed at all: it would silently take the
		// default policy rather than the one the operator wrote for it.
		WithNamespaceRequired(len(p.Config.Encryption.Overrides) > 0),
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

	tlsCfg, err := cfg.Listen.Listener().TLSConfig()
	if err != nil {
		return nil, err
	}

	handler := Handler(
		p.Codecs,
		overrides,
		NewReporter(p.Metrics.ForSubsystem("codec_server")),
		opts...,
	)

	return NewServer(cfg.Listen.HostPort, handler, tlsCfg, log, func(error) {
		_ = p.Shutdowner.Shutdown(fx.ExitCode(1))
	}), nil
}
