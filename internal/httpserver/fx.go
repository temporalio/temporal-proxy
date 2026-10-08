package httpserver

import (
	"net/http"

	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// Module provides the HTTP server and forces its construction, since nothing
// else depends on it. Include it unconditionally: with no routes contributed,
// it yields a nil [Server] and no lifecycle hook, so the module is inert.
var Module = fx.Options(
	fx.Provide(newFromParams),
	fx.Invoke(func(*Server) {}),
)

type (
	// Route mounts one route group's handler on the shared mux at Pattern, an
	// [http.ServeMux] pattern. Patterns from different groups must not overlap,
	// or building the server panics.
	Route struct {
		Pattern string
		Handler http.Handler
	}

	// Params collects the fx-provided dependencies the HTTP server needs.
	// Routes holds whatever the enabled route groups contributed, and may be
	// empty.
	Params struct {
		fx.In
		Shutdowner fx.Shutdowner

		Config    *config.Config
		Logger    logger.Logger
		Lifecycle fx.Lifecycle
		Routes    []Route `group:"http_routes"`
	}
)

// AsRoutes annotates constructor, which must return []Route and optionally an
// error, so its routes are mounted on the shared server. A disabled group
// returns none.
func AsRoutes(constructor any) any {
	return fx.Annotate(constructor, fx.ResultTags(`group:"http_routes,flatten"`))
}

// newFromParams builds the Server for the contributed routes and binds it to
// the fx lifecycle, or returns nil when no route group is enabled. Returns an
// error when the TLS material will not load.
func newFromParams(p Params) (*Server, error) {
	if len(p.Routes) == 0 {
		return nil, nil
	}

	cfg := &p.Config.HTTP.Listen

	tlsCfg, err := cfg.Listener().TLSConfig()
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	for _, r := range p.Routes {
		mux.Handle(r.Pattern, r.Handler)
	}

	svr := New(cfg.HostPort, mux, tlsCfg, p.Logger.With(tag.Component("httpserver")), func(error) {
		_ = p.Shutdowner.Shutdown(fx.ExitCode(1))
	})

	p.Lifecycle.Append(fx.Hook{OnStart: svr.Start, OnStop: svr.Stop})

	return svr, nil
}
