package pprofserver

import (
	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/api"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/httpserver"
	"go.temporal.io/proxy/pkg/logger"
)

// Module contributes the pprof routes to the shared HTTP server in
// [httpserver.Module]. Include it unconditionally: a disabled pprof block
// contributes no routes, so the module is inert.
var Module = fx.Provide(httpserver.AsRoutes(newFromParams))

// Params collects the fx-provided dependencies the pprof routes need. Every
// field is required. Conns is used only to resolve an extension-server
// authenticator and may be empty otherwise.
type Params struct {
	fx.In

	Config *config.Config
	Conns  api.Connections
	Logger logger.Logger
}
