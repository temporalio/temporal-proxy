package auth

import (
	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/api"
	"go.temporal.io/proxy/internal/config"
)

// Module provides the inbound Authenticator the gateway's auth block selects.
// See [For] for how the selection works. The server adapts the Authenticator
// into a stream interceptor via StreamServerInterceptor.
var Module = fx.Options(fx.Provide(func(cfg *config.Config, conns api.Connections) (Authenticator, error) {
	return For(cfg.Auth, conns)
}))
