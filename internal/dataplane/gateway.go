package dataplane

import (
	"fmt"

	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/auth"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/protoutil"
	"go.temporal.io/proxy/internal/router"
	"go.temporal.io/proxy/internal/server"
	"go.temporal.io/proxy/internal/services"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/pkg/logger"
)

// gatewayParams collects what the inbound gateway is built with. Reloader may
// be nil, which leaves the gateway with the TLS material it loaded first.
type gatewayParams struct {
	fx.In

	Config         *config.Config
	Extractor      *protoutil.Extractor
	Allowlist      services.Allowlist
	Auth           auth.Authenticator
	Upstreams      *upstreams
	RouterReporter *router.Reporter
	ServerReporter *server.Reporter
	Reloader       *creds.Reloader
	Logger         logger.Logger
}

// newGateway builds the inbound gateway: the routing mux from the matchers
// config preparation compiled, directing each stream to its upstream's
// forwarder, behind the interceptors every request passes. It binds nothing;
// the Dataplane serves it on Start.
func newGateway(p gatewayParams) (*server.Server, error) {
	cfg := p.Config

	handler := router.Handler(
		router.NewDirector(router.MuxFor(cfg.Routing), p.Upstreams.handlers, p.RouterReporter, p.Logger),
		p.Allowlist,
	)

	gateway, err := server.New(
		server.WithCredentials(cfg.Listen.Listener(creds.WithReloader(p.Reloader, "gateway"))),
		server.WithServerCodec(router.Codec()),
		// Health entries come from the allowlist, so what the gateway reports a
		// status for is exactly what it will forward.
		server.WithHealthServices(p.Allowlist.ServiceNames()...),
		server.WithLoopbackHealthCheck(cfg.Health.CheckInterval(), cfg.Health.CheckTimeout()),
		server.WithStreamInterceptor(p.ServerReporter.StreamInterceptor()),
		// Straight after the reporter, so a panic in any interceptor after it or in
		// the handler is recovered and still recorded as an Internal request.
		server.WithRecoveryHandler(p.Logger, p.ServerReporter.Panic),
		// Ahead of authentication: it resolves what the request is addressing, and
		// an authenticator decides on that as well as on the caller's credentials.
		server.WithStreamInterceptor(router.PeekInterceptor(p.Extractor, p.Allowlist)),
		server.WithStreamInterceptor(auth.StreamServerInterceptor(p.Auth, p.Logger)),
		server.WithUnknownServiceHandler(handler),
		server.WithLogger(p.Logger),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create server: %w", err)
	}

	return gateway, nil
}
