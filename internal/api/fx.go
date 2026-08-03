package api

import (
	"context"
	"fmt"

	"go.uber.org/fx"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/auth/outbound"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
)

// extensionKeyPrefix namespaces extension-server entries in the shared
// connection pool.
//
// A static resolver uses its dial address as the pool key, and Pool.ConnOrCreate
// returns the existing connection for a key while ignoring the options passed
// with it. Upstream hostPorts are unique among themselves, but nothing stops an
// extension server from sitting on the same host:port as an upstream, so without
// a distinct key the two would collapse onto whichever was dialed first and
// silently inherit its TLS settings and credentials.
const extensionKeyPrefix = "extension:"

// Module provides the pooled connection for every configured extension server,
// built when the provider runs rather than on first use, so a bad dial target
// surfaces at construction instead of on the first encryption call, and opened on
// start so an unreachable server (or one whose certificate this proxy will not
// accept) fails startup. Per-call credentials are still only exercised by a real
// request.
var Module = fx.Options(
	fx.Provide(func(p APIParams) (Connections, error) {
		// api.Module is wired ahead of server.Module, which is what validates the
		// config as a whole, so re-check here rather than dial whatever happened
		// to parse. Validating the list rather than each entry also covers its
		// collection invariants: duplicate names would otherwise collapse into a
		// single map entry, silently dropping a server.
		if err := p.Config.ExtensionServers.Validate(); err != nil {
			return nil, fmt.Errorf("invalid extension server configuration: %w", err)
		}

		out := make(Connections, len(p.Config.ExtensionServers))
		conns := make([]*connect.Conn, 0, len(p.Config.ExtensionServers))

		for i := range p.Config.ExtensionServers {
			es := &p.Config.ExtensionServers[i]

			conn, err := extensionConn(p.Pool, es)
			if err != nil {
				return nil, err
			}

			out[es.Name] = conn
			conns = append(conns, conn)
		}

		// Config rejects a templated extension server hostPort, so every one of
		// these is static and reachable now or not at all. An extension server backs
		// payload encryption, so serving without one means failing encrypted
		// traffic; fail startup instead.
		p.Lifecycle.Append(fx.StartHook(func(ctx context.Context) error {
			if err := connect.WaitReady(ctx, conns...); err != nil {
				return fmt.Errorf("extension server connection not ready: %w", err)
			}

			return nil
		}))

		return out, nil
	}),
)

type (
	// APIParams collects the fx-provided dependencies needed to reach the
	// configured extension servers. Pool is shared with the proxy's upstream
	// connections; [connect.Module] owns it and closes every pooled connection
	// on shutdown, which is why [KMS.Close] is a no-op.
	APIParams struct {
		fx.In

		Config    *config.Config
		Lifecycle fx.Lifecycle
		Pool      *connect.Pool
	}

	// Connections maps an extension server name to a connection to that server.
	// It carries no lifecycle: closing a connection is the owner's
	// responsibility, not the caller's.
	//
	// Callers get connections rather than finished clients because the two do not
	// correspond one-to-one: several keys may live on one extension server, so a
	// caller builds one [KMS] per key over the shared connection.
	Connections map[string]grpc.ClientConnInterface
)

// extensionConn builds the pooled connection for a single extension server.
// Config rejects a templated hostPort, so the target is always static: the
// resolver is fixed and [connect.NewConn] creates the connection here, which the
// module then opens on start.
//
// This is deliberately narrower than the equivalent upstream path in
// internal/proxy. There is no namespace translation, because an extension
// server is not a Temporal service and has no namespaces to rewrite, and no
// payload encryption interceptor, because an extension server is the thing that
// wraps DEKs; sealing its traffic with the vault it backs would be circular.
func extensionConn(pool *connect.Pool, s *config.ExtensionServer) (*connect.Conn, error) {
	var opts []grpc.DialOption

	cp, err := outbound.CredentialProviderFor(s.Credentials)
	if err != nil {
		return nil, fmt.Errorf("invalid credentials for extension server %q: %w", s.Name, err)
	}

	if cp != nil {
		opts = append(opts, outbound.DialOptions(cp)...)
	}

	// A nil TLS block resolves to a plaintext dialer, so this is safe unset.
	serverName := ""
	if s.Listen.TLS != nil {
		serverName = s.Listen.TLS.ServerName
	}

	cred, err := s.Listen.TLS.Dialer().DialOption(serverName)
	if err != nil {
		return nil, fmt.Errorf("failed to build credentials for extension server %q: %w", s.Name, err)
	}

	return connect.NewConn(
		func(key, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
			return pool.ConnOrCreate(extensionKeyPrefix+key, target, opts...)
		},
		connect.StaticResolver(s.Listen.HostPort, append(opts, cred)...),
	)
}
