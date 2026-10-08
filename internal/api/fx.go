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
// connection pool. A static resolver keys the pool by dial address and
// Pool.ConnOrCreate ignores the options for an existing key, so without the
// prefix an extension server on an upstream's host:port would share that
// connection and silently inherit its TLS settings and credentials.
const extensionKeyPrefix = "extension:"

// Module provides the pooled connection for every configured extension server,
// built when the provider runs rather than on first use, so a bad dial target
// surfaces at construction instead of on the first call, and opened on
// start so an unreachable server (or one whose certificate this proxy will not
// accept) fails startup. Per-call credentials are still only exercised by a real
// request.
var Module = fx.Options(
	fx.Provide(func(p APIParams) (Connections, error) {
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
		// payload encryption or inbound auth, so serving without one means failing
		// that traffic; fail startup instead.
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
	// responsibility, not the caller's. Several keys may live on one extension
	// server, so a caller builds one [KMS] per key over the shared connection.
	Connections map[string]grpc.ClientConnInterface
)

// extensionConn builds the pooled connection for a single extension server.
// Config rejects a templated hostPort, so the target is always static: the
// resolver is fixed and [connect.NewConn] creates the connection here, which the
// module then opens on start. Unlike an upstream connection it installs no
// namespace translation or payload encryption interceptor; an extension server
// wraps DEKs, so sealing its traffic with the vault it backs would be circular.
func extensionConn(pool *connect.Pool, s *config.ExtensionServer) (*connect.Conn, error) {
	var opts []grpc.DialOption

	cp, err := outbound.CredentialProviderFor(s.Credentials)
	if err != nil {
		return nil, fmt.Errorf("invalid credentials for extension server %q: %w", s.Name, err)
	}

	if cp != nil {
		opts = append(opts, outbound.DialOptions(cp)...)
	}

	// Only a TLS block can override SNI, and the dialer ignores the name when it
	// is not verifying a peer, so this is safe unset.
	serverName := ""
	if s.Listen.TLS != nil {
		serverName = s.Listen.TLS.ServerName
	}

	cred, err := s.Listen.Dialer().DialOption(serverName)
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
