package router

import (
	"fmt"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
)

// Module is the fx module that provides the transparent-forwarding pieces: a
// pass-through [google.golang.org/grpc/encoding.CodecV2] and a
// [google.golang.org/grpc.StreamHandler]. The handler dials the proxy's unix
// socket, whose path is derived from the upstream host:port in configuration,
// and the connection is closed on shutdown. Consumers (the server module)
// depend on these by type without importing this package directly.
var Module = fx.Options(fx.Provide(
	Codec,
	func(p RouterParams) (grpc.StreamHandler, error) {
		upstream, err := p.Config.PrimaryUpstream()
		if err != nil {
			return nil, fmt.Errorf("failed to resolve upstream: %w", err)
		}

		sockPath, err := socket.UnixPath(upstream.Listen.HostPort)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve proxy socket path: %w", err)
		}

		conn, err := grpc.NewClient(
			"unix://"+sockPath,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create upstream client: %w", err)
		}

		p.Lifecycle.Append(fx.StopHook(func() error {
			return conn.Close()
		}))

		return Handler(conn), nil
	},
))

// RouterParams collects the fx-provided dependencies needed to build the
// forwarding stream handler.
type RouterParams struct {
	fx.In
	Lifecycle fx.Lifecycle

	Config *config.Config
}
