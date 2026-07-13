package router

import (
	"fmt"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
)

// Module is the fx module that provides the transparent-forwarding pieces: a
// pass-through [google.golang.org/grpc/encoding.CodecV2] and a
// [google.golang.org/grpc.StreamHandler]. The handler obtains a connection to
// the proxy's unix socket, whose path is derived from the upstream host:port in
// configuration, from the shared [connect.Pool].
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

		conn, err := p.Pool.GetOrSet(
			"unix://"+sockPath,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create upstream client: %w", err)
		}

		return Handler(conn), nil
	},
))

// RouterParams collects the fx-provided dependencies needed to build the
// forwarding stream handler.
type RouterParams struct {
	fx.In

	Config *config.Config
	Pool   *connect.Pool
}
