package proxy

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// ConnectionDialOptions returns the dial options that apply an upstream's
// connection block: the largest response it will accept and how idle
// connections are kept alive. Absent fields take their defaults, which match the
// Temporal Go SDK, so a worker sees the same limits through the proxy as on a
// direct connection.
func ConnectionDialOptions(c *config.ConnectionConfig) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(c.ResponseLimit()))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.KeepAlive.PingTime(),
			Timeout:             c.KeepAlive.PingTimeout(),
			PermitWithoutStream: true,
		}),
	}
}
