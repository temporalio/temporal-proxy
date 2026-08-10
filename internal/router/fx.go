package router

import (
	"fmt"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// Module is the fx module that provides the routing-and-forwarding pieces: a
// pass-through [google.golang.org/grpc/encoding.CodecV2], a [Mux] compiled from
// the routing configuration, and a [google.golang.org/grpc.StreamHandler]. The
// handler dials one connection per configured upstream from the shared
// [connect.Pool] (each unix socket path derived from that upstream's
// host:port), then routes every request to an upstream by matching it with the
// Mux. Requests are admitted through the [services.Allowlist] [config.Module]
// provides before any of that happens.
var Module = fx.Options(fx.Provide(
	Codec,
	func(p RouterParams) (grpc.StreamHandler, error) {
		conns := make(map[string]grpc.ClientConnInterface, len(p.Config.Upstreams))
		for i := range p.Config.Upstreams {
			upstream := &p.Config.Upstreams[i]
			sockPath, err := socket.UnixPath(upstream.Listen.HostPort)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve proxy socket path[%q]: %w", upstream.Name, err)
			}

			sock := "unix://" + sockPath
			conn, err := p.Pool.ConnOrCreate(sock, sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, fmt.Errorf("failed to create upstream client[%q]: %w", upstream.Name, err)
			}

			conns[upstream.Name] = conn
		}

		return Handler(
			NewDirector(p.Mux, conns, p.Reporter, p.Logger),
			p.Extractor,
			p.Allowlist,
			p.Reporter,
		), nil
	},
	func(c *config.Config, f *metrics.Factory) *Reporter {
		names := make([]string, 0, len(c.Upstreams))
		for i := range c.Upstreams {
			names = append(names, c.Upstreams[i].Name)
		}

		return NewReporter(f.ForSubsystem("router"), names)
	},
	func(c *config.Config) (*Mux, error) { return CompileMux(c.Routing) },
))

type (
	// RouterParams collects the fx-provided dependencies needed to build the
	// forwarding stream handler.
	RouterParams struct {
		fx.In

		Allowlist services.Allowlist
		Config    *config.Config
		Extractor *protoutil.Extractor
		Mux       *Mux
		Pool      *connect.Pool
		Reporter  *Reporter

		// Optional; falls back to [logger.Default].
		Logger logger.Logger `optional:"true"`
	}
)
