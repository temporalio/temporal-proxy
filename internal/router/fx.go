package router

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
	"github.com/temporalio/temporal-proxy/pkg/match"
)

// Module is the fx module that provides the routing-and-forwarding pieces: a
// pass-through [google.golang.org/grpc/encoding.CodecV2], a [Mux] compiled from
// the routing configuration, and a [google.golang.org/grpc.StreamHandler]. The
// handler dials one connection per configured upstream from the shared
// [connect.Pool] (each unix socket path derived from that upstream's
// host:port), then routes every request to an upstream by matching it with the
// Mux.
var Module = fx.Options(fx.Provide(
	Codec,
	func(p RouterParams) (grpc.StreamHandler, error) {
		conns := make(map[string]*grpc.ClientConn, len(p.Config.Upstreams))
		for i := range p.Config.Upstreams {
			upstream := &p.Config.Upstreams[i]
			sockPath, err := socket.UnixPath(upstream.Listen.HostPort)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve proxy socket path[%q]: %w", upstream.Name, err)
			}

			conn, err := p.Pool.GetOrSet(
				"unix://"+sockPath,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create upstream client[%q]: %w", upstream.Name, err)
			}

			conns[upstream.Name] = conn
		}

		return Handler(
			&director{
				conns: conns,
				mux:   p.Mux,
			},
			p.Extractor,
		), nil
	},
	func(c *config.Config) (*Mux, error) {
		rules := make([]Rule, 0, len(c.Routing.Rules))
		for i, r := range c.Routing.Rules {
			p := r.Match.Namespace
			if p == "" {
				p = "*"
			}

			ns, err := match.Compile(p)
			if err != nil {
				return nil, fmt.Errorf("routing: rules[%d].match.namespace: %w", i, err)
			}

			meta := make(map[string]Matcher, len(r.Match.Metadata))
			seen := make(map[string]string, len(r.Match.Metadata))
			for k, v := range r.Match.Metadata {
				lk := strings.ToLower(k)
				if prev, ok := seen[lk]; ok {
					return nil, fmt.Errorf(
						"routing: rules[%d].match.metadata: keys %q and %q both map to %q when lowercased",
						i, prev, k, lk,
					)
				}

				seen[lk] = k
				m, err := match.Compile(v)
				if err != nil {
					return nil, fmt.Errorf("routing: rules[%d].match.metadata[%q]: %w", i, k, err)
				}

				meta[lk] = m
			}

			rules = append(rules, Rule{
				upstream: r.Upstream,
				ns:       ns,
				meta:     meta,
			})
		}

		return New(
			c.Routing.DefaultUpstream,
			c.Routing.SystemUpstream,
			rules...,
		), nil
	},
))

type (
	// RouterParams collects the fx-provided dependencies needed to build the
	// forwarding stream handler.
	RouterParams struct {
		fx.In

		Config    *config.Config
		Extractor *protoutil.Extractor
		Mux       *Mux
		Pool      *connect.Pool
	}

	// director is the [Director] used by the module's handler. It maps the
	// upstream name chosen by the Mux to that upstream's pooled connection.
	director struct {
		conns map[string]*grpc.ClientConn
		mux   *Mux
	}
)

// Resolve routes a request by matching it against the Mux and returning the
// connection for the resulting upstream. It fails with FailedPrecondition when
// no upstream matches (and no default is configured) and with Unavailable when
// the matched upstream has no connection.
func (d *director) Resolve(
	ctx context.Context,
	_, namespace string,
	md map[string][]string,
) (*grpc.ClientConn, error) {
	upstream := d.mux.Switch(namespace, md)
	if upstream == "" {
		return nil, status.Error(codes.FailedPrecondition, "no upstream matched the request and no default is configured")
	}

	cc, ok := d.conns[upstream]
	if !ok {
		return nil, status.Errorf(codes.Unavailable, "router: no connection for upstream %q", upstream)
	}

	return cc, nil
}
