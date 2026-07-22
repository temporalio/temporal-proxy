package connect

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

type (
	// Conn is a [grpc.ClientConnInterface] that resolves its dial target per
	// call through a Resolver and fetches (lazily creating) the underlying
	// pooled connection through a ConnFactory. With a dynamic Resolver a single
	// Conn fronts many physical connections (e.g. one per namespace); with a
	// static Resolver it always resolves to the same one. Construct one with
	// NewConn.
	Conn struct {
		factory  ConnFactory
		resolver Resolver
	}

	// ConnFactory returns the pooled connection for a (key, target) pair,
	// creating it on first use. [Pool.ConnOrCreate] satisfies this signature:
	// the first argument is the logical cache key and the second is the dial
	// address.
	ConnFactory func(string, string, ...grpc.DialOption) (*grpc.ClientConn, error)

	// Resolver decides, per request, which connection a Conn should use. Resolve
	// returns the pool cache key, the dial target, and the dial options for that
	// connection. IsStatic reports whether the resolution is fixed for the life
	// of the Conn: a static resolver is dialed eagerly when the Conn is created;
	// a dynamic one is resolved lazily on every call.
	Resolver interface {
		IsStatic() bool
		Resolve(context.Context) (string, string, []grpc.DialOption, error)
	}

	// staticResolver resolves to a fixed address and options, ignoring the
	// request context. Its cache key equals its dial target, so two static
	// resolvers for the same address with different options would share one
	// pooled connection; that does not arise here because upstream hostPorts are
	// unique (enforced by config). Contrast the dynamic resolver in
	// internal/proxy, which folds the rendered server name into its key.
	staticResolver struct {
		addr string
		opts []grpc.DialOption
	}
)

// NewConn returns a Conn that resolves through r and dials through f. When r is
// static the pooled connection is created eagerly here, so a malformed target
// or bad dial option surfaces at construction rather than on the first request;
// the socket itself is not opened until first use (gRPC connects lazily). A
// dynamic resolver defers creation to the first call that resolves a given
// target.
func NewConn(f ConnFactory, r Resolver) (*Conn, error) {
	cc := &Conn{
		factory:  f,
		resolver: r,
	}

	// Static resolvers are eager loaded
	if r.IsStatic() {
		if _, err := cc.conn(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to initialize connection: %w", err)
		}
	}

	return cc, nil
}

// StaticResolver returns a Resolver that always resolves to hostPort with the
// given dial options. It reports IsStatic as true, so a Conn built from it is
// created eagerly and reuses a single pooled connection keyed by hostPort.
func StaticResolver(hostPort string, opts ...grpc.DialOption) Resolver {
	return &staticResolver{
		addr: hostPort,
		opts: opts,
	}
}

// Invoke resolves the connection for this call and forwards the unary RPC to
// it, satisfying [grpc.ClientConnInterface].
func (c *Conn) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	cc, err := c.conn(ctx)
	if err != nil {
		return err
	}

	return cc.Invoke(ctx, method, args, reply, opts...)
}

// NewStream resolves the connection for this call and opens the stream on it,
// satisfying [grpc.ClientConnInterface]. The target is resolved from ctx before
// any message is sent, so streaming and unary calls share one resolution path.
func (c *Conn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	cc, err := c.conn(ctx)
	if err != nil {
		return nil, err
	}

	return cc.NewStream(ctx, desc, method, opts...)
}

// conn resolves the request and returns the pooled connection for it.
func (c *Conn) conn(ctx context.Context) (*grpc.ClientConn, error) {
	key, target, opts, err := c.resolver.Resolve(ctx)
	if err != nil {
		return nil, err
	}

	return c.factory(key, target, opts...)
}

// IsStatic reports that a staticResolver never varies with the request.
func (r *staticResolver) IsStatic() bool {
	return true
}

// Resolve returns the fixed address as both the cache key and dial target,
// along with the configured dial options.
func (r *staticResolver) Resolve(context.Context) (string, string, []grpc.DialOption, error) {
	return r.addr, r.addr, r.opts, nil
}
