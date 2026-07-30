package connect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// readyMargin is how far short of its context's deadline [WaitReady] stops, so
// that the error it returns survives (see [WaitReady]). It only needs to cover
// unwinding back to the caller, so it is small enough to be irrelevant next to
// any sane start timeout.
const readyMargin = 100 * time.Millisecond

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
	// of the Conn: a static resolver has its connection created when the Conn is,
	// and opened up front by [Conn.WaitReady]; a dynamic one is resolved lazily on
	// every call.
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
// or bad dial option surfaces at construction rather than on the first request.
// Creating it is not the same as opening it: gRPC connects on demand, so no
// socket exists until [Conn.WaitReady] or the first request. A dynamic resolver
// defers creation to the first call that resolves a given target.
func NewConn(f ConnFactory, r Resolver) (*Conn, error) {
	cc := &Conn{
		factory:  f,
		resolver: r,
	}

	// Static resolvers create their connection up front
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

// WaitReady opens conns and blocks until each is ready or ctx is done,
// whichever comes first. They are waited on concurrently, so they share ctx's
// deadline rather than consuming it in turn, and every target that never came up
// is reported rather than only the first, so one unreachable address cannot mask
// another.
//
// When ctx has a deadline this stops just short of it. Callers are fx start
// hooks, and fx prefers its start context's error over what a hook returns
// (app.go, withTimeout), so a wait that runs to the deadline is reported as a
// bare "context deadline exceeded" and the target names are lost. Returning
// early is what keeps them.
func WaitReady(ctx context.Context, conns ...*Conn) error {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > 2*readyMargin {
		var cancel context.CancelFunc

		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-readyMargin))
		defer cancel()
	}

	errs := make([]error, len(conns))

	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() { errs[i] = conn.WaitReady(ctx) })
	}

	wg.Wait()
	return errors.Join(errs...)
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

// WaitReady opens the underlying connection and blocks until it is ready or ctx
// is done, whichever comes first. [NewConn] creates a static Conn's connection
// but grpc.NewClient only dials on demand, so nothing is open until this runs (or
// the first request arrives); this is what makes the connection real ahead of
// serving traffic.
//
// A refused connection is not on its own fatal: gRPC retries with backoff, so a
// target still coming up passes as long as it answers before ctx expires. gRPC
// keeps the underlying dial error private, so one that never answers is reported
// by the state it was stuck in, wrapping ctx's error.
//
// A dynamic Conn holds no connection until a request resolves one, so there is
// nothing to open and this does nothing. Callers can pass a mixed set of conns
// without sorting them first.
func (c *Conn) WaitReady(ctx context.Context) error {
	if !c.resolver.IsStatic() {
		return nil
	}

	// Resolved here rather than through conn so the target is in hand to name any
	// error; the factory hands back the connection built in NewConn.
	key, target, opts, err := c.resolver.Resolve(ctx)
	if err != nil {
		return err
	}

	cc, err := c.factory(key, target, opts...)
	if err != nil {
		return err
	}

	// Connect moves the connection out of idle. It does not wait for the attempt
	// to begin, let alone finish, so the state changes are awaited below.
	cc.Connect()

	for {
		switch state := cc.GetState(); state {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return fmt.Errorf("%s: connection is closed", target)
		default:
			// WaitForStateChange only reports false once ctx is done, so ctx.Err()
			// says whether the wait ran out of time or was cancelled (fx cancels the
			// start context when another hook fails).
			if !cc.WaitForStateChange(ctx, state) {
				return fmt.Errorf("%s: not ready (last state: %s): %w", target, state, ctx.Err())
			}
		}
	}
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
