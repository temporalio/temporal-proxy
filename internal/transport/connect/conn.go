package connect

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
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
	// static Resolver it always resolves to the same one. [WithConnections]
	// spreads calls across several connections per target. Construct one with
	// NewConn.
	Conn struct {
		factory  ConnFactory
		resolver Resolver
		size     int
		next     atomic.Uint64
	}

	// ConnOption configures a Conn at construction.
	ConnOption func(*Conn)

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
func NewConn(f ConnFactory, r Resolver, opts ...ConnOption) (*Conn, error) {
	cc := &Conn{
		factory:  f,
		resolver: r,
		size:     1,
	}

	for _, opt := range opts {
		opt(cc)
	}

	if cc.size < 1 {
		return nil, fmt.Errorf("connection pool size must be at least 1, got %d", cc.size)
	}

	// Static resolvers create their connections up front
	if r.IsStatic() {
		if _, _, err := cc.connections(context.Background()); err != nil {
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

// WithConnections spreads calls across n connections per target, taken in turn.
// Each is a separate pool entry, keyed by the resolved key with a "#i" suffix; at
// the default of 1 the key is used as is.
func WithConnections(n int) ConnOption {
	return func(c *Conn) { c.size = n }
}

// WaitReady opens conns and blocks until each is ready or ctx is done,
// whichever comes first. They are waited on concurrently and share ctx's
// deadline, and every target that never came up is reported, not only the
// first. When ctx has a deadline this returns just short of it, so an fx start
// hook reports the target names rather than fx's bare "context deadline
// exceeded".
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

// WaitReady opens the underlying connections and blocks until each is ready or
// ctx is done, whichever comes first. A static Conn dials on demand, so nothing
// is open until this runs or the first request arrives.
//
// A refused connection is not on its own fatal: gRPC retries with backoff, so a
// target still coming up passes as long as it answers before ctx expires. gRPC
// keeps the underlying dial error private, so one that never answers is reported
// by the state it was stuck in, wrapping ctx's error.
//
// A dynamic Conn holds no connection until a request resolves one, so this does
// nothing for it and callers can pass a mixed set of conns.
func (c *Conn) WaitReady(ctx context.Context) error {
	if !c.resolver.IsStatic() {
		return nil
	}

	// The factory hands back the connections built in NewConn, along with the
	// target to name any error.
	conns, target, err := c.connections(ctx)
	if err != nil {
		return err
	}

	// Connect moves a connection out of idle. It does not wait for the attempt to
	// begin, let alone finish, so every one is started before any is awaited and
	// they come up together.
	for _, cc := range conns {
		cc.Connect()
	}

	for _, cc := range conns {
		if err := awaitReady(ctx, cc, target); err != nil {
			return err
		}
	}

	return nil
}

// conn resolves the request and returns the next pooled connection for it.
func (c *Conn) conn(ctx context.Context) (*grpc.ClientConn, error) {
	key, target, opts, err := c.resolver.Resolve(ctx)
	if err != nil {
		return nil, err
	}

	i := 0
	if c.size > 1 {
		i = int((c.next.Add(1) - 1) % uint64(c.size))
	}

	return c.factory(c.member(key, i), target, opts...)
}

// connections resolves the request and returns every pooled connection for it,
// creating any that do not exist yet, along with the dial target.
func (c *Conn) connections(ctx context.Context) ([]*grpc.ClientConn, string, error) {
	key, target, opts, err := c.resolver.Resolve(ctx)
	if err != nil {
		return nil, "", err
	}

	conns := make([]*grpc.ClientConn, c.size)
	for i := range conns {
		if conns[i], err = c.factory(c.member(key, i), target, opts...); err != nil {
			return nil, "", err
		}
	}

	return conns, target, nil
}

// member is the pool key of connection i for key. A single connection keeps the
// key unchanged.
func (c *Conn) member(key string, i int) string {
	if c.size == 1 {
		return key
	}

	return key + "#" + strconv.Itoa(i)
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

// awaitReady blocks until cc is ready or ctx is done, naming target in any error.
func awaitReady(ctx context.Context, cc *grpc.ClientConn, target string) error {
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
