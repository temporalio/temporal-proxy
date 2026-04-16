package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// A custom scheme used by the resolver to identify connections.
const scheme = "tmprl"

type (
	// ClientConn implements grpc.ClientConnInterface with configurable load balancing
	// across a dynamic set of Sessions. Each active Session is registered as a resolver
	// endpoint; gRPC's built-in round-robin policy distributes calls across them.
	//
	// The set of endpoints is updated via OnSessionsUpdated as Sessions are added or
	// removed. The underlying gRPC connection is closed when the context is canceled.
	ClientConn struct {
		ctx      context.Context
		name     string
		client   grpc.ClientConnInterface
		resolver *manual.Resolver
		conns    map[string]func() (net.Conn, error)
		connMu   sync.RWMutex
	}

	connector interface {
		Connect()
	}
)

// NewClientConn creates a new ClientConn with the supplied DialOptions.
//
// Warning: do NOT supply WithContextDialer or WithResolvers options. They are not detectable and will interfere with
// this implementation.
func NewClientConn(ctx context.Context, name string, opts ...grpc.DialOption) (*ClientConn, error) {
	conn := &ClientConn{
		ctx:      ctx,
		name:     name,
		resolver: manual.NewBuilderWithScheme(scheme),
	}

	// Add our custom resolver and context dialer.
	// NB: If the caller supplies these, we're in trouble.
	dopts := make([]grpc.DialOption, len(opts)+2)
	dopts[0] = grpc.WithResolvers(conn.resolver)
	dopts[1] = grpc.WithContextDialer(conn.contextDialer)
	copy(dopts[2:], opts)

	client, err := grpc.NewClient(
		fmt.Sprintf("%s://%s", scheme, name),
		dopts...,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}
	conn.client = client
	if cn, ok := conn.client.(connector); ok {
		// NB: In the expected case (*grpc.ClientConn), this attempts immediate connection but is non-blocking.
		cn.Connect()
	}

	context.AfterFunc(ctx, func() { _ = conn.Close() })
	return conn, nil
}

// Close closes the underlying client connection.
func (c *ClientConn) Close() error {
	if cl, ok := c.client.(io.Closer); ok {
		return cl.Close()
	}

	return nil
}

// OnSessionsUpdated replaces the set of active Sessions used for load balancing.
// Each Session is registered as a resolver endpoint identified by its remote address.
// Passing an empty map removes all endpoints, causing subsequent calls to fail until
// new sessions are provided.
//
// Satisfies MuxSessionListener
func (c *ClientConn) OnSessionsUpdated(conns map[string]*Session) {
	if len(conns) == 0 {
		c.updateState(nil)
		return
	}

	connMap := make(map[string]func() (net.Conn, error), len(conns))
	for k, v := range conns {
		connMap[k] = v.Open
	}

	c.updateState(connMap)
}

// IsReady returns true when there is at least endpoint available and the context hasn't been canceled.
func (c *ClientConn) IsReady() bool {
	if c.ctx.Err() != nil {
		return false
	}

	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return len(c.conns) > 0
}

// String returns a human-readable description of the ClientConn and its current endpoints.
func (c *ClientConn) String() string {
	c.connMu.RLock()
	addrs := maps.Keys(c.conns)
	c.connMu.RUnlock()

	return fmt.Sprintf(
		"[clientconn name:%s, scheme:%s, addrs:{%s}]",
		c.name,
		scheme,
		strings.Join(slices.Collect(addrs), ","),
	)
}

// Invoke makes a unary RPC. It implements grpc.ClientConnInterface.
func (c *ClientConn) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	if c.client == nil {
		return errors.New("failed to invoke with uninitialized ClientConn")
	}

	return c.client.Invoke(ctx, method, args, reply, opts...)
}

// NewStream creates a new streaming RPC. It implements grpc.ClientConnInterface.
func (c *ClientConn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if c.client == nil {
		return nil, errors.New("failed to create new stream with uninitialized ClientConn")
	}

	return c.client.NewStream(ctx, desc, method, opts...)
}

func (c *ClientConn) contextDialer(_ context.Context, addr string) (net.Conn, error) {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	if fn, ok := c.conns[addr]; ok {
		return fn()
	}

	return nil, fmt.Errorf("unknown connection: %s", addr)
}

func (c *ClientConn) updateState(conns map[string]func() (net.Conn, error)) {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	state := resolver.State{
		Endpoints: make([]resolver.Endpoint, len(conns)),
	}

	idx := 0
	for addr := range conns {
		state.Endpoints[idx] = resolver.Endpoint{Addresses: []resolver.Address{{Addr: addr}}}
		idx++
	}

	c.conns = conns
	c.resolver.UpdateState(state)
}
