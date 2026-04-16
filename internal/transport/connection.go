package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/yamux"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const dialTimeout = 5 * time.Second // NB: not too tight, don't want temporary issues to be a problem.

type (
	// ConnectionFactory creates new outbound network connections to a remote endpoint.
	ConnectionFactory interface {
		// Address returns the remote address this factory dials.
		Address() string
		// Done returns a channel that is closed once the factory has finished cleaning up.
		// Mux waits on this before signaling its own shutdown.
		Done() <-chan struct{}
		// NewConnection dials a new connection to the remote endpoint.
		NewConnection() (net.Conn, error)
	}

	// FactoryOptions defines options (all optional) for configuring ConnectionFactory instances.
	FactoryOptions struct {
		Log         log.Logger
		TLS         *tls.Config
		RetryPolicy backoff.RetryPolicy
	}

	// InboundFactory implements ConnectionFactory by running a local TCP server.
	// Each call to NewConnection blocks until a remote peer connects.
	// The factory's Done channel closes once the underlying listener has shut down,
	// so Mux waits for it before signaling its own shutdown.
	InboundFactory struct {
		ctx     context.Context
		lis     net.Listener
		adapter func(net.Conn) net.Conn
		log     log.Logger
		done    channel.ShutdownOnce
	}

	// OutboundFactory implements ConnectionFactory by dialing TCP connections
	// with exponential backoff retry.
	OutboundFactory struct {
		ctx         context.Context
		addr        string
		done        <-chan struct{}
		adapter     func(net.Conn) net.Conn
		log         log.Logger
		retryPolicy backoff.RetryPolicy
	}

	// ConnectionManager receives successfully established connections and their yamux
	// sessions, typically to register them for use by a ClientConn.
	ConnectionManager interface {
		// AddConnection registers a new connection and its associated yamux session.
		AddConnection(net.Conn, *yamux.Session)
	}

	connectionManagerFunc struct {
		fn func(net.Conn, *yamux.Session)
	}
)

// NewInboundFactory creates an InboundFactory that listens on addr over TCP.
// The listener is started immediately; the returned error is non-nil if the port cannot be bound.
func NewInboundFactory(ctx context.Context, addr string, opts *FactoryOptions) (*InboundFactory, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	f := &InboundFactory{
		ctx:     ctx,
		lis:     lis,
		adapter: func(c net.Conn) net.Conn { return c },
		log:     log.NewNoopLogger(),
		done:    channel.NewShutdownOnce(),
	}

	if opts == nil {
		f.log = log.With(f.log, tag.String("addr", addr))
		return f, nil
	}

	if opts.Log != nil {
		f.log = opts.Log
	}

	if opts.TLS != nil {
		f.adapter = func(conn net.Conn) net.Conn {
			return tls.Client(conn, opts.TLS)
		}
	}

	f.log = log.With(f.log, tag.String("addr", addr))

	// Ensure we Close when the context is canceled.
	go func() {
		<-ctx.Done()
		if err := f.Close(); err != nil {
			opts.Log.Error("Failed to close inbound mux: %w", tag.Error(err))
		}
	}()

	return f, nil
}

// NewOutboundFactory creates an OutboundConnectionFactory that dials addr over TCP.
// The returned factory's Done channel is always closed — OutboundConnectionFactory has no
// async cleanup of its own, so Mux is never blocked waiting for it.
func NewOutboundFactory(ctx context.Context, addr string, opts *FactoryOptions) *OutboundFactory {
	done := make(chan struct{})
	close(done) // closed so Done() doesn't block

	f := &OutboundFactory{
		ctx:     ctx,
		addr:    addr,
		done:    done,
		adapter: func(c net.Conn) net.Conn { return c },
		log:     log.NewNoopLogger(),
		retryPolicy: backoff.NewExponentialRetryPolicy(time.Second).
			WithBackoffCoefficient(1.5).
			WithMaximumInterval(30 * time.Second),
	}

	if opts == nil {
		f.log = log.With(f.log, tag.String("addr", f.addr))
		return f
	}

	if opts.Log != nil {
		f.log = opts.Log
	}

	if opts.TLS != nil {
		f.adapter = func(conn net.Conn) net.Conn {
			return tls.Client(conn, opts.TLS)
		}
	}

	if opts.RetryPolicy != nil {
		f.retryPolicy = opts.RetryPolicy
	}

	f.log = log.With(f.log, tag.String("addr", f.addr))
	return f
}

// ConnectionManagerFunc adapts a plain function into a ConnectionManager.
func ConnectionManagerFunc(fn func(net.Conn, *yamux.Session)) ConnectionManager {
	return &connectionManagerFunc{fn: fn}
}

// Address returns the local address the factory is listening on.
func (f *InboundFactory) Address() string {
	return f.lis.Addr().String()
}

// Done returns a channel that is closed once the listener has shut down.
func (f *InboundFactory) Done() <-chan struct{} {
	return f.done.Channel()
}

func (f *InboundFactory) Close() error {
	f.done.Shutdown()
	if err := f.lis.Close(); err != nil {
		return fmt.Errorf("failed to close listener: %w", err)
	}

	return nil
}

// NewConnection blocks until a remote peer connects, then returns the accepted connection.
// If the context is canceled while waiting, it returns the context error immediately.
func (f *InboundFactory) NewConnection() (net.Conn, error) {
	conn, err := f.lis.Accept()
	if f.ctx.Err() != nil {
		f.log.Info("Listener canceled due to shutdown")
		return nil, f.ctx.Err()
	}

	if err != nil {
		f.log.Fatal("Listener failed to accept connection", tag.Error(err))
		return nil, fmt.Errorf("failed to accept connection on %s: %w", f.Address(), err)
	}

	f.log.Info("Accepted connection on %s, remote: %s", tag.String(
		"remoteAddr",
		conn.RemoteAddr().String(),
	))
	return f.adapter(conn), nil
}

// Address returns the remote address this factory dials.
func (f *OutboundFactory) Address() string {
	return f.addr
}

// Done returns an already-closed channel; OutboundConnectionFactory has no async cleanup.
func (f *OutboundFactory) Done() <-chan struct{} {
	return f.done
}

// NewConnection dials addr, retrying with exponential backoff until a connection is
// established or the context is canceled.
func (f *OutboundFactory) NewConnection() (net.Conn, error) {
	var conn net.Conn

	dial := func() error {
		f.log.Debug("Attempting to dial")

		var err error
		conn, err = net.DialTimeout("tcp", f.addr, dialTimeout)
		if err != nil {
			return fmt.Errorf("timeout establishing connection to %s: %w", f.addr, err)
		}

		conn = f.adapter(conn)
		return nil
	}

	retryable := func(err error) bool {
		if f.ctx.Err() != nil {
			// Context was canceled, bail now.
			return false
		}

		f.log.Info("Failed to dial outbound")
		return true
	}

	if err := backoff.ThrottleRetry(dial, f.retryPolicy, retryable); err != nil {
		if f.ctx.Err() != nil {
			// Context is done, exit immediately.
			return nil, f.ctx.Err()
		}

		f.log.Error("Failed all attempts to dial", tag.Error(err))
		return nil, fmt.Errorf("failed all attepts to connect to %s: %w", f.addr, err)
	}

	return conn, nil
}

func (cm *connectionManagerFunc) AddConnection(conn net.Conn, sess *yamux.Session) {
	cm.fn(conn, sess)
}
