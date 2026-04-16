package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"golang.org/x/sync/semaphore"
)

// How long a stream can be dangling after calling Close on it.
const streamCloseTimeout = 30 * time.Second

const (
	Inbound  MuxKind = "server"
	Outbound MuxKind = "client"
)

type (
	// Mux manages a pool of outbound multiplexed connections to a remote endpoint.
	// It maintains up to numConns concurrent yamux sessions, replacing any that fail.
	//
	// Call Start to begin establishing connections, Done or Wait to observe shutdown.
	Mux struct {
		ctx       context.Context
		name      string
		log       log.Logger
		connFn    ConnectionFactory
		connMgr   ConnectionManager
		sessionFn SessionFactory

		connPermits *semaphore.Weighted
		done        channel.ShutdownOnce
		init        sync.Once
		initialized atomic.Bool
	}

	// MuxOption configures a Mux at construction time.
	MuxOption interface {
		apply(*Mux)
	}

	MuxKind string

	muxOptionFunc func(*Mux)
)

// NewMux creates a Mux that will maintain up to numConns concurrent connections.
// Values of numConns less than 1 are treated as 1.
// Use WithConnectionFactory, WithSessionFactory, and WithConnectionManager to inject dependencies.
func NewMux(ctx context.Context, name string, numConns int, opts ...MuxOption) (*Mux, error) {
	numConns = max(1, numConns) // Ensure we've got at least one

	mux := &Mux{
		ctx:         ctx,
		name:        name,
		log:         log.NewNoopLogger(),
		connPermits: semaphore.NewWeighted(int64(numConns)),
		done:        channel.NewShutdownOnce(),
	}

	for _, opt := range opts {
		opt.apply(mux)
	}

	return mux, nil
}

// NewInboundMux is a convenience constructor that creates a Mux backed by an InboundFactory.
// It listens on addr, accepts up to numConns concurrent yamux sessions, and delivers
// established connections to cm.
func NewInboundMux(
	ctx context.Context,
	name string,
	addr string,
	numConns int,
	cm ConnectionManager,
	opts FactoryOptions,
) (*Mux, error) {
	if opts.Log == nil {
		opts.Log = log.NewNoopLogger()
	}
	opts.Log = log.With(
		opts.Log,
		tag.String("component", "InboundMux"),
		tag.String("name", name),
	)

	f, err := NewInboundFactory(ctx, addr, &opts)
	if err != nil {
		return nil, err
	}

	return NewMux(
		ctx,
		name,
		numConns,
		WithLogger(opts.Log),
		WithConnectionFactory(f),
		WithConnectionManager(cm),
		WithSessionFactory(SessionFactoryFunc(func(conn net.Conn) (*yamux.Session, error) {
			cfg := yamux.DefaultConfig()
			cfg.Logger = yamuxLogger{logger: opts.Log}
			cfg.LogOutput = nil // Must be nil when Logger is set
			cfg.StreamCloseTimeout = streamCloseTimeout
			return yamux.Client(conn, cfg)
		})),
	)
}

// NewOutboundMux is a convenience constructor that creates a Mux backed by an OutboundFactory.
// It dials name as the remote address, maintains up to numConns concurrent yamux sessions,
// and delivers established connections to cm.
func NewOutboundMux(
	ctx context.Context,
	name string,
	addr string,
	numConns int,
	cm ConnectionManager,
	opts FactoryOptions,
) (*Mux, error) {
	if opts.Log == nil {
		opts.Log = log.NewNoopLogger()
	}
	opts.Log = log.With(
		opts.Log,
		tag.String("component", "OutboundMux"),
		tag.String("name", name),
	)

	return NewMux(
		ctx,
		name,
		numConns,
		WithLogger(opts.Log),
		WithConnectionFactory(NewOutboundFactory(ctx, addr, &opts)),
		WithConnectionManager(cm),
		WithSessionFactory(SessionFactoryFunc(func(conn net.Conn) (*yamux.Session, error) {
			cfg := yamux.DefaultConfig()
			cfg.Logger = yamuxLogger{logger: opts.Log}
			cfg.LogOutput = nil // Must be nil when Logger is set
			cfg.StreamCloseTimeout = streamCloseTimeout
			return yamux.Client(conn, cfg)
		})),
	)
}

// WithConnectionFactory sets the factory used to open new network connections.
func WithConnectionFactory(cf ConnectionFactory) MuxOption {
	return muxOptionFunc(func(m *Mux) { m.connFn = cf })
}

// WithSessionFactory sets the factory used to establish yamux sessions over connections.
func WithSessionFactory(sf SessionFactory) MuxOption {
	return muxOptionFunc(func(m *Mux) { m.sessionFn = sf })
}

// WithConnectionManager sets the manager that receives successfully established connections.
func WithConnectionManager(cm ConnectionManager) MuxOption {
	return muxOptionFunc(func(m *Mux) { m.connMgr = cm })
}

func WithLogger(l log.Logger) MuxOption {
	return muxOptionFunc(func(m *Mux) { m.log = l })
}

// Address returns the remote address this Mux connects to.
func (m *Mux) Address() string {
	return m.connFn.Address()
}

// Start begins the connection loop, which runs until the context passed to NewMux is canceled.
// It is safe to call Start multiple times; only the first call has any effect.
func (m *Mux) Start() {
	m.init.Do(func() {
		m.initialized.Store(true)

		var err error
		go func() {
			defer func() {
				m.log.Info("mux shutting down", tag.NewErrorTag("lastError", err))
				<-m.connFn.Done() // Wait for connection factory to clean up.
				m.done.Shutdown() // Anyone blocking on Done() can be notified now.
			}()

			for {
				err = m.connPermits.Acquire(m.ctx, 1)
				if err != nil {
					// NB: Only returns non-nil error if context is canceled.
					return
				}
				m.log.Info("Creating a new connection")

				var conn net.Conn
				conn, err = m.connFn.NewConnection()
				if err != nil {
					// If context is done, so are we.
					if m.ctx.Err() != nil {
						return
					}

					// failed, log and retry
					m.connPermits.Release(1)
					m.log.Error("Failed to create connection", tag.Error(err))
					continue
				}

				var session *yamux.Session
				session, err = m.sessionFn.NewSession(conn)
				if err != nil {
					// If context is done, so are we.
					if m.ctx.Err() != nil {
						return
					}

					_ = conn.Close()
					m.connPermits.Release(1)
					m.log.Error("Failed to establish session", tag.Error(err))
					continue
				}

				// Ensure the session is alive
				_, err := session.Ping()
				if err != nil {
					// If context is done, so are we.
					if m.ctx.Err() != nil {
						return
					}

					tags := []tag.Tag{
						tag.Error(err),
						tag.String("remoteAddr", session.RemoteAddr().String()),
					}

					if errors.Is(err, yamux.ErrConnectionWriteTimeout) {
						m.log.Info("Timeout connecting to remote host", tags...)
					} else if errors.Is(err, io.EOF) {
						m.log.Info("Remote disconnected immediately", tags...)
					} else {
						m.log.Warn("Unknown request error", tags...)
					}

					_ = session.Close()
					_ = conn.Close()
					m.connPermits.Release(1)
					continue
				}

				m.connMgr.AddConnection(conn, session)
			}
		}()
	})
}

// Done returns a channel that is closed once the Mux has fully shut down.
func (m *Mux) Done() <-chan struct{} {
	return m.done.Channel()
}

// Wait blocks until the Mux has fully shut down. If Start was never called, Wait returns immediately.
func (m *Mux) Wait() {
	// If this is called before Start, we need to signal to anyone waiting on Done() regardless.
	if !m.initialized.Load() {
		m.done.Shutdown()
	}

	// Wait for everything to finish (in the expected case, this is closed in Start)
	<-m.done.Channel()
}

// apply implements MuxOption.
func (f muxOptionFunc) apply(m *Mux) { f(m) }
