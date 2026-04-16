package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
)

const (
	Connected SessionState = iota // The session is established and healthy.
	Closed    SessionState = iota // The session has been closed, either locally or by the remote.
	Errored   SessionState = iota // The session encountered an error during a health check.
)

// Defines how often to Ping the yamux session.
const sessionHealthCheckInterval = time.Minute

// ErrSessionClosed is returned when attempting to accept connections on closed sessions.
var ErrSessionClosed = errors.New("session closed")

type (
	// Session wraps a yamux.Session with context-aware lifecycle management.
	// It periodically health-checks the underlying connection and transitions
	// its state to Errored or Closed as appropriate.
	//
	// Closing the parent context or calling Close both trigger a clean shutdown:
	// the yamux session and net.Conn are closed and any registered teardown
	// function is called.
	Session struct {
		ctx     context.Context
		cancel  context.CancelFunc
		id      string
		conn    net.Conn
		session *yamux.Session
		state   atomic.Pointer[SessionInfo]
	}

	// SessionFactory wraps a net.Conn in a yamux session for multiplexing.
	SessionFactory interface {
		// NewSession establishes a yamux session over the given connection.
		NewSession(net.Conn) (*yamux.Session, error)
	}

	sessionFactoryFunc struct {
		fn func(net.Conn) (*yamux.Session, error)
	}

	SessionListener interface {
		OnSessionsUpdated(map[string]*Session)
	}

	// SessionOption configures a Session at construction time.
	SessionOption interface {
		apply(*sessionOptions)
	}

	sessionOptions struct {
		builders []SessionBuilder
		teardown func()
	}

	sessionOptionFunc func(*sessionOptions)

	// SessionBuilder is called once during NewSession to perform additional setup
	// on the yamux session (e.g. registering a gRPC server). It receives the
	// session's context, ID, and the underlying yamux.Session.
	SessionBuilder func(context.Context, string, *yamux.Session)

	// SessionInfo holds a point-in-time snapshot of a Session's state.
	SessionInfo struct {
		State SessionState
		Err   error // non-nil when State is Errored
	}

	// SessionState represents the lifecycle state of a Session.
	SessionState byte
)

// NewSession creates a new Session with the given options. When the context is canceled, the yamux session and net.Conn
// are closed, along with all resources allocated by any SessionBuilders (caller must respect context).
func NewSession(ctx context.Context, id string, conn net.Conn, session *yamux.Session, opts ...SessionOption) *Session {
	ctx, cancel := context.WithCancel(ctx)
	s := &Session{
		ctx:     ctx,
		cancel:  cancel,
		id:      id,
		conn:    conn,
		session: session,
		state:   atomic.Pointer[SessionInfo]{},
	}

	s.state.Store(&SessionInfo{State: Connected})

	sopts := &sessionOptions{teardown: func() {}}
	for _, opt := range opts {
		opt.apply(sopts)
	}

	go s.startHealthChecking()
	for i := range sopts.builders {
		sopts.builders[i](ctx, id, session)
	}

	go s.wait(sopts.teardown)
	return s
}

func SessionFactoryFunc(f func(net.Conn) (*yamux.Session, error)) SessionFactory {
	return &sessionFactoryFunc{fn: f}
}

// WithSessionBuilder registers one or more SessionBuilders to be called during NewSession.
func WithSessionBuilder(b ...SessionBuilder) SessionOption {
	return sessionOptionFunc(func(so *sessionOptions) { so.builders = b })
}

// WithSessionTeardown registers a function to be called once the Session has shut down.
func WithSessionTeardown(f func()) SessionOption {
	return sessionOptionFunc(func(so *sessionOptions) { so.teardown = f })
}

// Open opens a new stream on the yamux session. Returns ErrSessionClosed if the session is no longer active.
func (s *Session) Open() (net.Conn, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSessionClosed, err)
	}

	return s.session.Open()
}

// State returns the current state of the session.
func (s *Session) State() *SessionInfo {
	return s.state.Load()
}

// Accept waits for and returns the next stream opened by the remote peer.
// Returns ErrSessionClosed if the session is no longer active.
func (s *Session) Accept() (net.Conn, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSessionClosed, err)
	}

	return s.session.Accept()
}

// Addr returns the local address of the underlying connection.
func (s *Session) Addr() net.Addr {
	return s.session.Addr()
}

// Close initiates a graceful shutdown of the session. It is safe to call multiple times.
func (s *Session) Close() error {
	s.cancel()
	return nil
}

// Done returns a channel that is closed when the session shuts down.
func (s *Session) Done() <-chan struct{} {
	return s.ctx.Done()
}

// IsClosed reports whether the session has been closed.
func (s *Session) IsClosed() bool {
	return s.ctx.Err() != nil
}

func (s *Session) String() string {
	return fmt.Sprintf("[session id:%s, state:%v, addr:%s]", s.id, s.state.Load(), s.conn.RemoteAddr())
}

func (s *Session) startHealthChecking() {
	// Periodically Ping the underlying session to make sure it stays alive, healthy, and connected.
	for !s.session.IsClosed() {
		current := s.state.Load()
		updated := &SessionInfo{State: Connected}

		if _, err := s.session.Ping(); err != nil {
			updated.State = Errored
			updated.Err = err
		}

		// Only update if it hasn't changed since we checked.
		s.state.CompareAndSwap(current, updated)

		select {
		case <-s.session.CloseChan():
		case <-time.After(sessionHealthCheckInterval):
		}
	}
}

func (s *Session) wait(teardown func()) {
	select {
	case <-s.session.CloseChan():
	case <-s.ctx.Done():
	}

	s.cancel()

	// Should we log errors at least?
	_ = s.session.Close()
	_ = s.conn.Close()
	s.state.Store(&SessionInfo{State: Closed, Err: s.state.Load().Err})
	teardown()
}

func (f *sessionFactoryFunc) NewSession(conn net.Conn) (*yamux.Session, error) {
	return f.fn(conn)
}

func (opt sessionOptionFunc) apply(s *sessionOptions) {
	opt(s)
}
