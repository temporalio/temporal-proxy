package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
)

const (
	defaultGRPCConnections   = 10
	defaultGrpcMuxStartDelay = time.Minute
	grpcMuxLogInterval       = time.Minute
)

type (
	// GRPCMux manages a pool of yamux sessions and exposes them as a dynamic set of Sessions.
	// The connection direction (inbound or outbound) is determined by the GRPCConfig.Kind
	// supplied to NewGRPCMux. Registered SessionListeners are notified whenever the session
	// set changes due to new connections or teardowns.
	GRPCMux struct {
		ctx              context.Context
		name             string
		done             channel.ShutdownOnce
		log              log.Logger
		connCount        int
		startDelay       time.Duration
		mux              *Mux
		idSequence       uint64
		init             sync.Once
		sessionsLock     sync.RWMutex
		sessions         map[string]*Session
		sessionBuilders  []SessionBuilder
		sessionListeners []SessionListener
	}

	// GRPCConfig holds the network configuration for a GRPCMux.
	GRPCConfig struct {
		// Address is the TCP address to connect to or listen on (e.g. "0.0.0.0:7233").
		Address string
		// Kind controls the connection direction: Inbound listens for incoming connections,
		// Outbound dials the remote address.
		Kind MuxKind
		// Server will be registered on each new Session.
		Server *grpc.Server
		// TLS, if non-nil, applies TLS to each connection.
		TLS *tls.Config
	}

	// GRPCMuxOption is a functional option for configuring a GRPCMux at construction time.
	GRPCMuxOption func(*GRPCMux)
)

// NewGRPCMux creates a GRPCMux that manages multiple yamux sessions.
// The connection direction is determined by cfg.Kind: Inbound listens on cfg.Address,
// Outbound dials it. Use GRPCMuxOption functions to override defaults.
func NewGRPCMux(ctx context.Context, name string, cfg GRPCConfig, opts ...GRPCMuxOption) (*GRPCMux, error) {
	m := &GRPCMux{
		ctx:        ctx,
		name:       name,
		log:        log.NewNoopLogger(),
		connCount:  defaultGRPCConnections,
		startDelay: defaultGrpcMuxStartDelay,
		done:       channel.NewShutdownOnce(),
		sessions:   make(map[string]*Session),
	}

	for _, opt := range opts {
		opt(m)
	}

	mfOpts := FactoryOptions{
		Log: m.log,
		TLS: cfg.TLS,
	}

	var mux *Mux
	var err error

	if cfg.Kind == Inbound {
		mux, err = NewInboundMux(ctx, name, cfg.Address, m.connCount, m, mfOpts)
		if err != nil {
			return nil, err
		}
	} else {
		mux, err = NewOutboundMux(ctx, name, cfg.Address, m.connCount, m, mfOpts)
		if err != nil {
			return nil, err
		}
	}

	m.mux = mux
	m.sessionBuilders = []SessionBuilder{
		registerYamuxObserverBuilder(name, m.log),
		func(ctx context.Context, id string, session *yamux.Session) {
			if cfg.Server == nil {
				return
			}

			go func() {
				for ctx.Err() == nil {
					_ = cfg.Server.Serve(session)
				}
			}()
		},
	}

	context.AfterFunc(m.ctx, m.close)
	return m, nil
}

// WithGRPCLogger sets the logger used by the GRPCMux.
func WithGRPCLogger(l log.Logger) GRPCMuxOption {
	return func(g *GRPCMux) { g.log = l }
}

// WithGRPCConnections sets the number of concurrent yamux sessions the GRPCMux maintains.
func WithGRPCConnections(n int) GRPCMuxOption {
	return func(g *GRPCMux) { g.connCount = n }
}

// WithGRPCSessionListeners registers listeners that are notified whenever the active session set changes.
func WithGRPCSessionListeners(ls ...SessionListener) GRPCMuxOption {
	return func(g *GRPCMux) { g.sessionListeners = ls }
}

// WithGRPCStartDelay overrides the delay that Start waits after launching the underlying Mux.
// The default is grpcMuxStartDelay (1 minute); pass a shorter value in tests.
func WithGRPCStartDelay(d time.Duration) GRPCMuxOption {
	return func(g *GRPCMux) { g.startDelay = d }
}

// AddConnection registers a new connection and yamux session. It implements ConnectionManager.
func (m *GRPCMux) AddConnection(conn net.Conn, sess *yamux.Session) {
	m.sessionsLock.Lock()
	defer m.sessionsLock.Unlock()
	if m.ctx.Err() != nil {
		return
	}

	key := fmt.Sprintf("%d", m.idSequence)
	m.idSequence++
	m.sessions[key] = NewSession(
		m.ctx,
		key,
		conn,
		sess,
		WithSessionBuilder(m.sessionBuilders...),
		WithSessionTeardown(func() {
			m.dropSession(key)
		}),
	)

	m.notify()
}

// Address returns the local or remote address this GRPCMux is bound to.
func (m *GRPCMux) Address() string {
	return m.mux.Address()
}

// Done returns a channel that is closed once the GRPCMux has fully shut down.
func (m *GRPCMux) Done() <-chan struct{} {
	return m.done.Channel()
}

// Start begins establishing connections and the session logging loop.
// It is safe to call Start multiple times; only the first call has any effect.
func (m *GRPCMux) Start() {
	m.init.Do(func() {
		m.mux.Start()

		go func() {
			ticker := time.NewTicker(grpcMuxLogInterval)
			defer ticker.Stop()

			// wait for tick or cancel
			select {
			case <-ticker.C:
			case <-m.ctx.Done():
				return
			}

			m.sessionsLock.RLock()
			details := make([]string, 0, len(m.sessions))
			for _, v := range m.sessions {
				details = append(details, v.String())
			}
			m.sessionsLock.RUnlock()

			m.log.Info(
				"GRPCMux status",
				tag.String("name", m.name),
				tag.NewBoolTag("shutdown", m.done.IsShutdown()),
				tag.String("sessions", strings.Join(details, ",")),
			)
		}()

		// Allow time to establish connections
		select {
		case <-time.After(m.startDelay):
		case <-m.ctx.Done():
		}
	})
}

func (m *GRPCMux) close() {
	m.mux.Wait() // Wait until mux has finished cleaning up.

	// Each session will unregister itself on Close, hence the lock.
	m.sessionsLock.Lock()
	defer m.sessionsLock.Unlock()
	for _, s := range m.sessions {
		if err := s.Close(); err != nil {
			m.log.Error("Failed to close session: %w", tag.Error(err))
		}
	}
}

func (m *GRPCMux) dropSession(key string) {
	m.sessionsLock.Lock()
	defer m.sessionsLock.Unlock()

	s := m.sessions[key]
	m.log.Info(
		"Dropping mux connection",
		tag.NewStringTag("id", key),
		tag.Error(s.State().Err),
		tag.NewInt("state", int(s.State().State)),
	)

	delete(m.sessions, key)
	m.notify()
}

// NB: must be called by code that holds the sessionsLock.
func (m *GRPCMux) notify() {
	for _, lis := range m.sessionListeners {
		lis.OnSessionsUpdated(m.sessions)
	}
}
