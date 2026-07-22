package connect

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
)

var (
	// ErrDuplicateKey is returned by Pool.Set when a connection is already
	// registered for the given key.
	ErrDuplicateKey = errors.New("key already defined")

	// ErrKeyNotFound is returned by Pool.Conn when no connection is registered
	// for the given key.
	ErrKeyNotFound = errors.New("no connection for key")
)

type (
	// Pool is a concurrency-safe set of gRPC client connections keyed by a
	// caller-supplied logical key, which is distinct from the dial target.
	// Callers that need two connections to the same dial target (e.g. the same
	// host:port with different TLS server names) must use different keys, or
	// they will collapse onto whichever connection was dialed first.
	// The zero value is not usable; create one with NewPool.
	Pool struct {
		mu        sync.RWMutex
		conns     map[string]*grpc.ClientConn
		closeOnce sync.Once
		closeErr  error
	}
)

// NewPool returns an empty Pool ready for use.
func NewPool() *Pool {
	return &Pool{
		conns: make(map[string]*grpc.ClientConn),
	}
}

// Conn returns the connection registered for key. It returns ErrKeyNotFound
// if no connection is registered for that key.
func (p *Pool) Conn(key string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	cn, ok := p.conns[key]
	p.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}

	return cn, nil
}

// ConnOrCreate returns the connection registered for key, creating and registering
// one with grpc.NewClient(target, opts...) when none exists yet. key is the
// logical cache key and target is the dial address; callers that need distinct
// connections to the same target (e.g. identical host:port with different TLS
// server names) must pass distinct keys. If callers race to create the same
// key, each constructs a client but only one connection is kept; the losers are closed and
// every caller receives the same *grpc.ClientConn.
func (p *Pool) ConnOrCreate(key, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if conn, _ := p.Conn(key); conn != nil {
		return conn, nil
	}

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s, %w", target, err)
	}

	if err := p.Set(key, conn); err != nil {
		if errors.Is(err, ErrDuplicateKey) {
			_ = conn.Close()   // lost the race; don't leak our dial
			return p.Conn(key) // return the connection that won
		}

		return nil, err
	}

	return conn, nil
}

// Set registers conn for key. It returns ErrDuplicateKey if a connection is
// already registered for that key, leaving the existing connection untouched.
func (p *Pool) Set(key string, conn *grpc.ClientConn) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.conns[key]; ok {
		return fmt.Errorf("%w: %s", ErrDuplicateKey, key)
	}

	p.conns[key] = conn
	return nil
}

// Close shuts down every connection in the pool, joining any errors. It runs
// at most once; subsequent calls return the same result without re-closing.
func (p *Pool) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		errs := make([]error, 0, len(p.conns))
		for _, conn := range p.conns {
			if err := conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		p.mu.Unlock()
		p.closeErr = errors.Join(errs...)
	})

	return p.closeErr
}
