package connect

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
)

var (
	// ErrDuplicateHost is returned by Pool.Set when a connection is already
	// registered for the given host.
	ErrDuplicateHost = errors.New("host already defined")

	// ErrHostNotFound is returned by Pool.Conn when no connection is registered
	// for the given host.
	ErrHostNotFound = errors.New("no connection for host")
)

type (
	// Pool is a concurrency-safe set of gRPC client connections keyed by host.
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

// Conn returns the connection registered for host. It returns ErrHostNotFound
// if no connection is registered for that host.
func (p *Pool) Conn(host string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	cn, ok := p.conns[host]
	p.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrHostNotFound, host)
	}

	return cn, nil
}

// Set registers conn for host. It returns ErrDuplicateHost if a connection is
// already registered for that host, leaving the existing connection untouched.
func (p *Pool) Set(host string, conn *grpc.ClientConn) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.conns[host]; ok {
		return fmt.Errorf("%w: %s", ErrDuplicateHost, host)
	}

	p.conns[host] = conn
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
