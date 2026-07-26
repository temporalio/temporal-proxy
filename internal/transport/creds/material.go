package creds

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
)

// material reads and parses the certificate and CA files a mode references
// exactly once, caching the result (or the error) for reuse across every
// DialOption call of a single Dialer. It is safe for concurrent use: the parsed
// certificate and CA pool are immutable after the first load. A rotated file on
// disk is not picked up until the process restarts, matching how a fixed-address
// upstream loads its material once at startup.
type material struct {
	opts *options
	mode Mode

	once   sync.Once
	cert   tls.Certificate
	caPool *x509.CertPool
	err    error
}

func (m *material) load() error {
	m.once.Do(func() {
		switch m.mode {
		case ModeMutualTLS:
			cert, err := tls.LoadX509KeyPair(m.opts.cert, m.opts.key)
			if err != nil {
				m.err = fmt.Errorf("failed to load client key pair: %w", err)
				return
			}

			pool, err := loadCAPool(m.opts.ca)
			if err != nil {
				m.err = err
				return
			}

			m.cert, m.caPool = cert, pool
		case ModeCustomCA:
			m.caPool, m.err = loadCAPool(m.opts.ca)
		}
	})

	return m.err
}

// loadCAPool reads a PEM-encoded CA certificate file and returns a cert pool
// containing it.
func loadCAPool(path string) (*x509.CertPool, error) {
	caBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA certificate: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("failed to parse CA file: %s", path)
	}

	return pool, nil
}
