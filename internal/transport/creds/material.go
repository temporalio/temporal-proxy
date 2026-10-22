package creds

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/proxy/pkg/validation"
)

type (
	// files holds the raw bytes of the files a mode references. Fields a mode
	// does not use are nil.
	files struct{ cert, key, ca []byte }

	// snapshot is the parsed form of one set of files. It is immutable once
	// built, so it can be shared freely across goroutines.
	snapshot struct {
		cert         *tls.Certificate  // nil unless the mode presents a certificate
		pool         *x509.CertPool    // nil unless the mode has a CA
		sum          [sha256.Size]byte // over cert, key, and ca bytes in that order
		certNotAfter time.Time         // zero when no cert
		caNotAfter   time.Time         // earliest NotAfter in the CA bundle; zero when no CA
	}

	// reloadResult describes one call to [material.reload].
	reloadResult struct {
		attempted bool      // false only for "unchanged, last attempt succeeded"
		err       error     // nil on success or when not attempted
		snap      *snapshot // the snapshot now in use
	}

	// material reads the certificate and CA files a mode references, validates
	// and parses those bytes, and publishes the result as an immutable
	// [snapshot]. The first [material.load] builds the initial snapshot;
	// [material.reload] re-reads the files and swaps in a new snapshot only when
	// the bytes changed (or the previous attempt failed) and the new set is
	// valid, so a failed reload leaves the snapshot in use untouched. It is
	// safe for concurrent use.
	material struct {
		opts *options
		mode Mode

		once    sync.Once
		loadErr error
		cur     atomic.Pointer[snapshot]

		mu     sync.Mutex // serializes reload; guards failed
		failed bool
	}
)

// load builds the first snapshot. Its error is cached until a later reload
// succeeds, so repeated callers do not re-read the files on every call.
func (m *material) load() error {
	m.once.Do(func() {
		m.loadErr = m.reload().err
	})

	if m.current() != nil {
		return nil
	}

	return m.loadErr
}

// current returns the snapshot in use, or nil before the first successful load.
func (m *material) current() *snapshot {
	return m.cur.Load()
}

// reload re-reads the files. When the bytes match the snapshot in use and the
// last attempt succeeded it does nothing. Otherwise it validates and parses the
// new bytes: on success the snapshot is swapped, and on failure the old one is
// kept and the next call retries, even on identical bytes.
func (m *material) reload() reloadResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur := m.current()

	f, err := readFiles(m.opts, m.mode)
	if err != nil {
		return m.fail(cur, err)
	}

	if cur != nil && !m.failed && sumFiles(f) == cur.sum {
		return reloadResult{snap: cur}
	}

	next, err := parseMaterial(m.mode, f)
	if err != nil {
		return m.fail(cur, err)
	}

	m.cur.Store(next)
	m.failed = false

	return reloadResult{attempted: true, snap: next}
}

// fail records a failed attempt. Callers hold m.mu.
func (m *material) fail(cur *snapshot, err error) reloadResult {
	m.failed = true

	return reloadResult{attempted: true, err: err, snap: cur}
}

// readFiles reads each file the mode references exactly once. Read failures are
// reported per field as a [validation.Errors].
func readFiles(o *options, mode Mode) (files, error) {
	var (
		f    files
		errs validation.Errors
	)

	read := func(field, path, msg string, dst *[]byte) {
		data, err := os.ReadFile(path)
		if err != nil {
			errs = append(errs, validation.Error{Field: field, Message: fmt.Sprintf("%s: %v", msg, err)})
			return
		}

		*dst = data
	}

	switch mode {
	case ModeCustomCA:
		read("ca", o.ca, "failed to read PEM file", &f.ca)
	case ModeServerTLS:
		read("cert", o.cert, "failed to read PEM file", &f.cert)
		read("key", o.key, "failed to read PEM key file", &f.key)
	case ModeMutualTLS:
		read("cert", o.cert, "failed to read PEM file", &f.cert)
		read("key", o.key, "failed to read PEM key file", &f.key)
		read("ca", o.ca, "failed to read PEM file", &f.ca)
	}

	if len(errs) > 0 {
		return files{}, errs
	}

	return f, nil
}

// parseMaterial validates the bytes, then builds the snapshot: the key pair
// (which also proves the key matches the certificate) and the CA pool.
func parseMaterial(mode Mode, f files) (*snapshot, error) {
	if err := validateBytes(mode, f); err != nil {
		return nil, err
	}

	s := &snapshot{sum: sumFiles(f)}

	if mode == ModeServerTLS || mode == ModeMutualTLS {
		pair, err := tls.X509KeyPair(f.cert, f.key)
		if err != nil {
			return nil, fmt.Errorf("failed to load key pair: %w", err)
		}

		leaf, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse certificate: %w", err)
		}

		pair.Leaf = leaf
		s.cert, s.certNotAfter = &pair, leaf.NotAfter
	}

	if mode == ModeCustomCA || mode == ModeMutualTLS {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(f.ca) {
			return nil, errors.New("failed to parse CA file: no certificates found")
		}

		notAfter, err := earliestNotAfter(f.ca)
		if err != nil {
			return nil, err
		}

		s.pool, s.caNotAfter = pool, notAfter
	}

	return s, nil
}

// sumFiles hashes the three byte slices in order, length-prefixed so a byte
// moving between files changes the sum.
func sumFiles(f files) [sha256.Size]byte {
	h := sha256.New()

	for _, b := range [][]byte{f.cert, f.key, f.ca} {
		_ = binary.Write(h, binary.BigEndian, uint64(len(b)))
		_, _ = h.Write(b)
	}

	var sum [sha256.Size]byte
	copy(sum[:], h.Sum(nil))

	return sum
}

// earliestNotAfter returns the smallest NotAfter among the CERTIFICATE blocks
// in bundle.
func earliestNotAfter(bundle []byte) (time.Time, error) {
	var earliest time.Time

	for rest := bundle; len(rest) > 0; {
		var block *pem.Block

		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}

		if block.Type != "CERTIFICATE" {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to parse CA certificate: %w", err)
		}

		if earliest.IsZero() || cert.NotAfter.Before(earliest) {
			earliest = cert.NotAfter
		}
	}

	if earliest.IsZero() {
		return time.Time{}, errors.New("failed to parse CA file: no certificates found")
	}

	return earliest, nil
}
