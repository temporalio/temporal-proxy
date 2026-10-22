package creds_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"go.temporal.io/proxy/internal/transport/creds"
)

// pki is a CA plus a leaf signed by it, written to disk. The leaf carries both
// server and client auth usages and DNSNames=localhost.
type pki struct {
	caFile, certFile, keyFile string
	serial                    *big.Int
	ca                        *x509.Certificate
	pool                      *x509.CertPool
}

func TestListener_NewHandshakeSeesReloadedCert(t *testing.T) {
	t.Parallel()

	live, next := newPKI(t, t.TempDir()), newPKI(t, t.TempDir())
	l := creds.NewListener(creds.WithCertificate(live.certFile, live.keyFile))
	addr := serveTLS(t, listenerConfig(t, l))
	client := &tls.Config{RootCAs: poolOf(live, next), ServerName: "localhost", MinVersion: tls.VersionTLS12}

	serial, err := handshake(addr, client)
	require.NoError(t, err)
	require.Equal(t, live.serial, serial)

	rotate(t, next, live)
	reload(t, l)

	serial, err = handshake(addr, client)
	require.NoError(t, err)
	require.Equal(t, next.serial, serial)
}

func TestListener_ReloadedClientCA(t *testing.T) {
	t.Parallel()

	server, client := newPKI(t, t.TempDir()), newPKI(t, t.TempDir())
	l := creds.NewListener(creds.WithCA(server.caFile), creds.WithCertificate(server.certFile, server.keyFile))
	addr := serveTLS(t, listenerConfig(t, l))

	cfg := &tls.Config{
		Certificates: []tls.Certificate{client.keyPair(t)},
		RootCAs:      server.pool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
	}

	_, err := handshake(addr, cfg)
	require.Error(t, err)

	copyFile(t, client.caFile, server.caFile)
	reload(t, l)

	serial, err := handshake(addr, cfg)
	require.NoError(t, err)
	require.Equal(t, server.serial, serial)
}

func TestListener_NegotiatesH2(t *testing.T) {
	t.Parallel()

	p := newPKI(t, t.TempDir())
	l := creds.NewListener(creds.WithCertificate(p.certFile, p.keyFile))
	addr := serveTLS(t, listenerConfig(t, l))

	for _, proto := range []string{"h2", "http/1.1"} {
		conn, err := tls.Dial("tcp", addr, &tls.Config{
			RootCAs:    p.pool,
			ServerName: "localhost",
			NextProtos: []string{proto},
			MinVersion: tls.VersionTLS12,
		})
		require.NoError(t, err)
		require.Equal(t, proto, conn.ConnectionState().NegotiatedProtocol)
		require.NoError(t, conn.Close())
	}

	opt, err := l.ServerOption()
	require.NoError(t, err)

	dial, err := creds.NewDialer(creds.WithCA(p.caFile)).DialOption("localhost")
	require.NoError(t, err)
	require.NoError(t, checkHealth(t, serveHealth(t, opt), dial))
}

func TestListener_HTTPServerServesTLS(t *testing.T) {
	t.Parallel()

	p := newPKI(t, t.TempDir())
	cfg := listenerConfig(t, creds.NewListener(creds.WithCertificate(p.certFile, p.keyFile)))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &http.Server{
		TLSConfig:         cfg,
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "ok")
		}),
	}

	served := make(chan error, 1)
	go func() { served <- srv.ServeTLS(ln, "", "") }()
	t.Cleanup(func() {
		_ = srv.Close()
		require.ErrorIs(t, <-served, http.ErrServerClosed)
	})

	hc := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: p.pool, MinVersion: tls.VersionTLS12},
	}}
	t.Cleanup(hc.CloseIdleConnections)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+localhost(ln.Addr()), nil)
	require.NoError(t, err)

	resp, err := hc.Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "ok", string(body))
}

func TestListener_ConcurrentHandshakesDuringReload(t *testing.T) {
	t.Parallel()

	a, b := newPKI(t, t.TempDir()), newPKI(t, t.TempDir())
	live := pki{certFile: filepath.Join(t.TempDir(), "cert.pem")}
	live.keyFile = filepath.Join(filepath.Dir(live.certFile), "key.pem")
	rotate(t, a, live)

	l := creds.NewListener(creds.WithCertificate(live.certFile, live.keyFile))
	addr := serveTLS(t, listenerConfig(t, l))
	client := &tls.Config{RootCAs: poolOf(a, b), ServerName: "localhost", MinVersion: tls.VersionTLS12}

	var (
		wg   sync.WaitGroup
		done = make(chan struct{})
	)

	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}

				serial, err := handshake(addr, client)
				if err != nil {
					t.Errorf("handshake: %v", err)
					return
				}

				if serial.Cmp(a.serial) != 0 && serial.Cmp(b.serial) != 0 {
					t.Errorf("unexpected serial %s", serial)
					return
				}
			}
		})
	}

	for i := range 20 {
		src := a
		if i%2 == 0 {
			src = b
		}

		rotate(t, src, live)
		reload(t, l)
	}

	close(done)
	wg.Wait()
}

func TestDialer_EveryDialOptionSeesReloadedCA(t *testing.T) {
	t.Parallel()

	trusted, server := newPKI(t, t.TempDir()), newPKI(t, t.TempDir())
	addr := serveHealth(t, serverOption(t, server))
	d := creds.NewDialer(creds.WithCA(trusted.caFile))

	named, err := d.DialOption("localhost")
	require.NoError(t, err)
	require.Equal(t, codes.Unavailable, status.Code(checkHealth(t, addr, named)))

	unnamed, err := d.DialOption("")
	require.NoError(t, err)

	copyFile(t, server.caFile, trusted.caFile)
	dialerReload(t, d)

	after, err := d.DialOption("localhost")
	require.NoError(t, err)

	for name, opt := range map[string]grpc.DialOption{"named": named, "unnamed": unnamed, "after": after} {
		require.NoError(t, checkHealth(t, addr, opt), name)
	}
}

func TestDialer_NewConnectionPresentsReloadedClientCert(t *testing.T) {
	t.Parallel()

	server, a, b := newPKI(t, t.TempDir()), newPKI(t, t.TempDir()), newPKI(t, t.TempDir())
	serials := make(chan *big.Int, 4)

	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{server.keyPair(t)},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    poolOf(a, b),
			MinVersion:   tls.VersionTLS12,
		})),
		grpc.UnaryInterceptor(func(
			ctx context.Context,
			req any,
			_ *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (any, error) {
			if p, ok := peer.FromContext(ctx); ok {
				if info, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(info.State.PeerCertificates) > 0 {
					serials <- info.State.PeerCertificates[0].SerialNumber
				}
			}

			return handler(ctx, req)
		}),
	)
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	addr := localhost(ln.Addr())
	d := creds.NewDialer(creds.WithCA(server.caFile), creds.WithCertificate(a.certFile, a.keyFile))

	opt, err := d.DialOption("localhost")
	require.NoError(t, err)
	require.NoError(t, checkHealth(t, addr, opt))
	require.Equal(t, a.serial, <-serials)

	rotate(t, b, a)
	dialerReload(t, d)

	opt, err = d.DialOption("localhost")
	require.NoError(t, err)
	require.NoError(t, checkHealth(t, addr, opt))
	require.Equal(t, b.serial, <-serials)
}

// newPKI writes ca.pem, cert.pem, and key.pem into dir.
func newPKI(t *testing.T, dir string) pki {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber:          randSerial(t),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	leafTmpl := &x509.Certificate{
		SerialNumber: randSerial(t),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)

	p := pki{
		caFile:   filepath.Join(dir, "ca.pem"),
		certFile: filepath.Join(dir, "cert.pem"),
		keyFile:  filepath.Join(dir, "key.pem"),
		serial:   leafTmpl.SerialNumber,
		ca:       ca,
	}
	p.pool = poolOf(p)

	writePEM(t, p.caFile, "CERTIFICATE", caDER)
	writePEM(t, p.certFile, "CERTIFICATE", leafDER)
	writePEM(t, p.keyFile, "EC PRIVATE KEY", keyDER)

	return p
}

func (p pki) keyPair(t *testing.T) tls.Certificate {
	t.Helper()

	pair, err := tls.LoadX509KeyPair(p.certFile, p.keyFile)
	require.NoError(t, err)

	return pair
}

func randSerial(t *testing.T) *big.Int {
	t.Helper()

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	require.NoError(t, err)

	return serial
}

func poolOf(ps ...pki) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, p := range ps {
		pool.AddCert(p.ca)
	}

	return pool
}

// rotate copies src's leaf and key over dst's paths.
func rotate(t *testing.T, src, dst pki) {
	t.Helper()

	copyFile(t, src.certFile, dst.certFile)
	copyFile(t, src.keyFile, dst.keyFile)
}

func reload(t *testing.T, l *creds.Listener) {
	t.Helper()

	attempted, err := creds.ListenerReload(l)
	require.NoError(t, err)
	require.True(t, attempted)
}

func dialerReload(t *testing.T, d *creds.Dialer) {
	t.Helper()

	attempted, err := creds.DialerReload(d)
	require.NoError(t, err)
	require.True(t, attempted)
}

func listenerConfig(t *testing.T, l *creds.Listener) *tls.Config {
	t.Helper()

	cfg, err := l.TLSConfig()
	require.NoError(t, err)

	return cfg
}

func serverOption(t *testing.T, p pki) grpc.ServerOption {
	t.Helper()

	opt, err := creds.NewListener(creds.WithCertificate(p.certFile, p.keyFile)).ServerOption()
	require.NoError(t, err)

	return opt
}

// serveTLS accepts TLS connections on a loopback port. Each connection that
// completes the handshake gets one byte, so a client can surface a TLS 1.3
// client-certificate rejection by reading it.
func serveTLS(t *testing.T, cfg *tls.Config) string {
	t.Helper()

	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go func() {
				defer func() { _ = conn.Close() }()

				if err := conn.(*tls.Conn).Handshake(); err != nil {
					return
				}

				_, _ = conn.Write([]byte{1})
			}()
		}
	}()

	return localhost(ln.Addr())
}

// handshake dials addr, waits for the server's byte, and returns the serial of
// the server's leaf.
func handshake(addr string, cfg *tls.Config) (*big.Int, error) {
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, err
	}

	defer func() { _ = conn.Close() }()

	if _, err := io.ReadFull(conn, make([]byte, 1)); err != nil {
		return nil, err
	}

	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return nil, errors.New("no peer certificate")
	}

	return certs[0].SerialNumber, nil
}

func serveHealth(t *testing.T, opt grpc.ServerOption) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(opt)
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())

	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	return localhost(ln.Addr())
}

func checkHealth(t *testing.T, addr string, opt grpc.DialOption) error {
	t.Helper()

	cc, err := grpc.NewClient(addr, opt)
	require.NoError(t, err)

	defer func() { _ = cc.Close() }()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err = grpc_health_v1.NewHealthClient(cc).Check(ctx, &grpc_health_v1.HealthCheckRequest{})

	return err
}

// localhost rewrites a loopback address as localhost:<port> so verification
// matches the test leaf's DNSNames.
func localhost(addr net.Addr) string {
	return net.JoinHostPort("localhost", strconv.Itoa(addr.(*net.TCPAddr).Port))
}
