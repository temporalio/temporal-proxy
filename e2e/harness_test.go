package e2e

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

type (
	// capturingWorkflowService is a fake upstream Temporal frontend that records
	// the incoming metadata on GetSystemInfo, the smallest WorkflowService method
	// that needs no namespace argument.
	capturingWorkflowService struct {
		workflowservice.UnimplementedWorkflowServiceServer

		mu sync.Mutex
		md metadata.MD
	}

	// fakeTLSUpstream is a running fake WorkflowService frontend over TLS together
	// with the CA and client-identity cert/key files needed to build a matching
	// upstream [config.TLSConfig]. It exposes the raw file paths (rather than an
	// assembled TLSConfig) so callers can vary other TLS fields, or the rest of the
	// upstream config, per case.
	fakeTLSUpstream struct {
		svc            *capturingWorkflowService
		addr           string
		caFile         string
		clientCertFile string
		clientKeyFile  string
	}
)

func (s *capturingWorkflowService) GetSystemInfo(
	ctx context.Context, _ *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	s.mu.Lock()
	s.md = md
	s.mu.Unlock()

	return &workflowservice.GetSystemInfoResponse{}, nil
}

// received returns the metadata observed by the most recent GetSystemInfo
// call, or nil if none has arrived yet.
func (s *capturingWorkflowService) received() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.md
}

// newFullApp builds an fx.App wiring every module the production app wires
// (see cmd/proxy/serve.go), so tests can drive the full stack -- inbound
// server, router, and per-upstream proxy -- rather than any single module in
// isolation. Each call gets a fresh Prometheus registry (avoiding
// duplicate-registration panics across parallel tests) and an ephemeral
// metrics listener address, since these tests only care about the proxy path.
func newFullApp(t *testing.T, cfg *config.Config) *fx.App {
	t.Helper()

	reg := prometheus.NewRegistry()

	return fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		fx.Supply(fx.Annotate("127.0.0.1:0", metrics.AddrTag)),
		fx.Supply(fx.Annotate("test", metrics.NamespaceTag)),
		fx.Provide(
			func() logger.Logger { return logger.NewNoopLogger() },
			func() prometheus.Gatherer { return reg },
			func() prometheus.Registerer { return reg },
		),
		auth.Module,
		connect.Module,
		metrics.Module,
		protoutil.Module,
		proxy.Module,
		router.Module,
		server.Module,
		fx.NopLogger,
	)
}

// newProxyApp is a minimal, proxy.Module-only fx app for the socket-level
// test in this package. internal/proxy/fx_test.go has its own copy (with
// support for extra fx.Options) that its unit tests still depend on; this is
// a deliberate small duplication rather than an exported helper, so the two
// packages stay decoupled.
func newProxyApp(t *testing.T, cfg *config.Config) *fx.App {
	t.Helper()

	return fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		proxy.Module,
		fx.NopLogger,
	)
}

// newFakeTLSUpstream stands up a fake WorkflowService frontend over TLS and
// returns it along with the cert files needed to dial it.
func newFakeTLSUpstream(t *testing.T) fakeTLSUpstream {
	t.Helper()

	caFile, serverCertFile, serverKeyFile := testutil.GenerateMTLSCerts(t)
	serverCert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	require.NoError(t, err)

	svc := &capturingWorkflowService{}
	fakeUpstream := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
	})))
	workflowservice.RegisterWorkflowServiceServer(fakeUpstream, svc)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = fakeUpstream.Serve(lis) }()
	t.Cleanup(fakeUpstream.Stop)

	clientCertFile, clientKeyFile := generateMatchingRSAKeyPair(t)

	return fakeTLSUpstream{
		svc:            svc,
		addr:           lis.Addr().String(),
		caFile:         caFile,
		clientCertFile: clientCertFile,
		clientKeyFile:  clientKeyFile,
	}
}

// freeTCPAddr reserves an ephemeral localhost TCP port and returns its
// address. The listener is closed before returning so the caller (here, the
// inbound server started by newFullApp) can bind it; the small race window is
// acceptable in tests. Mirrors the identically named helper in
// internal/server/fx_test.go, which package e2e cannot see.
func freeTCPAddr(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

// dialInbound dials the top-level server's inbound address the way a real
// client would: plaintext, since the full-stack cases here never configure
// inbound TLS.
func dialInbound(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return conn
}

// dialUnix returns a client connection to the proxy's unix socket for the
// given upstream host. The socket path matches what proxy.Start binds.
// internal/proxy/server_test.go has its own copy that its unit tests still
// depend on; this is a deliberate small duplication for the socket-level test
// in this package.
func dialUnix(t *testing.T, upstream string) *grpc.ClientConn {
	t.Helper()

	path, err := socket.UnixPath(upstream)
	require.NoError(t, err)

	conn, err := grpc.NewClient(
		"unix://"+path,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	return conn
}

// generateMatchingRSAKeyPair writes a self-signed RSA-2048 certificate and its
// matching private key to a fresh [testing.T.TempDir] and returns the paths.
// Unlike [testutil.RSACert], the private key is retained and written out, so
// the pair loads via [tls.LoadX509KeyPair] for use as a real TLS client
// identity.
func generateMatchingRSAKeyPair(t *testing.T) (certFile, keyFile string) {
	t.Helper()

	dir := t.TempDir()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "proxy-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	certFile = testutil.WriteFile(t, dir, "client-cert.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyFile = testutil.WriteFile(t, dir, "client-key.pem",
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))

	return certFile, keyFile
}
