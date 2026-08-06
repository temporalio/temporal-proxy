package e2e

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/kms"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
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

	// Load applies this default when parsing YAML, which these tests skip in
	// favour of building a Config directly. Admit everything forwardable so
	// admission never stands in for the behaviour under test; it has its own
	// coverage in internal/services and internal/router.
	if len(cfg.AllowedServices) == 0 {
		cfg.AllowedServices = config.Services(services.Known())
	}

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
		api.Module,
		auth.Module,
		connect.Module,
		kms.Module,
		metrics.Module,
		protoutil.Module,
		proxy.Module,
		router.Module,
		server.Module,
		fx.NopLogger,
	)
}

// newProxyApp is a minimal fx app for the socket-level test in this package: it
// wires proxy.Module plus protoutil.Module (which provides the Translator) and
// connect.Module (which provides the Pool), both required by proxy.Module.
// internal/proxy/fx_test.go has its own copy (with support for extra
// fx.Options) that its unit tests still depend on; this is a deliberate small
// duplication rather than an exported helper, so the two packages stay
// decoupled.
func newProxyApp(t *testing.T, cfg *config.Config) *fx.App {
	t.Helper()

	return fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		fx.Provide(func() *crypto.Vault { return nil }),
		fx.Provide(func() *metrics.Factory { return metrics.New("test", promauto.With(prometheus.NewRegistry())) }),
		connect.Module,
		protoutil.Module,
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

	clientCertFile, clientKeyFile := testutil.GenerateRSACert(t)

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
// given upstream host. The socket path matches what proxy.Listen binds.
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
