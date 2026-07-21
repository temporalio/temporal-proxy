package proxy_test

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("wires defaults and runs the lifecycle", func(t *testing.T) {
		t.Parallel()

		const upstream = "127.0.0.1:47233"

		app := newProxyApp(t, &config.Config{
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: upstream}}},
		})
		require.NoError(t, app.Err())

		startServeStop(t, app, upstream)
	})

	t.Run("uses the supplied logger", func(t *testing.T) {
		t.Parallel()

		const upstream = "127.0.0.1:57233"

		log := logger.NewTestLogger()
		app := newProxyApp(
			t,
			&config.Config{Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: upstream}}}},
			fx.Provide(func() logger.Logger { return log }),
		)
		require.NoError(t, app.Err())

		startServeStop(t, app, upstream)

		require.True(t, log.Contains("Starting the server"), "expected the injected logger to be used")
	})

	t.Run("rejects invalid upstream configuration before construction", func(t *testing.T) {
		t.Parallel()

		app := newProxyApp(t, &config.Config{
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "not-a-host-port"}}},
		})

		require.Error(t, app.Err())
		require.ErrorContains(t, app.Err(), "invalid upstream configuration")

		var errs validation.Errors
		require.ErrorAs(t, app.Err(), &errs, "expected validation.Errors in chain")
		require.NotEmpty(t, errs)
	})
}

func TestModuleMultipleUpstreams(t *testing.T) {
	t.Parallel()

	const (
		a = "127.0.0.1:47234"
		b = "127.0.0.1:47235"
	)

	app := newProxyApp(t, &config.Config{
		Upstreams: []config.Upstream{
			{Name: "a", Listen: config.ListenConfig{HostPort: a}},
			{Name: "b", Listen: config.ListenConfig{HostPort: b}},
		},
	})
	require.NoError(t, app.Err())

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	// Both upstreams get their own proxy, so both sockets must serve.
	for _, upstream := range []string{a, b} {
		conn := dialUnix(t, upstream)
		resp, err := grpc_health_v1.NewHealthClient(conn).Check(
			startCtx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true),
		)
		require.NoError(t, err, "upstream %s should serve after start", upstream)
		require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
		_ = conn.Close()
	}

	stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, app.Stop(stopCtx))

	// Every proxy must be stopped too. A fresh dial without WaitForReady fails
	// fast once the listener is gone; if a hook had captured the wrong server,
	// one socket would linger and keep serving here.
	for _, upstream := range []string{a, b} {
		conn := dialUnix(t, upstream)
		checkCtx, checkCancel := context.WithTimeout(t.Context(), 2*time.Second)
		_, err := grpc_health_v1.NewHealthClient(conn).Check(
			checkCtx, &grpc_health_v1.HealthCheckRequest{},
		)

		checkCancel()
		require.Error(t, err, "upstream %s should not serve after stop", upstream)
		require.NoError(t, conn.Close())
	}
}

// TestModuleWithUpstreamCredentials is construction-only: proxy.New dials
// lazily, so this asserts the Module accepts a credentialed+TLS upstream without
// erroring, not that the credential is actually attached. See
// TestProxyAttachesUpstreamCredential (e2e/upstream_credential_socket_test.go)
// for proof the key reaches the upstream.
func TestModuleWithUpstreamCredentials(t *testing.T) {
	t.Parallel()

	const upstream = "127.0.0.1:47236"

	// The upstream TLS validation path requires an RSA cert (it checks
	// compatibility with the RSA-only cipher suites in creds.TLS); the key file
	// only needs to be a validly formatted PEM key, since New never dials
	// (construction, not a live handshake, is what this test exercises).
	certPEM := testutil.RSACert(t, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	})
	certFile := testutil.WriteFile(t, t.TempDir(), "cert.pem", certPEM)
	_, keyFile := testutil.GenerateSelfSignedCert(t)

	app := newProxyApp(t, &config.Config{
		Upstreams: []config.Upstream{{
			Name:        "workers",
			Listen:      config.ListenConfig{HostPort: upstream, TLS: &config.TLSConfig{Cert: certFile, Key: keyFile}},
			Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "k3y"}},
		}},
	})
	require.NoError(t, app.Err())
}

func TestModuleRejectsTemplatedUpstream(t *testing.T) {
	t.Parallel()

	app := newProxyApp(t, &config.Config{
		Upstreams: []config.Upstream{
			{Name: "tmpl", Listen: config.ListenConfig{HostPort: "{{ .LocalNamespace }}.acme.cloud:7233"}},
		},
	})

	require.Error(t, app.Err())
	require.ErrorContains(t, app.Err(), "templated")
}

func TestModuleInstallsNamespaceTranslation(t *testing.T) {
	t.Parallel()

	const upstream = "127.0.0.1:47241"

	// An upstream that configures namespace rules wires translation from the
	// injected Translator and must still build and serve.
	app := newProxyApp(t, &config.Config{
		Upstreams: []config.Upstream{{
			Name:       "primary",
			Listen:     config.ListenConfig{HostPort: upstream},
			Namespaces: config.NamespaceConfig{Rules: config.NamespaceRules{Suffix: ".remote"}},
		}},
	})
	require.NoError(t, app.Err())

	startServeStop(t, app, upstream)
}

func TestModuleRequiresTranslator(t *testing.T) {
	t.Parallel()

	// proxy.Module requires a *protoutil.Translator. Without protoutil.Module
	// (or another provider) the app must fail to build rather than run without
	// one.
	app := fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(&config.Config{
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:47242"}}},
		}),
		proxy.Module,
		fx.NopLogger,
	)

	require.Error(t, app.Err())
}

func newProxyApp(t *testing.T, cfg *config.Config, opts ...fx.Option) *fx.App {
	t.Helper()

	base := []fx.Option{
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		protoutil.Module,
		proxy.Module,
		fx.NopLogger,
	}

	return fx.New(append(base, opts...)...)
}

// startServeStop starts the app, confirms the proxy serves on its unix socket
// via the local health service, then stops the app.
func startServeStop(t *testing.T, app *fx.App, upstream string) {
	t.Helper()

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	conn := dialUnix(t, upstream)
	defer func() { _ = conn.Close() }()

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(
		startCtx,
		&grpc_health_v1.HealthCheckRequest{},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

	stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, app.Stop(stopCtx))
}
