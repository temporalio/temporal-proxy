package proxy_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("wires defaults and runs the lifecycle", func(t *testing.T) {
		t.Parallel()

		upstream := serveUpstream(t)

		app := newProxyApp(t, &config.Config{
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: upstream}}},
		})
		require.NoError(t, app.Err())

		startServeStop(t, app, upstream)
	})

	t.Run("uses the supplied logger", func(t *testing.T) {
		t.Parallel()

		upstream := serveUpstream(t)

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

	a, b := serveUpstream(t), serveUpstream(t)

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

func TestModuleWithUpstreamCredentials(t *testing.T) {
	t.Parallel()

	const upstream = "127.0.0.1:47236"

	// A client certificate selects mutual TLS, which verifies the upstream
	// against the configured CA. Construction (not a live handshake) is what
	// this test exercises.
	caFile, certFile, keyFile := testutil.GenerateMTLSCerts(t)

	app := newProxyApp(t, &config.Config{
		Upstreams: []config.Upstream{{
			Name: "workers",
			Listen: config.ListenConfig{
				HostPort: upstream,
				TLS:      &config.TLSConfig{CA: caFile, Cert: certFile, Key: keyFile, ServerName: "localhost"},
			},
			Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "k3y"}},
		}},
	})
	require.NoError(t, app.Err())
}

func TestModuleWithCAOnlyClientTLS(t *testing.T) {
	t.Parallel()

	const upstream = "127.0.0.1:47237"

	// A CA with no client certificate is client-side TLS: verify the upstream
	// against the CA, present no client cert. Construction must succeed.
	caFile, _, _ := testutil.GenerateMTLSCerts(t)

	app := newProxyApp(t, &config.Config{
		Upstreams: []config.Upstream{{
			Name: "workers",
			Listen: config.ListenConfig{
				HostPort: upstream,
				TLS:      &config.TLSConfig{CA: caFile, ServerName: "localhost"},
			},
			Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "k3y"}},
		}},
	})
	require.NoError(t, app.Err())
}

func TestModuleAcceptsTemplatedUpstream(t *testing.T) {
	t.Parallel()

	app := newProxyApp(t, &config.Config{
		Upstreams: []config.Upstream{
			{Name: "tmpl", Listen: config.ListenConfig{HostPort: "{{ .LocalNamespace }}.acme.cloud:7233"}},
		},
	})

	// Previously this failed with "templated upstreams are not yet supported".
	// A templated hostPort is now resolved per-request via a templatedPlan, so
	// construction (and the full start/stop lifecycle) succeeds without ever
	// dialing the upstream. Nothing here is listening, so this also covers the
	// start hook that opens static upstreams skipping templated ones.
	require.NoError(t, app.Err())

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, app.Stop(stopCtx))
}

func TestModuleInstallsNamespaceTranslation(t *testing.T) {
	t.Parallel()

	upstream := serveUpstream(t)

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

func TestModuleFailsStartWhenStaticUpstreamUnreachable(t *testing.T) {
	t.Parallel()

	app := newProxyApp(t, &config.Config{
		Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: deadUpstream(t)}}},
	})

	// An unreachable upstream is valid configuration, so it survives construction
	// (grpc.NewClient does not dial) and fails when the connection is opened on
	// start.
	require.NoError(t, app.Err())

	startCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	err := app.Start(startCtx)
	require.ErrorContains(t, err, "upstream connection not ready")
}

func TestModuleFailsFastWhenEncryptionEnabledWithoutVault(t *testing.T) {
	t.Parallel()

	// Encryption is a security control. If it is enabled but the vault is nil
	// (a wiring fault), the proxy must refuse to start rather than silently
	// forward cleartext upstream. newProxyApp supplies a nil vault.
	app := newProxyApp(t, &config.Config{
		Encryption: config.Encryption{Enabled: true},
		Upstreams:  []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:47243"}}},
	})

	require.Error(t, app.Err())
	require.ErrorContains(t, app.Err(), "encryption is enabled but no vault")
}

func TestModuleRequiresTranslator(t *testing.T) {
	t.Parallel()

	app := fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(&config.Config{
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:47242"}}},
		}),
		fx.Provide(func() *crypto.Vault { return nil }),
		fx.Provide(func() *metrics.Factory { return metrics.New("tmprl_proxy", promauto.With(prometheus.NewRegistry())) }),
		connect.Module,
		proxy.Module,
		fx.Provide(config.NewAllowlist),
		fx.NopLogger,
	)

	// Naming the missing type keeps this from passing for any other absent
	// dependency, which is how it would silently stop testing the Translator.
	require.ErrorContains(t, app.Err(), "protoutil.Translator")
}

func TestModuleRequiresAllowlist(t *testing.T) {
	t.Parallel()

	// Deliberately without config.NewAllowlist: the gate is required so missing
	// wiring fails here, rather than degrading to a proxy that forwards nothing.
	app := fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(&config.Config{
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:47244"}}},
		}),
		fx.Provide(func() *crypto.Vault { return nil }),
		fx.Provide(func() *metrics.Factory { return metrics.New("tmprl_proxy", promauto.With(prometheus.NewRegistry())) }),
		connect.Module,
		protoutil.Module,
		proxy.Module,
		fx.NopLogger,
	)

	require.ErrorContains(t, app.Err(), "services.Allowlist")
}

// serveUpstream starts a plaintext gRPC server on a loopback port, registers any
// services supplied, and returns its address for use as an upstream hostPort. A
// static upstream's connection is opened on start, so an upstream pointing at
// nothing fails the lifecycle. The ephemeral port also keeps the proxy's derived
// socket path unique across parallel tests.
func serveUpstream(t *testing.T, register ...func(*grpc.Server)) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svr := grpc.NewServer()
	for _, reg := range register {
		reg(svr)
	}

	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	return lis.Addr().String()
}

// deadUpstream returns a loopback address with nothing behind it, by taking a
// port from the kernel and immediately giving it back.
func deadUpstream(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	return addr
}

func newProxyApp(t *testing.T, cfg *config.Config, opts ...fx.Option) *fx.App {
	t.Helper()

	base := []fx.Option{
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		// No encryption keys configured: kms.Module provides a nil vault in that
		// case, and the proxy skips the encryption interceptor. Tests that
		// exercise encryption override this with a real vault.
		fx.Provide(func() *crypto.Vault { return nil }),
		fx.Provide(func() *metrics.Factory { return metrics.New("tmprl_proxy", promauto.With(prometheus.NewRegistry())) }),
		connect.Module,
		protoutil.Module,
		proxy.Module,
		fx.Provide(config.NewAllowlist),
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
