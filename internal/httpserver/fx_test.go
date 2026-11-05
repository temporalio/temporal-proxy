package httpserver_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/httpserver"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/testutil"
)

// credentialRecorder is a [creds.Reporter] that keeps the credential label of
// every expiry it is told about.
type credentialRecorder struct {
	mu    sync.Mutex
	names []string
}

func TestModuleMountsEveryRouteGroup(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		HTTP: config.HTTP{Listen: config.ListenConfig{HostPort: "127.0.0.1:0", Insecure: true}},
	}

	var svr *httpserver.Server

	app := fx.New(
		fx.Supply(cfg, creds.NewReloader()),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		fx.Provide(httpserver.AsRoutes(func() []httpserver.Route {
			return []httpserver.Route{{Pattern: "/", Handler: body("root")}}
		})),
		fx.Provide(httpserver.AsRoutes(func() ([]httpserver.Route, error) {
			return []httpserver.Route{{Pattern: "/other/", Handler: body("other")}}, nil
		})),
		httpserver.Module,
		fx.Populate(&svr),
		fx.NopLogger,
	)
	require.NoError(t, app.Err())
	require.NoError(t, app.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, app.Stop(context.WithoutCancel(t.Context()))) })

	base := "http://" + svr.Addr().String()
	require.Equal(t, "root", get(t, base+"/decode"))
	require.Equal(t, "other", get(t, base+"/other/thing"))
}

func TestModuleFailsToStartWhenThePortIsTaken(t *testing.T) {
	t.Parallel()

	// Start binds before it returns, so a taken port fails the app's start
	// rather than surfacing later through the abort path.
	held, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close() })

	app := newTestApp(t, config.ListenConfig{HostPort: held.Addr().String(), Insecure: true})
	require.NoError(t, app.Err())

	require.ErrorContains(t, app.Start(t.Context()), "address already in use")
}

func TestModuleFailsWhenTheTLSMaterialWillNotLoad(t *testing.T) {
	t.Parallel()

	missing := t.TempDir() + "/missing.pem"
	app := newTestApp(t, config.ListenConfig{
		HostPort: "127.0.0.1:0",
		TLS:      &config.TLSConfig{Cert: missing, Key: missing},
	})

	require.ErrorContains(t, app.Err(), "failed to read PEM file")
}

func TestNewFromParams_RegistersHTTPCredential(t *testing.T) {
	t.Parallel()

	cert, key := testutil.GenerateSelfSignedCert(t)

	rec := &credentialRecorder{}
	r := creds.NewReloader(creds.WithReporter(rec))

	app := newTestAppWithReloader(t, config.ListenConfig{
		HostPort: "127.0.0.1:0",
		TLS:      &config.TLSConfig{Cert: cert, Key: key},
	}, r)
	require.NoError(t, app.Err())

	require.NoError(t, r.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, r.Stop(context.WithoutCancel(t.Context()))) })

	require.Equal(t, []string{"http"}, rec.credentials())
}

func TestModuleIsInertWithNoRoutes(t *testing.T) {
	t.Parallel()

	// With no group enabled nothing may bind, so the module must produce no
	// Server at all rather than one that happens not to have started. Starting
	// the app also exercises the lifecycle hook itself: with no Server, it must
	// never be appended, so there is nothing for OnStart to call on a nil
	// receiver.
	var svr *httpserver.Server

	app := fx.New(
		fx.Supply(&config.Config{}, creds.NewReloader()),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		fx.Provide(httpserver.AsRoutes(func() []httpserver.Route { return nil })),
		httpserver.Module,
		fx.Populate(&svr),
		fx.NopLogger,
	)
	require.NoError(t, app.Err())
	require.Nil(t, svr)

	require.NoError(t, app.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, app.Stop(context.WithoutCancel(t.Context()))) })
}

func (c *credentialRecorder) Attempt(string, error) {}

func (c *credentialRecorder) NotAfter(credential, _ string, _ time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.names = append(c.names, credential)
}

func (c *credentialRecorder) credentials() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.names...)
}

// newTestApp builds the module serving a single root route on listen.
func newTestApp(t *testing.T, listen config.ListenConfig) *fx.App {
	t.Helper()

	return newTestAppWithReloader(t, listen, creds.NewReloader())
}

// newTestAppWithReloader builds the module serving a single root route on
// listen, registering its TLS material with r.
func newTestAppWithReloader(t *testing.T, listen config.ListenConfig, r *creds.Reloader) *fx.App {
	t.Helper()

	return fx.New(
		fx.Supply(&config.Config{HTTP: config.HTTP{Listen: listen}}, r),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		fx.Provide(httpserver.AsRoutes(func() []httpserver.Route {
			return []httpserver.Route{{Pattern: "/", Handler: body("root")}}
		})),
		httpserver.Module,
		fx.NopLogger,
	)
}

func body(s string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, s) })
}

func get(t *testing.T, url string) string {
	t.Helper()

	res, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()

	require.Equal(t, http.StatusOK, res.StatusCode)

	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	return string(b)
}
