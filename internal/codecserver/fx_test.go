package codecserver_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/codecserver"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/httpserver"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

type contributed struct {
	fx.In
	Routes []httpserver.Route `group:"http_routes"`
}

func TestModuleContributesRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
	}{
		{name: "none when disabled"},
		{name: "the codec routes at the root when enabled", enabled: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.Config{HTTP: config.HTTP{CodecServer: config.CodecServer{Enabled: tc.enabled}}}

			var routes []httpserver.Route

			app := fx.New(
				fx.Supply(cfg),
				fx.Provide(func() (*proxy.Codecs, error) { return proxy.NewCodecs(proxy.CodecOptions{}) }),
				fx.Provide(func() api.Connections { return api.Connections{} }),
				fx.Provide(func() *metrics.Factory {
					return metrics.New("test", promauto.With(prometheus.NewRegistry()))
				}),
				fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
				codecserver.Module,
				fx.Invoke(func(c contributed) { routes = c.Routes }),
				fx.NopLogger,
			)
			require.NoError(t, app.Err())

			if !tc.enabled {
				require.Empty(t, routes)

				return
			}

			require.Len(t, routes, 1)
			require.Equal(t, "/", routes[0].Pattern)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/decode", strings.NewReader(`{"payloads":[]}`))
			req.Header.Set("Content-Type", "application/json")
			routes[0].Handler.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// TestModuleRequiresANamespaceUnderStrict pins the fx wiring for the strict
// requirement on both routes. Keying it on anything but strict fails a row,
// dropping it from /encode fails the strict encode row, and applying it to
// /encode outside strict fails the permissive and off encode rows.
func TestModuleRequiresANamespaceUnderStrict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		binding  config.NamespaceBinding
		route    string
		wantCode int
	}{
		{name: "strict decode", binding: config.NamespaceBindingStrict, route: "/decode", wantCode: http.StatusBadRequest},
		{name: "strict encode", binding: config.NamespaceBindingStrict, route: "/encode", wantCode: http.StatusBadRequest},
		{name: "permissive decode", binding: config.NamespaceBindingPermissive, route: "/decode", wantCode: http.StatusOK},
		{name: "off decode", binding: config.NamespaceBindingOff, route: "/decode", wantCode: http.StatusOK},
		{name: "permissive encode", binding: config.NamespaceBindingPermissive, route: "/encode", wantCode: http.StatusOK},
		{name: "off encode", binding: config.NamespaceBindingOff, route: "/encode", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.Config{
				HTTP:       config.HTTP{CodecServer: config.CodecServer{Enabled: true}},
				Encryption: config.Encryption{NamespaceBinding: tc.binding},
			}

			var routes []httpserver.Route

			app := fx.New(
				fx.Supply(cfg),
				fx.Provide(func() (*proxy.Codecs, error) { return proxy.NewCodecs(proxy.CodecOptions{}) }),
				fx.Provide(func() api.Connections { return api.Connections{} }),
				fx.Provide(func() *metrics.Factory {
					return metrics.New("test", promauto.With(prometheus.NewRegistry()))
				}),
				fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
				codecserver.Module,
				fx.Invoke(func(c contributed) { routes = c.Routes }),
				fx.NopLogger,
			)
			require.NoError(t, app.Err())
			require.Len(t, routes, 1)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, tc.route, strings.NewReader(`{"payloads":[]}`))
			req.Header.Set("Content-Type", "application/json")
			routes[0].Handler.ServeHTTP(rec, req)
			require.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
