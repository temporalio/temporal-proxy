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
