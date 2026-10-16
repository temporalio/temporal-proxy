package pprofserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/pkg/logger"
)

func TestNewFromParamsDisabled(t *testing.T) {
	t.Parallel()

	routes, err := newFromParams(Params{
		Config: &config.Config{},
		Logger: logger.NewTestLogger(),
	})
	require.NoError(t, err)
	require.Empty(t, routes)
}

func TestNewFromParamsEnabled(t *testing.T) {
	t.Parallel()

	routes, err := newFromParams(Params{
		Config: &config.Config{
			HTTP: config.HTTP{PProf: config.PProf{Enabled: true}},
		},
		Logger: logger.NewTestLogger(),
	})
	require.NoError(t, err)
	require.Len(t, routes, 1)
	require.Equal(t, route, routes[0].Pattern)
}

func TestNewFromParamsWithAuth(t *testing.T) {
	t.Parallel()

	routes, err := newFromParams(Params{
		Config: &config.Config{
			HTTP: config.HTTP{
				PProf: config.PProf{
					Enabled: true,
					Auth: &config.AuthConfig{
						StaticToken: &config.StaticTokenConfig{Token: "s3cret"},
					},
				},
			},
		},
		Logger: logger.NewTestLogger(),
	})
	require.NoError(t, err)
	require.Len(t, routes, 1)

	// An unauthenticated caller is rejected before any handler runs.
	rec := httptest.NewRecorder()
	routes[0].Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, route, nil))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestNewFromParamsBadAuth(t *testing.T) {
	t.Parallel()

	_, err := newFromParams(Params{
		Config: &config.Config{
			HTTP: config.HTTP{
				PProf: config.PProf{
					Enabled: true,
					Auth:    &config.AuthConfig{},
				},
			},
		},
		Logger: logger.NewTestLogger(),
	})
	require.Error(t, err)
}

func TestHandlerRawEndpoints(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// The standard index lists the profile endpoints as relative links. The
	// quote style around href differs between Go releases, so the assertion
	// sticks to the part that does not.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, route, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "heap?debug=1")
	require.Contains(t, rec.Body.String(), "goroutine?debug=1")

	// heap has no pattern of its own: the index handler dispatches it, which
	// is the path the README's curl recipe takes.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, route+"heap", nil))
	require.Equal(t, http.StatusOK, rec.Code)
}
