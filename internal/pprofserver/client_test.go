package pprofserver

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/pkg/logger"
)

// These tests drive the routes the way a caller does: a real HTTP client
// against a real listener, no handler reached directly.

func TestClientAuthenticatesOverTheFullStack(t *testing.T) {
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

	svr := httptest.NewServer(routes[0].Handler)
	t.Cleanup(svr.Close)

	resp, err := svr.Client().Get(svr.URL + route)
	require.NoError(t, err)
	_ = responseBody(t, resp)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// The static token scheme reads a Bearer credential from the
	// Authorization header, its default when config names no other.
	req, err := http.NewRequest(http.MethodGet, svr.URL+route, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer s3cret")

	resp, err = svr.Client().Do(req)
	require.NoError(t, err)
	_ = responseBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func responseBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(body)
}
