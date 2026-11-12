package e2e

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
)

// TestHealthzAnswersWithoutTheCodecServersCredential proves both halves of the
// route's contract against the production module graph: a load balancer
// holding no credential gets its answer, and that exemption stops at /healthz.
func TestHealthzAnswersWithoutTheCodecServersCredential(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.HTTP = config.HTTP{
		Listen: config.ListenConfig{HostPort: "127.0.0.1:0", Insecure: true},
		CodecServer: config.CodecServer{
			Enabled: true,
			Auth:    &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "s3cret"}},
		},
	}

	f := dataplanetest.Start(t, cfg)

	req, err := http.NewRequestWithContext(f.Context(), http.MethodGet, "http://"+f.HTTPAddr()+"/healthz", nil)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = res.Body.Close() })

	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode, "response body: %s", raw)
	require.Equal(t, "ok\n", string(raw))

	for _, route := range []string{"/encode", "/decode", "/download"} {
		res := postCodecServer(t, f, route, `{"payloads":[]}`, nil)
		require.Equal(t, http.StatusUnauthorized, res.StatusCode, route)
	}
}
