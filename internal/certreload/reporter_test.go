package certreload_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	goprom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/certreload"
	"go.temporal.io/proxy/internal/metrics"
)

func TestReporter(t *testing.T) {
	t.Parallel()

	reg := goprom.NewRegistry()
	r := certreload.NewReporter(metrics.New("ns", promauto.With(reg)))

	r.Attempt("gateway", nil)
	r.Attempt("gateway", nil)
	r.Attempt("gateway", errors.New("x"))
	r.NotAfter("gateway", "cert", time.Unix(1700000000, 0))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP ns_tls_reloads_total TLS material load attempts, by credential and result.
# TYPE ns_tls_reloads_total counter
ns_tls_reloads_total{credential="gateway",result="failure"} 1
ns_tls_reloads_total{credential="gateway",result="success"} 2
# HELP ns_tls_cert_expiry_timestamp_seconds Expiry (NotAfter) of the loaded certificate, as a Unix timestamp.
# TYPE ns_tls_cert_expiry_timestamp_seconds gauge
ns_tls_cert_expiry_timestamp_seconds{credential="gateway",file="cert"} 1.7e+09
`)))
}
