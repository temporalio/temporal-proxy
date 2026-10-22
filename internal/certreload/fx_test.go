package certreload_test

import (
	"testing"

	goprom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"go.temporal.io/proxy/internal/certreload"
	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/pkg/logger"
	pkgtestutil "go.temporal.io/proxy/pkg/testutil"
)

func TestModule_StartsAndStopsReloader(t *testing.T) {
	t.Parallel()

	certFile, keyFile := pkgtestutil.GenerateSelfSignedCert(t)
	reg := goprom.NewRegistry()

	app := fxtest.New(t,
		fx.NopLogger,
		fx.Supply(
			fx.Annotate(logger.NewNoopLogger(), fx.As(new(logger.Logger))),
			metrics.New("ns", promauto.With(reg)),
		),
		certreload.Module,
		fx.Invoke(func(r *creds.Reloader) {
			creds.NewListener(
				creds.WithCertificate(certFile, keyFile),
				creds.WithReloader(r, "gateway"),
			)
		}),
	)

	app.RequireStart()

	families, err := reg.Gather()
	require.NoError(t, err)

	var found bool

	for _, mf := range families {
		if mf.GetName() != "ns_tls_cert_expiry_timestamp_seconds" {
			continue
		}

		for _, m := range mf.GetMetric() {
			found = true

			require.NotZero(t, m.GetGauge().GetValue())
		}
	}

	require.True(t, found, "expiry gauge not reported")

	app.RequireStop()
}
