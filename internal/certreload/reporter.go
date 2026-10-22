package certreload

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/transport/creds"
)

type reporter struct {
	reloads *prometheus.CounterVec
	expiry  *prometheus.GaugeVec
}

// NewReporter returns a [creds.Reporter] that records reload attempts and
// certificate expiry under the "tls" subsystem of f. Both series are labeled by
// credential id, never by Namespace.
func NewReporter(f *metrics.Factory) creds.Reporter {
	f = f.ForSubsystem("tls")

	return &reporter{
		reloads: f.NewCounter(prometheus.CounterOpts{
			Name: "reloads_total",
			Help: "TLS material load attempts, by credential and result.",
		}, []string{"credential", "result"}),
		expiry: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cert_expiry_timestamp_seconds",
			Help: "Expiry (NotAfter) of the loaded certificate, as a Unix timestamp.",
		}, []string{"credential", "file"}),
	}
}

func (r *reporter) Attempt(credential string, err error) {
	result := "success"
	if err != nil {
		result = "failure"
	}

	r.reloads.WithLabelValues(credential, result).Inc()
}

func (r *reporter) NotAfter(credential, file string, t time.Time) {
	r.expiry.WithLabelValues(credential, file).Set(float64(t.Unix()))
}
