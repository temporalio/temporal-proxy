package proxy

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

// Reporter records DEK payload-operation telemetry to Prometheus: each seal
// (encrypt) and open (decrypt) the encryption interceptor performs. The
// namespace label is unbounded, so handles are resolved per call via
// WithLabelValues rather than pre-computed. A Reporter is safe for concurrent
// use.
type Reporter struct {
	ops      *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewReporter builds the Prometheus-backed DEK-operation Reporter. f must
// already be scoped to the "encryption" subsystem by the caller.
func NewReporter(f *metrics.Factory) *Reporter {
	return &Reporter{
		ops: f.NewCounter(prometheus.CounterOpts{
			Name: "dek_ops_total",
			Help: "Total DEK payload operations (encrypt/decrypt), labeled by operation, result, and namespace.",
		}, []string{"operation", "result", "namespace"}),
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name: "dek_ops_duration_secs",
			Help: "Duration of DEK payload operations in seconds, labeled by operation and namespace.",
		}, []string{"operation", "namespace"}),
	}
}

// DEKOp records a single DEK payload operation and its duration.
func (r *Reporter) DEKOp(operation, result, namespace string, seconds float64) {
	r.ops.WithLabelValues(operation, result, namespace).Inc()
	r.duration.WithLabelValues(operation, namespace).Observe(seconds)
}
