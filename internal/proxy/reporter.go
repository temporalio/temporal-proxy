package proxy

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

// Reporter records envelope-operation telemetry to Prometheus: each seal
// (encrypt) and open (decrypt) the encryption interceptor performs, timed end
// to end, including any KEK wrap or unwrap and any DEK cache lookup along the
// way. The AES-step duration alone is owned by internal/kms. The namespace
// label is unbounded, so handles are resolved per call via WithLabelValues
// rather than pre-computed. A Reporter is safe for concurrent use.
type Reporter struct {
	ops      *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewReporter builds the Prometheus-backed vault-operation Reporter. f must
// already be scoped to the "encryption" subsystem by the caller.
func NewReporter(f *metrics.Factory) *Reporter {
	return &Reporter{
		ops: f.NewCounter(prometheus.CounterOpts{
			Name: "vault_ops_total",
			Help: "Total envelope operations (encrypt/decrypt), labeled by operation, result, and namespace.",
		}, []string{"operation", "result", "namespace"}),
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name: "vault_ops_duration_secs",
			Help: "Duration of envelope operations in seconds end to end, including any KEK wrap or unwrap, labeled by operation and namespace.",
		}, []string{"operation", "namespace"}),
	}
}

// VaultOp records a single envelope operation and its duration.
func (r *Reporter) VaultOp(operation, result, namespace string, seconds float64) {
	r.ops.WithLabelValues(operation, result, namespace).Inc()
	r.duration.WithLabelValues(operation, namespace).Observe(seconds)
}
