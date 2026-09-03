package proxy

import (
	"context"

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
	tags     metrics.Tags
}

// NewReporter builds the Prometheus-backed vault-operation Reporter. f must
// already be scoped to the "encryption" subsystem by the caller. tags are the
// configured metadata labels, and may be the zero value.
func NewReporter(f *metrics.Factory, tags metrics.Tags) *Reporter {
	return &Reporter{
		ops: f.NewCounter(prometheus.CounterOpts{
			Name: "vault_ops_total",
			Help: "Total envelope operations (encrypt/decrypt), labeled by operation, result, and namespace.",
		}, append([]string{"operation", "result", "namespace"}, tags.Labels()...)),
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name: "vault_ops_duration_secs",
			Help: "Duration of envelope operations in seconds end to end, including any KEK wrap or unwrap, labeled by operation and namespace.",
		}, append([]string{"operation", "namespace"}, tags.Labels()...)),
		tags: tags,
	}
}

// VaultOp records a single envelope operation and its duration. ctx is the
// request's, and supplies the configured metadata label values. This runs on the
// per-upstream hop, where the metadata is what the gateway forwarded rather than
// what it received, so a header the inbound authenticator consumed is gone.
func (r *Reporter) VaultOp(ctx context.Context, operation, result, namespace string, seconds float64) {
	tags := r.tags.AppendValues(ctx, nil)
	r.ops.WithLabelValues(append([]string{operation, result, namespace}, tags...)...).Inc()
	r.duration.WithLabelValues(append([]string{operation, namespace}, tags...)...).Observe(seconds)
}
