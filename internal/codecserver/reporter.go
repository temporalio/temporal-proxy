package codecserver

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

// Reporter records codec server request telemetry to Prometheus: one count and
// one duration sample per request, on every path including the failures.
//
// A Reporter is safe for concurrent use. Handles are resolved per call through
// WithLabelValues rather than pre-computed, since the label set is small and
// fixed.
type Reporter struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewReporter builds the Prometheus-backed request Reporter and registers its
// collectors. Build one per registry; Prometheus rejects a duplicate
// registration, and the factory panics rather than erring on one.
//
// Parameters:
//   - f: must already be scoped to the "codec_server" subsystem by the caller,
//     which is what produces the published tmprl_proxy_codec_server_* names.
//
// Returns a Reporter publishing requests_total, labelled by route and code, and
// request_duration_seconds, labelled by route.
func NewReporter(f *metrics.Factory) *Reporter {
	return &Reporter{
		requests: f.NewCounter(prometheus.CounterOpts{
			Name: "requests_total",
			Help: "Total codec server requests, labeled by route and HTTP status code.",
		}, []string{"route", "code"}),
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name: "request_duration_seconds",
			Help: "Duration of codec server requests in seconds, labeled by route.",
		}, []string{"route"}),
	}
}

// Request records one served request, counting it and observing its duration.
// Call it once per request, on every path including the failures, so an error
// rate is derivable from the code label alone.
//
// Safe for concurrent use.
func (r *Reporter) Request(route string, code int, seconds float64) {
	r.requests.WithLabelValues(route, strconv.Itoa(code)).Inc()
	r.duration.WithLabelValues(route).Observe(seconds)
}
