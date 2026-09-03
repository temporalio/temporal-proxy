package server

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

// durationBuckets covers both sub-second unary calls and long-poll methods
// (PollWorkflowTaskQueue, PollActivityTaskQueue, long-poll history) that block
// 60s+ server-side; the default prometheus buckets top out at 10s.
var durationBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}

// Reporter records server-layer telemetry to Prometheus: per-RPC latency and
// completed-request counts by gRPC status code, both labeled by method. The
// method label set is not known at startup, so handles are resolved per call
// via WithLabelValues rather than pre-resolved. A Reporter is safe for
// concurrent use.
//
// Cardinality assumption: method comes from the request line, and the proxy
// serves every request through a catch-all handler, so any distinct method
// string a client sends becomes a new series. This is bounded only for trusted
// callers (real Temporal SDK clients use a fixed method set); a client sending
// arbitrary method paths can grow the series set without bound. The proxy
// therefore assumes trusted callers and must not be exposed directly to
// untrusted clients without first bounding this label. namespace is never a
// label for the same reason.
type Reporter struct {
	duration *prometheus.HistogramVec
	requests *prometheus.CounterVec
	tags     metrics.Tags
}

// NewReporter builds the Prometheus-backed Reporter. f must already be scoped to
// the "server" subsystem by the caller. tags are the configured metadata labels
// carried on both collectors, and may be the zero value.
func NewReporter(f *metrics.Factory, tags metrics.Tags) *Reporter {
	return &Reporter{
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "request_duration_seconds",
			Help:    "Time spent serving an RPC end to end, labeled by method.",
			Buckets: durationBuckets,
		}, append([]string{"method"}, tags.Labels()...)),
		requests: f.NewCounter(prometheus.CounterOpts{
			Name: "requests_total",
			Help: "Total RPCs served, labeled by method and gRPC status code.",
		}, append([]string{"method", "code"}, tags.Labels()...)),
		tags: tags,
	}
}

// Observe records one completed RPC: its duration on the method histogram and a
// count on the (method, code) counter. ctx is the request's, and supplies the
// configured metadata label values; the two collectors carry different fixed
// labels, so the values are resolved once and appended to each.
func (r *Reporter) Observe(ctx context.Context, method string, code codes.Code, d time.Duration) {
	tags := r.tags.AppendValues(ctx, nil)
	r.duration.WithLabelValues(append([]string{method}, tags...)...).Observe(d.Seconds())
	r.requests.WithLabelValues(append([]string{method, code.String()}, tags...)...).Inc()
}

// StreamInterceptor returns a stream server interceptor that times the handler
// and records the RPC's duration and final gRPC status code. It covers all
// forwarded traffic, which grpc-go serves through the unknown-service handler as
// streams. The local health service's unary Check is not metered, since unary
// calls do not pass through a stream interceptor; its streaming Watch, if a
// client uses it, would be metered under its own method name.
func (r *Reporter) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()
		err := handler(srv, ss)
		r.Observe(ss.Context(), info.FullMethod, status.Code(err), time.Since(start))
		return err
	}
}
