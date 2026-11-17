package server

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"go.temporal.io/proxy/internal/metrics"
)

// MethodUnknown is the method label reported for any method the Reporter was not
// told about.
const MethodUnknown = "unknown"

// durationBuckets covers both sub-second unary calls and long-poll methods
// (PollWorkflowTaskQueue, PollActivityTaskQueue, long-poll history) that block
// 60s+ server-side; the default prometheus buckets top out at 10s.
var durationBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}

// Reporter records server-layer telemetry to Prometheus: per-RPC latency and
// completed-request counts by gRPC status code, both labeled by method. A
// Reporter is safe for concurrent use.
//
// The method label is bounded to a fixed set. The method comes from the request
// path, the catch-all handler accepts any path, and the reporter runs ahead of
// authentication, so labeling by the raw path would let an unauthenticated
// caller add a series per request until the process runs out of memory. Only
// the methods passed to [NewReporter] and the health service's own are reported
// by name; every other path is reported as [MethodUnknown]. Namespace is never
// a label, for the same reason.
type Reporter struct {
	duration *prometheus.HistogramVec
	requests *prometheus.CounterVec
	panics   *prometheus.CounterVec
	labels   metrics.MetadataLabels
	methods  map[string]struct{}
}

// NewReporter builds the Prometheus-backed Reporter. f must already be scoped to
// the "server" subsystem by the caller. labels are the configured metadata
// labels carried on every collector, and may be the zero value. methods are the
// gRPC full methods ("/pkg.Service/Method") reported by name; the health
// service's methods are always added, and any other method is reported as
// [MethodUnknown].
func NewReporter(f *metrics.Factory, labels metrics.MetadataLabels, methods []string) *Reporter {
	known := make(map[string]struct{}, len(methods))
	for _, m := range methods {
		known[m] = struct{}{}
	}

	health := grpc_health_v1.Health_ServiceDesc
	for _, m := range health.Methods {
		known["/"+health.ServiceName+"/"+m.MethodName] = struct{}{}
	}

	for _, s := range health.Streams {
		known["/"+health.ServiceName+"/"+s.StreamName] = struct{}{}
	}

	return &Reporter{
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "request_duration_seconds",
			Help:    "Time spent serving an RPC end to end, labeled by method.",
			Buckets: durationBuckets,
		}, append([]string{"method"}, labels.Names()...)),
		requests: f.NewCounter(prometheus.CounterOpts{
			Name: "requests_total",
			Help: "Total RPCs served, labeled by method and gRPC status code.",
		}, append([]string{"method", "code"}, labels.Names()...)),
		panics: f.NewCounter(prometheus.CounterOpts{
			Name: "panics_total",
			Help: "Total panics recovered while serving an RPC, labeled by method.",
		}, append([]string{"method"}, labels.Names()...)),
		labels:  labels,
		methods: known,
	}
}

// Panic counts one panic recovered serving method. ctx is the request's, and
// supplies the configured metadata label values.
func (r *Reporter) Panic(ctx context.Context, method string) {
	r.panics.WithLabelValues(append([]string{r.method(method)}, r.labels.AppendValues(ctx, nil)...)...).Inc()
}

// Observe records one completed RPC: its duration on the method histogram and a
// count on the (method, code) counter. ctx is the request's, and supplies the
// configured metadata label values; the two collectors declare different label
// sets of their own, so the values are resolved once and appended to each.
func (r *Reporter) Observe(ctx context.Context, method string, code codes.Code, d time.Duration) {
	method = r.method(method)
	labels := r.labels.AppendValues(ctx, nil)
	r.duration.WithLabelValues(append([]string{method}, labels...)...).Observe(d.Seconds())
	r.requests.WithLabelValues(append([]string{method, code.String()}, labels...)...).Inc()
}

// method returns the label value for a request's full method: the method itself
// when it is one the Reporter knows, [MethodUnknown] otherwise.
func (r *Reporter) method(fullMethod string) string {
	if _, ok := r.methods[fullMethod]; ok {
		return fullMethod
	}

	return MethodUnknown
}

// StreamInterceptor returns a stream server interceptor that times the handler
// and records the RPC's duration and final gRPC status code. It covers all
// forwarded traffic, which grpc-go serves through the unknown-service handler as
// streams. The local health service's unary Check is not metered, since unary
// calls do not pass through a stream interceptor; its streaming Watch is
// metered under its own method name, which includes the call the loopback
// health check makes every interval when [WithLoopbackHealthCheck] is set.
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
