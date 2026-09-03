package router

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

const (
	// upstreamUnknown is the upstream label used for unroutable decisions, where
	// no upstream was chosen.
	upstreamUnknown = "unknown"

	reasonNoConnection = "no_connection"
	reasonStreamSetup  = "stream_setup"
)

type (
	// Reporter records router telemetry to Prometheus: routing decisions and the
	// forwarding failures the router itself originates. It pre-resolves a counter
	// for every meaningful (upstream, outcome) and (upstream, reason) combination
	// so the emit path is a lock-free map read; an unexpected label combination
	// falls back to CounterVec.WithLabelValues. A Reporter is safe for concurrent
	// use.
	//
	// Configured metadata tags suppress that pre-resolution, because their values
	// arrive with a request and cannot be enumerated at startup. Every emit then
	// takes the fallback, and no series starts at zero, so a query for a counter
	// that has not been incremented yet finds nothing rather than 0.
	Reporter struct {
		decisions *prometheus.CounterVec
		errors    *prometheus.CounterVec
		decHandle map[decisionKey]prometheus.Counter
		errHandle map[errorKey]prometheus.Counter
		tags      metrics.Tags
	}

	decisionKey struct {
		upstream string
		outcome  Outcome
	}

	errorKey struct {
		upstream string
		reason   string
	}
)

// NewReporter builds the Prometheus-backed Reporter, registering its collectors
// with the factory's registry and pre-resolving the meaningful label
// combinations so every series starts at zero. upstreams is the configured
// upstream name list. tags are the configured metadata labels, and may be the
// zero value.
//
// Nothing is pre-resolved when tags are configured: a handle would have to fix
// their values, which only a request carries. Leaving the maps empty routes
// every emit through the fallback the maps exist to avoid, rather than adding a
// second path that could drift from it.
func NewReporter(f *metrics.Factory, upstreams []string, tags metrics.Tags) *Reporter {
	decisions := f.NewCounter(prometheus.CounterOpts{
		Name: "decisions_total",
		Help: "Total routing decisions, labeled by chosen upstream and outcome.",
	}, append([]string{"upstream", "outcome"}, tags.Labels()...))

	errors := f.NewCounter(prometheus.CounterOpts{
		Name: "forwarding_errors_total",
		Help: "Total router-originated forwarding failures, labeled by upstream and reason.",
	}, append([]string{"upstream", "reason"}, tags.Labels()...))

	r := &Reporter{
		decisions: decisions,
		errors:    errors,
		decHandle: make(map[decisionKey]prometheus.Counter),
		errHandle: make(map[errorKey]prometheus.Counter),
		tags:      tags,
	}

	if tags.Len() > 0 {
		return r
	}

	outcomes := []Outcome{OutcomeMatch, OutcomeDefault, OutcomeSystem}
	reasons := []string{reasonNoConnection, reasonStreamSetup}
	for _, u := range upstreams {
		for _, o := range outcomes {
			r.decHandle[decisionKey{u, o}] = decisions.WithLabelValues(u, o.String())
		}
		for _, reason := range reasons {
			r.errHandle[errorKey{u, reason}] = errors.WithLabelValues(u, reason)
		}
	}
	r.decHandle[decisionKey{upstreamUnknown, OutcomeUnroutable}] = decisions.WithLabelValues(
		upstreamUnknown,
		OutcomeUnroutable.String(),
	)

	return r
}

// Decision increments the decision counter for the chosen upstream and outcome.
// ctx is the request's, and supplies the configured metadata label values.
func (r *Reporter) Decision(ctx context.Context, upstream string, outcome Outcome) {
	if c, ok := r.decHandle[decisionKey{upstream, outcome}]; ok {
		c.Inc()
		return
	}

	lvs := append([]string{upstream, outcome.String()}, r.tags.AppendValues(ctx, nil)...)
	r.decisions.WithLabelValues(lvs...).Inc()
}

// ForwardingError increments the forwarding-error counter for the upstream and
// reason. ctx is the request's, and supplies the configured metadata label
// values.
func (r *Reporter) ForwardingError(ctx context.Context, upstream, reason string) {
	if c, ok := r.errHandle[errorKey{upstream, reason}]; ok {
		c.Inc()
		return
	}

	lvs := append([]string{upstream, reason}, r.tags.AppendValues(ctx, nil)...)
	r.errors.WithLabelValues(lvs...).Inc()
}
