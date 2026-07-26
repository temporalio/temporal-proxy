package kms_test

import (
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/kms"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

func TestReporterKEKOp(t *testing.T) {
	t.Parallel()

	r, reg := newTestReporter(t)
	r.KEKOp("aws", "wrap", "success", 0.02)

	calls := gather(t, reg, "proxy_encryption_kek_ops_total")
	require.NotNil(t, calls)
	m := findMetric(t, calls, map[string]string{"provider": "aws", "operation": "wrap", "result": "success"})
	require.Equal(t, 1.0, m.GetCounter().GetValue())

	dur := gather(t, reg, "proxy_encryption_kek_ops_duration_secs")
	require.NotNil(t, dur)
	d := findMetric(t, dur, map[string]string{"provider": "aws", "operation": "wrap"})
	require.Equal(t, uint64(1), d.GetHistogram().GetSampleCount())
}

func TestReporterCacheEvents(t *testing.T) {
	t.Parallel()

	r, reg := newTestReporter(t)
	r.CacheMiss(crypto.CacheEvent{Size: 0})
	r.CacheHit(crypto.CacheEvent{Size: 1})
	r.CacheHit(crypto.CacheEvent{Size: 2})

	hits := gather(t, reg, "proxy_encryption_dek_cache_hits_total")
	require.NotNil(t, hits)
	require.Equal(t, 2.0, hits.GetMetric()[0].GetCounter().GetValue())

	misses := gather(t, reg, "proxy_encryption_dek_cache_misses_total")
	require.NotNil(t, misses)
	require.Equal(t, 1.0, misses.GetMetric()[0].GetCounter().GetValue())

	size := gather(t, reg, "proxy_encryption_dek_cache_size")
	require.NotNil(t, size)
	require.Equal(t, 2.0, size.GetMetric()[0].GetGauge().GetValue()) // last Set wins
}

func gather(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	return nil
}

func findMetric(t *testing.T, mf *dto.MetricFamily, labels map[string]string) *dto.Metric {
	t.Helper()
	for _, m := range mf.GetMetric() {
		got := map[string]string{}
		for _, l := range m.GetLabel() {
			got[l.GetName()] = l.GetValue()
		}
		if reflect.DeepEqual(got, labels) {
			return m
		}
	}
	require.Failf(t, "metric not found", "no metric with labels %v in %s", labels, mf.GetName())
	return nil
}

func newTestReporter(t *testing.T) (*kms.Reporter, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	return kms.NewReporter(metrics.New("proxy", promauto.With(reg)).ForSubsystem("encryption")), reg
}
