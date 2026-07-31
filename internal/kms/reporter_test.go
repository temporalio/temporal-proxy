package kms_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

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
	r.Observe(crypto.CacheEvent{Hit: false, Size: 0})
	r.Observe(crypto.CacheEvent{Hit: true, Size: 1})
	r.Observe(crypto.CacheEvent{Hit: true, Size: 2})

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

func TestReporterEnvelopeOp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		event         crypto.EnvelopeEvent
		operation     string
		wantDurCount  uint64
		wantSuccesses float64
		wantErrors    float64
	}{
		{
			name:          "encrypt records the AES step",
			event:         crypto.EnvelopeEvent{Op: crypto.OpEncrypt, Crypto: 250 * time.Microsecond, Total: 3 * time.Millisecond},
			operation:     "encrypt",
			wantDurCount:  1,
			wantSuccesses: 1,
		},
		{
			name: "decrypt with a CryptoErr counts as a failed AES step",
			// AES was attempted and timed even though it failed, so the histogram
			// still records it and the counter reports the failure.
			event:        crypto.EnvelopeEvent{Op: crypto.OpDecrypt, Crypto: 100 * time.Microsecond, Total: time.Millisecond, CryptoErr: crypto.ErrMalformedCipherText, Err: crypto.ErrMalformedCipherText},
			operation:    "decrypt",
			wantDurCount: 1,
			wantErrors:   1,
		},
		{
			name: "encrypt whose KEK fails to wrap still counts as an AES success",
			// Err is set because the overall Seal failed, but CryptoErr is nil
			// because AES itself succeeded: the KEK wrap is what failed, and that
			// failure belongs to kek_ops_total, not here.
			event:         crypto.EnvelopeEvent{Op: crypto.OpEncrypt, Crypto: 250 * time.Microsecond, Total: 3 * time.Millisecond, Err: errors.New("kms unavailable")},
			operation:     "encrypt",
			wantDurCount:  1,
			wantSuccesses: 1,
		},
		{
			name: "a failure before AES records nothing",
			// Crypto is zero and CryptoErr is nil, the only combination meaning AES
			// was never reached. Recording that zero would drag the quantiles toward
			// nothing, and there is no AES result to count either.
			event:        crypto.EnvelopeEvent{Op: crypto.OpDecrypt, Err: errors.New("unknown key"), Total: 2 * time.Millisecond},
			operation:    "decrypt",
			wantDurCount: 0,
		},
		{
			name: "an AES failure counts even with an unmeasurably short duration",
			// Whether AES ran is decided by CryptoErr, not by the duration, so this
			// failure is counted. Its duration is still withheld from the histogram,
			// which is why the count and the histogram's sample count can differ.
			event:        crypto.EnvelopeEvent{Op: crypto.OpDecrypt, CryptoErr: crypto.ErrMalformedCipherText, Err: crypto.ErrMalformedCipherText, Total: time.Millisecond},
			operation:    "decrypt",
			wantDurCount: 0,
			wantErrors:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, reg := newTestReporter(t)
			r.Observe(tt.event)

			mf := gather(t, reg, "proxy_encryption_dek_ops_duration_secs")
			require.NotNil(t, mf)
			m := findMetric(t, mf, map[string]string{"operation": tt.operation})
			require.Equal(t, tt.wantDurCount, m.GetHistogram().GetSampleCount())

			ops := gather(t, reg, "proxy_encryption_dek_ops_total")
			require.NotNil(t, ops)
			success := findMetric(t, ops, map[string]string{"operation": tt.operation, "result": "success"})
			require.Equal(t, tt.wantSuccesses, success.GetCounter().GetValue())
			failure := findMetric(t, ops, map[string]string{"operation": tt.operation, "result": "error"})
			require.Equal(t, tt.wantErrors, failure.GetCounter().GetValue())
		})
	}
}

func TestReporterRotated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		reason crypto.RotationReason
		want   string
	}{
		{name: "scheduled", reason: crypto.RotationScheduled, want: "scheduled"},
		{name: "on demand", reason: crypto.RotationOnDemand, want: "on_demand"},
		{name: "initial", reason: crypto.RotationInitial, want: "initial"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, reg := newTestReporter(t)
			r.Observe(crypto.RotationEvent{Namespace: "ns1", Reason: tt.reason})

			mf := gather(t, reg, "proxy_encryption_dek_rotations_total")
			require.NotNil(t, mf)
			m := findMetric(t, mf, map[string]string{"reason": tt.want})
			require.Equal(t, 1.0, m.GetCounter().GetValue())
		})
	}
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
