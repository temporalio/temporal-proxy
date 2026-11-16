package metrics_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	goprom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/metrics"
)

func TestNewMetadataLabels(t *testing.T) {
	t.Parallel()

	t.Run("no labels yields the zero value", func(t *testing.T) {
		t.Parallel()

		labels := metrics.NewMetadataLabels(nil)
		require.Zero(t, labels.Len())
		require.Empty(t, labels.Names())
	})

	t.Run("labels keep configured order and case", func(t *testing.T) {
		t.Parallel()

		labels := metrics.NewMetadataLabels([]config.MetricLabel{
			{Header: "x-tenant", Name: "Tenant"},
			{Header: "x-region", Name: "region"},
		})

		require.Equal(t, 2, labels.Len())
		require.Equal(t, []string{"Tenant", "region"}, labels.Names())
	})

	t.Run("Labels returns a copy", func(t *testing.T) {
		t.Parallel()

		labels := metrics.NewMetadataLabels([]config.MetricLabel{{Header: "x-tenant", Name: "tenant"}})

		got := labels.Names()
		got[0] = "mutated"
		require.Equal(t, []string{"tenant"}, labels.Names())
	})
}

func TestMetadataLabels_AppendValues(t *testing.T) {
	t.Parallel()

	labels := metrics.NewMetadataLabels([]config.MetricLabel{
		{Header: "x-tenant", Name: "tenant"},
		{Header: "x-region", Name: "region"},
	})

	tests := []struct {
		name string
		md   metadata.MD // nil means no incoming metadata at all
		want []string
	}{
		{
			name: "both headers present",
			md:   metadata.Pairs("x-tenant", "acme", "x-region", "us-east"),
			want: []string{"acme", "us-east"},
		},
		{
			name: "values follow label order, not metadata order",
			md:   metadata.Pairs("x-region", "us-east", "x-tenant", "acme"),
			want: []string{"acme", "us-east"},
		},
		{
			// gRPC canonicalizes keys to lowercase, so a header the config wrote
			// in mixed case still has to match.
			name: "a mixed-case header on the wire still matches",
			md:   metadata.Pairs("X-Tenant", "acme"),
			want: []string{"acme", ""},
		},
		{
			name: "an absent header yields an empty value",
			md:   metadata.Pairs("x-tenant", "acme"),
			want: []string{"acme", ""},
		},
		{
			name: "no incoming metadata yields empty values",
			md:   nil,
			want: []string{"", ""},
		},
		{
			// Matches how the proxy reduces multi-valued metadata elsewhere, e.g.
			// the template context in internal/proxy.
			name: "the last of several values wins",
			md:   metadata.Pairs("x-tenant", "first", "x-tenant", "last"),
			want: []string{"last", ""},
		},
		{
			// client_golang panics on a label value that is not valid UTF-8, and
			// gRPC does not police the bytes of a textual header, so a caller
			// could otherwise crash the process on demand.
			name: "a value that is not valid UTF-8 is dropped",
			md:   metadata.Pairs("x-tenant", string([]byte{0xff, 0xfe}), "x-region", "us-east"),
			want: []string{"", "us-east"},
		},
		{
			// The guard applies to whichever value the last-wins rule picked, so
			// a bad value last is cleared rather than falling back to a good one
			// earlier in the list.
			name: "an earlier valid value does not rescue an invalid last one",
			md:   metadata.Pairs("x-tenant", "acme", "x-tenant", string([]byte{0xff})),
			want: []string{"", ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if tt.md != nil {
				ctx = metadata.NewIncomingContext(ctx, tt.md)
			}

			require.Equal(t, tt.want, labels.AppendValues(ctx, nil))
		})
	}
}

func TestMetadataLabels_AppendValuesExtendsTheCallersSlice(t *testing.T) {
	t.Parallel()

	labels := metrics.NewMetadataLabels([]config.MetricLabel{{Header: "x-tenant", Name: "tenant"}})
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-tenant", "acme"))

	require.Equal(t, []string{"method", "code", "acme"}, labels.AppendValues(ctx, []string{"method", "code"}))
}

func TestMetadataLabels_AppendValuesWithNoneLeavesTheSliceAlone(t *testing.T) {
	t.Parallel()

	// The fast path every deployment that configures no labels takes: the label
	// list is handed back exactly as it arrived.
	var labels metrics.MetadataLabels

	require.Equal(t, []string{"method"}, labels.AppendValues(t.Context(), []string{"method"}))
	require.Nil(t, labels.AppendValues(t.Context(), nil))
}

func TestMetadataLabels_AppendValuesTruncatesOnARuneBoundary(t *testing.T) {
	t.Parallel()

	labels := metrics.NewMetadataLabels([]config.MetricLabel{{Header: "x-tenant", Name: "tenant"}})

	// Each euro sign is three bytes, so a cut at 256 bytes lands mid-rune and
	// the partial tail has to be dropped rather than reported.
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-tenant", strings.Repeat("€", 200)))

	got := labels.AppendValues(ctx, nil)
	require.Len(t, got, 1)
	require.True(t, utf8.ValidString(got[0]), "a truncated value must still be valid UTF-8")
	require.Equal(t, strings.Repeat("€", 85), got[0], "expected a whole number of runes within the cap")
}

func TestMetadataLabels_AppendValuesBoundsDistinctValues(t *testing.T) {
	t.Parallel()

	labels := metrics.NewMetadataLabels([]config.MetricLabel{{Header: "x-tenant", Name: "tenant"}})
	value := func(ls metrics.MetadataLabels, v string) string {
		ctx := t.Context()
		if v != "" {
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-tenant", v))
		}

		return ls.AppendValues(ctx, nil)[0]
	}

	for i := range metrics.MaxLabelValues {
		v := fmt.Sprintf("tenant-%d", i)
		require.Equal(t, v, value(labels, v), "values within the bound report as themselves")
	}

	require.Equal(t, metrics.LabelValueOther, value(labels, "one-too-many"),
		"a value past the bound must collapse rather than add a series")
	require.Equal(t, "tenant-0", value(labels, "tenant-0"), "a value already seen keeps reporting")
	require.Empty(t, value(labels, ""), "an absent header reports empty and does not count against the bound")

	// Copies share what has been seen, so handing the same labels to several
	// reporters does not multiply the bound.
	cp := labels
	require.Equal(t, metrics.LabelValueOther, value(cp, "via-a-copy"))
}

func TestMetadataLabels_AppendValuesBoundHoldsUnderConcurrency(t *testing.T) {
	t.Parallel()

	labels := metrics.NewMetadataLabels([]config.MetricLabel{{Header: "x-tenant", Name: "tenant"}})

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		reported = map[string]struct{}{}
	)

	for g := range 8 {
		wg.Go(func() {
			for i := range 100 {
				ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-tenant", fmt.Sprintf("%d-%d", g, i)))
				v := labels.AppendValues(ctx, nil)[0]
				mu.Lock()
				reported[v] = struct{}{}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	// Every admitted value plus the overflow value.
	require.Len(t, reported, metrics.MaxLabelValues+1)
}

// BenchmarkMetadataLabels_AppendValuesOverflow measures the path a flood of
// never-seen values takes once a label is full, which is what an attacker
// drives.
func BenchmarkMetadataLabels_AppendValuesOverflow(b *testing.B) {
	labels := metrics.NewMetadataLabels([]config.MetricLabel{{Header: "x-tenant", Name: "tenant"}})
	for i := range metrics.MaxLabelValues {
		labels.AppendValues(metadata.NewIncomingContext(b.Context(), metadata.Pairs("x-tenant", fmt.Sprint(i))), nil)
	}

	// Distinct contexts per goroutine, built up front so the loop measures
	// admission rather than metadata construction.
	ctxs := make([]context.Context, 1024)
	for i := range ctxs {
		ctxs[i] = metadata.NewIncomingContext(b.Context(), metadata.Pairs("x-tenant", fmt.Sprintf("junk-%d", i)))
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]string, 0, 1)
		i := 0
		for pb.Next() {
			buf = labels.AppendValues(ctxs[i%len(ctxs)], buf[:0])
			i++
		}
	})
}

func TestWithFixedLabels(t *testing.T) {
	t.Parallel()

	t.Run("stamps every label onto a collector registered through it", func(t *testing.T) {
		t.Parallel()

		reg := goprom.NewRegistry()
		f := metrics.New("test", promauto.With(
			metrics.WithFixedLabels(reg, map[string]string{"region": "us-west-2"}),
		))

		f.NewCounter(goprom.CounterOpts{
			Name: "ops_total",
			Help: "Total operations.",
		}, []string{"operation"}).WithLabelValues("wrap").Inc()

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP test_ops_total Total operations.
# TYPE test_ops_total counter
test_ops_total{operation="wrap",region="us-west-2"} 1
`), "test_ops_total"))
	})

	t.Run("leaves a collector's own labels alone when none are configured", func(t *testing.T) {
		t.Parallel()

		reg := goprom.NewRegistry()
		f := metrics.New("test", promauto.With(metrics.WithFixedLabels(reg, nil)))

		f.NewCounter(goprom.CounterOpts{
			Name: "ops_total",
			Help: "Total operations.",
		}, []string{"operation"}).WithLabelValues("wrap").Inc()

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP test_ops_total Total operations.
# TYPE test_ops_total counter
test_ops_total{operation="wrap"} 1
`), "test_ops_total"))
	})

	// The runtime's own go_* and process_* collectors register into the registry
	// directly rather than through the Factory, which is why they stay bare while
	// everything the proxy declares carries the labels.
	t.Run("leaves a collector registered around it bare", func(t *testing.T) {
		t.Parallel()

		reg := goprom.NewRegistry()
		fixed := map[string]string{"region": "us-west-2"}

		metrics.New("test", promauto.With(metrics.WithFixedLabels(reg, fixed))).
			NewCounter(goprom.CounterOpts{Name: "through_total", Help: "Through the factory."}, nil).
			WithLabelValues().Inc()

		promauto.With(reg).
			NewCounter(goprom.CounterOpts{Name: "test_around_total", Help: "Around it."}).Inc()

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP test_around_total Around it.
# TYPE test_around_total counter
test_around_total 1
# HELP test_through_total Through the factory.
# TYPE test_through_total counter
test_through_total{region="us-west-2"} 1
`), "test_around_total", "test_through_total"))
	})
}
