package metrics_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

func TestProvider_RegisterGauge(t *testing.T) {
	t.Parallel()

	t.Run("returns a usable handle", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
		h := p.RegisterGauge("test_gauge", "A test gauge", "cluster")
		h.Inc("cluster", "us-east-1")
		h.Add(2, "cluster", "us-east-1") // gauge = 3
		h.Dec("cluster", "us-east-1")    // gauge = 2

		expected := `
# HELP test_gauge A test gauge
# TYPE test_gauge gauge
test_gauge{cluster="us-east-1"} 2
`
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), "test_gauge"))
	})

	t.Run("duplicate registration shares the same series", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
		h1 := p.RegisterGauge("dup_gauge", "A gauge", "label")
		h2 := p.RegisterGauge("dup_gauge", "A gauge", "label")

		h1.Inc("label", "a")
		h2.Inc("label", "a")

		expected := `
# HELP dup_gauge A gauge
# TYPE dup_gauge gauge
dup_gauge{label="a"} 2
`
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), "dup_gauge"))
	})

	t.Run("invalid labels are a no-op", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
		h := p.RegisterGauge("noop_gauge", "help", "cluster")

		h.Inc("cluster")                    // odd-length kvs
		h.Inc()                             // wrong count (0 pairs for 1 label)
		h.Inc("cluster", "a", "extra", "b") // too many pairs
		h.Inc("wrong", "val")               // unknown key
		h.Dec("wrong", "val")
		h.Set(1.0, "wrong", "val")
		h.Add(1.0, "wrong", "val")

		// duplicate key: "a" appears twice, so "b" is never set
		h2 := p.RegisterGauge("noop_gauge2", "help", "a", "b")
		h2.Inc("a", "1", "a", "2")

		mfs, err := reg.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			require.Empty(t, mf.GetMetric(), "expected no observations for %s", mf.GetName())
		}
	})
}

func TestProvider_RegisterCounter(t *testing.T) {
	t.Parallel()

	t.Run("records increments and additions", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
		h := p.RegisterCounter("test_counter", "A test counter", "operation")
		h.Inc("operation", "StartWorkflow")
		h.Add(4, "operation", "StartWorkflow")

		expected := `
# HELP test_counter A test counter
# TYPE test_counter counter
test_counter{operation="StartWorkflow"} 5
`
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), "test_counter"))
	})

	t.Run("duplicate registration shares the same series", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
		h1 := p.RegisterCounter("dup_counter", "A counter", "label")
		h2 := p.RegisterCounter("dup_counter", "A counter", "label")

		h1.Inc("label", "a")
		h2.Inc("label", "a")

		expected := `
# HELP dup_counter A counter
# TYPE dup_counter counter
dup_counter{label="a"} 2
`
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), "dup_counter"))
	})

	t.Run("invalid labels are a no-op", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
		h := p.RegisterCounter("noop_counter", "help", "cluster")
		h.Inc("wrong", "val")
		h.Add(1.0, "wrong", "val")

		mfs, err := reg.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() == "noop_counter" {
				require.Empty(t, mf.GetMetric())
			}
		}
	})
}

func TestProvider_RegisterHistogram(t *testing.T) {
	t.Parallel()

	t.Run("records observations", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
		h := p.RegisterHistogram("test_histogram", "A test histogram", []float64{0.1, 0.5, 1.0}, "cluster")
		h.Observe(0.3, "cluster", "us-east-1")

		mfs, err := reg.Gather()
		require.NoError(t, err)
		require.Len(t, mfs, 1)
		require.Equal(t, "test_histogram", mfs[0].GetName())
		require.Equal(t, uint64(1), mfs[0].GetMetric()[0].GetHistogram().GetSampleCount())
	})

	t.Run("duplicate registration shares the same series", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
		buckets := []float64{0.1, 0.5, 1.0}
		h1 := p.RegisterHistogram("dup_histogram", "A histogram", buckets, "label")
		h2 := p.RegisterHistogram("dup_histogram", "A histogram", buckets, "label")

		h1.Observe(0.3, "label", "a")
		h2.Observe(0.7, "label", "a")

		mfs, err := reg.Gather()
		require.NoError(t, err)
		require.Len(t, mfs, 1)
		require.Equal(t, uint64(2), mfs[0].GetMetric()[0].GetHistogram().GetSampleCount())
	})

	t.Run("invalid labels are a no-op", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		p := metrics.NewProvider(metrics.Logger(log.NewNoopLogger()), metrics.Registerer(reg))
		h := p.RegisterHistogram("noop_histogram", "help", []float64{0.1}, "cluster")
		h.Observe(0.3, "wrong", "val")

		mfs, err := reg.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() == "noop_histogram" {
				require.Empty(t, mf.GetMetric())
			}
		}
	})
}
