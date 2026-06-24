package logger_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

func TestTestLoggerContains(t *testing.T) {
	t.Parallel()

	l := logger.NewTestLogger()
	require.False(t, l.Contains("hello"))

	l.Info("hello")
	require.True(t, l.Contains("hello"))
	require.False(t, l.Contains("goodbye"))
}

func TestTestLoggerContainsEntry(t *testing.T) {
	t.Parallel()

	l := logger.NewTestLogger()
	l.Warn("watch out", tag.String("k", "v"))

	tests := []struct {
		name  string
		level logger.Level
		msg   string
		tags  []tag.Tag
		want  bool
	}{
		{"exact match", logger.LevelWarn, "watch out", []tag.Tag{tag.String("k", "v")}, true},
		{"wrong level", logger.LevelError, "watch out", []tag.Tag{tag.String("k", "v")}, false},
		{"wrong message", logger.LevelWarn, "other", []tag.Tag{tag.String("k", "v")}, false},
		{"wrong tag value", logger.LevelWarn, "watch out", []tag.Tag{tag.String("k", "x")}, false},
		{"missing tag", logger.LevelWarn, "watch out", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, l.ContainsEntry(tt.level, tt.msg, tt.tags...))
		})
	}
}

func TestTestLoggerLevels(t *testing.T) {
	t.Parallel()

	l := logger.NewTestLogger()
	l.Debug("d")
	l.Info("i")
	l.Warn("w")
	l.Error("e")

	require.True(t, l.ContainsEntry(logger.LevelDebug, "d"))
	require.True(t, l.ContainsEntry(logger.LevelInfo, "i"))
	require.True(t, l.ContainsEntry(logger.LevelWarn, "w"))
	require.True(t, l.ContainsEntry(logger.LevelError, "e"))
}

func TestTestLoggerFatal(t *testing.T) {
	t.Parallel()

	l := logger.NewTestLogger()

	// Fatal must record (at LevelError) without exiting the process.
	require.NotPanics(t, func() { l.Fatal("boom") })
	require.True(t, l.ContainsEntry(logger.LevelError, "boom"))
}

func TestTestLoggerWithSharesStore(t *testing.T) {
	t.Parallel()

	parent := logger.NewTestLogger()
	child := parent.With(tag.String("component", "test"))
	child.Info("from child", tag.String("extra", "1"))

	// The entry is visible from the parent and carries both the With tag and
	// the per-call tag, in order.
	require.True(t, parent.Contains("from child"))
	require.True(t, parent.ContainsEntry(
		logger.LevelInfo,
		"from child",
		tag.String("component", "test"),
		tag.String("extra", "1"),
	))
}

func TestTestLoggerWithChains(t *testing.T) {
	t.Parallel()

	root := logger.NewTestLogger()
	root.With(tag.String("a", "1")).With(tag.String("b", "2")).Info("chained")

	require.True(t, root.ContainsEntry(
		logger.LevelInfo,
		"chained",
		tag.String("a", "1"),
		tag.String("b", "2"),
	))
}

func TestTestLoggerWithDoesNotMutateParentTags(t *testing.T) {
	t.Parallel()

	parent := logger.NewTestLogger()
	_ = parent.With(tag.String("child", "only"))

	parent.Info("from parent")

	// The parent's own entries must not pick up tags added to the child.
	require.False(t, parent.ContainsEntry(
		logger.LevelInfo,
		"from parent",
		tag.String("child", "only"),
	))
	require.True(t, parent.ContainsEntry(logger.LevelInfo, "from parent"))
}

func TestTestLoggerConcurrent(t *testing.T) {
	t.Parallel()

	l := logger.NewTestLogger()

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			l.Info("concurrent")
		})
	}
	wg.Wait()

	require.True(t, l.Contains("concurrent"))
}
