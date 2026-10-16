package logger_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
)

func TestNoopLogger(t *testing.T) {
	t.Parallel()

	l := logger.NewNoopLogger()

	// None of these should panic or produce output.
	require.NotPanics(t, func() {
		l.Debug("d", tag.String("k", "v"))
		l.Info("i")
		l.Warn("w")
		l.Error("e")
		l.Fatal("f") // must not exit the process
	})

	require.Same(t, l, l.With(tag.String("k", "v")))

	// Enabled reports false at every level, recognized or not, because every
	// entry is discarded.
	require.False(t, l.Enabled(logger.LevelDebug))
	require.False(t, l.Enabled(logger.Level(42)))
}
