package logger_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
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
}
