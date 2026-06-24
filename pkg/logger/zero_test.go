package logger_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

func TestZeroLoggerLevels(t *testing.T) {
	t.Parallel()

	// Logging at a level below the configured threshold should be discarded;
	// at or above it should be emitted.
	tests := []struct {
		name    string
		log     func(l logger.Logger)
		level   string // level field zerolog writes for the entry
		emitted bool
	}{
		{"debug below warn", func(l logger.Logger) { l.Debug("m") }, "debug", false},
		{"info below warn", func(l logger.Logger) { l.Info("m") }, "info", false},
		{"warn at warn", func(l logger.Logger) { l.Warn("m") }, "warn", true},
		{"error above warn", func(l logger.Logger) { l.Error("m") }, "error", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			l := logger.NewZeroLogger(&buf, logger.LevelWarn)
			tt.log(l)

			if !tt.emitted {
				require.Empty(t, buf.String())
				return
			}

			entry := decode(t, buf.Bytes())
			require.Equal(t, tt.level, entry["level"])
			require.Equal(t, "m", entry["message"])
		})
	}
}

func TestZeroLoggerTags(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	l := logger.NewZeroLogger(&buf, logger.LevelDebug)
	l.Info("hello", tag.String("foo", "bar"))

	entry := decode(t, buf.Bytes())
	require.Equal(t, "hello", entry["message"])
	require.Equal(t, "bar", entry["foo"])
}

func TestZeroLoggerWith(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	parent := logger.NewZeroLogger(&buf, logger.LevelDebug)
	child := parent.With(tag.String("component", "test"))
	child.Info("hello", tag.String("extra", "1"))

	entry := decode(t, buf.Bytes())
	require.Equal(t, "test", entry["component"], "With tags must appear on child entries")
	require.Equal(t, "1", entry["extra"])

	// The parent must be unaffected by With.
	buf.Reset()
	parent.Info("again")
	entry = decode(t, buf.Bytes())
	require.NotContains(t, entry, "component")
}

// decode parses a single zerolog JSON line into a map.
func decode(t *testing.T, b []byte) map[string]any {
	t.Helper()

	var entry map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(b), &entry))
	return entry
}
