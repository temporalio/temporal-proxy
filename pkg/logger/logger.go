package logger

import (
	"os"
	"strings"

	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

const (
	// LevelDebug logs everything, including verbose debugging detail.
	LevelDebug Level = iota
	// LevelInfo logs informational entries and above. It is the default.
	LevelInfo
	// LevelWarn logs warnings and above.
	LevelWarn
	// LevelError logs only errors (and fatals).
	LevelError
)

var defaultLogger Logger = NewZeroLogger(os.Stderr, LevelInfo)

type (
	// Level controls the minimum severity an entry must have to be logged.
	Level byte

	// Logger emits leveled, structured log entries. Each method takes a human
	// readable message and zero or more [tag.Tag] key/value pairs.
	Logger interface {
		// Debug logs msg and tags at [LevelDebug].
		Debug(msg string, tags ...tag.Tag)
		// Error logs msg and tags at [LevelError].
		Error(msg string, tags ...tag.Tag)
		// Fatal logs msg and tags, then typically calls os.Exit(1).
		Fatal(msg string, tags ...tag.Tag)
		// Info logs msg and tags at [LevelInfo].
		Info(msg string, tags ...tag.Tag)
		// Warn logs msg and tags at [LevelWarn].
		Warn(msg string, tags ...tag.Tag)

		// With returns a child Logger that includes tags on every entry.
		With(tags ...tag.Tag) Logger
	}
)

// Default returns a default [Logger] which writes to os.Stderr at LevelInfo.
func Default() Logger {
	return defaultLogger
}

// SetDefault replaces the [Logger] used by the package-level functions. A nil
// logger is ignored. It is not safe to call concurrently with the package-level
// logging functions.
func SetDefault(l Logger) {
	if l != nil {
		defaultLogger = l
	}
}

// ParseLevel converts a level name ("debug", "info", "warn", "error", case
// insensitive) into a [Level]. Unrecognized values default to [LevelInfo].
func ParseLevel(lvl string) Level {
	switch strings.ToLower(lvl) {
	case "debug":
		return LevelDebug
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	}

	return LevelInfo
}

// Debug logs msg and tags at [LevelDebug] using the default logger.
func Debug(msg string, tags ...tag.Tag) {
	defaultLogger.Debug(msg, tags...)
}

// Error logs msg and tags at [LevelError] using the default logger.
func Error(msg string, tags ...tag.Tag) {
	defaultLogger.Error(msg, tags...)
}

// Fatal logs msg and tags using the default logger. Depending on the
// implementation, it may call os.Exit(1).
func Fatal(msg string, tags ...tag.Tag) {
	defaultLogger.Fatal(msg, tags...)
}

// Info logs msg and tags at [LevelInfo] using the default logger.
func Info(msg string, tags ...tag.Tag) {
	defaultLogger.Info(msg, tags...)
}

// Warn logs msg and tags at [LevelWarn] using the default logger.
func Warn(msg string, tags ...tag.Tag) {
	defaultLogger.Warn(msg, tags...)
}

// With returns a child [Logger] derived from the default logger that includes
// tags on every entry.
func With(tags ...tag.Tag) Logger {
	return defaultLogger.With(tags...)
}
