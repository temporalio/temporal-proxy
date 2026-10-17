package logger

import "go.temporal.io/proxy/pkg/logger/tag"

type (
	// NoopLogger is a [Logger] that discards every entry. It is useful in tests
	// and anywhere a non-nil Logger is required but output is not wanted. Its
	// Fatal does not exit the process.
	NoopLogger struct{}
)

// NewNoopLogger returns a [NoopLogger].
func NewNoopLogger() *NoopLogger {
	return new(NoopLogger)
}

// Debug implements [Logger] by discarding the entry.
func (n *NoopLogger) Debug(string, ...tag.Tag) {
}

// Enabled implements [Logger]. NoopLogger discards every entry, whatever the
// level, so nothing it could log is worth building.
func (n *NoopLogger) Enabled(Level) bool {
	return false
}

// Error implements [Logger] by discarding the entry.
func (n *NoopLogger) Error(string, ...tag.Tag) {
}

// Fatal implements [Logger] by discarding the entry. It does not exit.
func (n *NoopLogger) Fatal(string, ...tag.Tag) {
}

// Info implements [Logger] by discarding the entry.
func (n *NoopLogger) Info(string, ...tag.Tag) {
}

// Warn implements [Logger] by discarding the entry.
func (n *NoopLogger) Warn(string, ...tag.Tag) {
}

// With implements [Logger] by returning the receiver; tags are discarded.
func (n *NoopLogger) With(...tag.Tag) Logger {
	return n
}
