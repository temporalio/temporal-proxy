package logger

import "github.com/temporalio/temporal-proxy/pkg/logger/tag"

type (
	// NoopLogger is a [Logger] that discards every entry. It is useful in tests
	// and anywhere a non-nil Logger is required but output is not wanted. Unlike
	// other implementations, its Fatal does not exit the process.
	NoopLogger struct{}
)

// NewNoopLogger returns a [NoopLogger].
func NewNoopLogger() *NoopLogger {
	return new(NoopLogger)
}

func (n *NoopLogger) Debug(string, ...tag.Tag) {
}

func (n *NoopLogger) Error(string, ...tag.Tag) {
}

func (n *NoopLogger) Fatal(string, ...tag.Tag) {
}

func (n *NoopLogger) Info(string, ...tag.Tag) {
}

func (n *NoopLogger) Warn(string, ...tag.Tag) {
}

func (n *NoopLogger) With(...tag.Tag) Logger {
	return n
}
