// Package logger provides a small, leveled, structured logging interface for
// the proxy along with a [zerolog]-backed implementation and a no-op
// implementation for tests.
//
// Package-level functions ([Debug], [Info], [Warn], [Error], [Fatal], [With])
// delegate to a default [Logger] that writes to os.Stderr at [LevelInfo].
package logger
