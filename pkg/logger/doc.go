// Package logger provides a small, leveled, structured logging interface for
// the proxy along with a [zerolog]-backed implementation, plus a no-op
// implementation and a recording [TestLogger] for tests.
//
// Package-level functions ([Debug], [Info], [Warn], [Error], [Fatal], [With])
// delegate to a default [Logger] that writes to os.Stderr at [LevelInfo]. The
// default is replaceable via [SetDefault].
package logger
