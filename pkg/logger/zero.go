package logger

import (
	"io"

	"github.com/rs/zerolog"

	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

type (
	// ZeroLogger is a [Logger] backed by [zerolog]. It writes structured,
	// leveled JSON log entries, each stamped with a timestamp.
	ZeroLogger struct {
		log zerolog.Logger
	}
)

// NewZeroLogger returns a [ZeroLogger] that writes to w and discards any entry
// below lvl.
func NewZeroLogger(w io.Writer, lvl Level) *ZeroLogger {
	return &ZeroLogger{
		log: zerolog.New(w).Level(zeroLevel(lvl)).With().Timestamp().Logger(),
	}
}

func (l *ZeroLogger) Debug(msg string, tags ...tag.Tag) {
	logEvent(l.log.Debug(), msg, tags)
}

func (l *ZeroLogger) Error(msg string, tags ...tag.Tag) {
	logEvent(l.log.Error(), msg, tags)
}

func (l *ZeroLogger) Fatal(msg string, tags ...tag.Tag) {
	logEvent(l.log.Fatal(), msg, tags)
}

func (l *ZeroLogger) Info(msg string, tags ...tag.Tag) {
	logEvent(l.log.Info(), msg, tags)
}

func (l *ZeroLogger) Warn(msg string, tags ...tag.Tag) {
	logEvent(l.log.Warn(), msg, tags)
}

func (l *ZeroLogger) With(tags ...tag.Tag) Logger {
	ctx := l.log.With()
	for _, t := range tags {
		ctx = ctx.Interface(t.Key, t.Value)
	}

	return &ZeroLogger{log: ctx.Logger()}
}

func logEvent(e *zerolog.Event, msg string, tags []tag.Tag) {
	for _, t := range tags {
		e.Interface(t.Key, t.Value)
	}

	e.Msg(msg)
}

func zeroLevel(l Level) zerolog.Level {
	switch l {
	case LevelDebug:
		return zerolog.DebugLevel
	case LevelWarn:
		return zerolog.WarnLevel
	case LevelError:
		return zerolog.ErrorLevel
	default:
		return zerolog.InfoLevel
	}
}
