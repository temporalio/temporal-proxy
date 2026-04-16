package transport

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/yamux"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type LogLevelCap int

const (
	DemoteToDebug LogLevelCap = iota
	DemoteToInfo  LogLevelCap = iota
	DemoteToWarn  LogLevelCap = iota
	DemoteToError LogLevelCap = iota
	NoDemote      LogLevelCap = iota
)

// yamuxLogger converts a temporal Logger to a Yamux logger
type yamuxLogger struct {
	maxLogLevel LogLevelCap
	logger      log.Logger
}

// registerYamuxObserverBuilder makes a closure with the muxCategory and logger so that sessions belonging to the same
// GRPCMux all emit the same metrics together
func registerYamuxObserverBuilder(muxCategory string, logger log.Logger) SessionBuilder {
	return func(ctx context.Context, id string, session *yamux.Session) {
		go emitYamuxMetrics(ctx, muxCategory, id, session, logger)
	}
}

// emitYamuxMetrics creates a loop that pings the provided yamux session repeatedly and gathers its two
// metrics: Whether the server is alive and how many streams it has open. Intended for use as a goroutine.
func emitYamuxMetrics(ctx context.Context, _muxCategory string, id string, session *yamux.Session, logger log.Logger) {
	host, _, err := net.SplitHostPort(session.RemoteAddr().String())
	if err != nil {
		host = session.RemoteAddr().String()
	}

	logger.Info("mux session watcher starting",
		tag.String("remote_addr", host),
		tag.String("local_addr", session.LocalAddr().String()),
		tag.String("mux_id", id))

	if session == nil {
		// If we got a null session, we can't even generate tags to report
		return
	}

	var sessionActive int8 = 1
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for sessionActive == 1 {
		// Prometheus gauges are cheap, but Session.NumStreams() takes a mutex in the session! Only check once per minute
		// to minimize overhead
		select {
		case <-ctx.Done():
			sessionActive = 0
		case <-ticker.C:
			// wake up so we can report NumStreams
		}

		// TODO: Metrics should be emitted here (e.g. Ping duration).
		// For now, debug log.
	}
}

// parseLogLevel returns the appropriate log level for a Yamux log string.
// Yamux logs have the format "[<level>] yamux: <message>".
// At time of writing, only ERR and WARN are logged
func (l yamuxLogger) parseLogLevel(s string) {
	if strings.HasPrefix(s, "[ERR]") {
		l.logAtLevel(s, min(l.maxLogLevel, DemoteToError))
	} else if strings.HasPrefix(s, "[WARN]") {
		l.logAtLevel(s, min(l.maxLogLevel, DemoteToWarn))
	} else {
		l.logAtLevel(s, min(l.maxLogLevel, DemoteToInfo))
	}
}

func (l yamuxLogger) logAtLevel(s string, level LogLevelCap) {
	switch level {
	case DemoteToDebug:
		l.logger.Debug(s)
	case DemoteToInfo:
		l.logger.Info(s)
	case DemoteToWarn:
		l.logger.Warn(s)
	case DemoteToError:
		l.logger.Error(s)
	case NoDemote:
		l.logger.Fatal(s)
	}
}

func (l yamuxLogger) Print(v ...any) {
	l.parseLogLevel(fmt.Sprint(v...))
}

func (l yamuxLogger) Printf(format string, v ...any) {
	l.parseLogLevel(fmt.Sprintf(format, v...))
}

func (l yamuxLogger) Println(v ...any) {
	l.parseLogLevel(fmt.Sprintln(v...))
}
