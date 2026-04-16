package transport

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

func TestYamuxLogger_parseLogLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		maxLevel  LogLevelCap
		input     string
		expectLog func(mock *log.MockLogger, input string)
	}{
		{
			name:     "[ERR] prefix routes to Error",
			maxLevel: NoDemote,
			input:    "[ERR] yamux: something failed",
			expectLog: func(m *log.MockLogger, input string) {
				m.EXPECT().Error(input)
			},
		},
		{
			name:     "[WARN] prefix routes to Warn",
			maxLevel: NoDemote,
			input:    "[WARN] yamux: something suspicious",
			expectLog: func(m *log.MockLogger, input string) {
				m.EXPECT().Warn(input)
			},
		},
		{
			name:     "unknown prefix routes to Info",
			maxLevel: NoDemote,
			input:    "[INFO] yamux: all good",
			expectLog: func(m *log.MockLogger, input string) {
				m.EXPECT().Info(input)
			},
		},
		{
			name:     "maxLevel=DemoteToDebug caps [ERR] to Debug",
			maxLevel: DemoteToDebug,
			input:    "[ERR] yamux: suppressed error",
			expectLog: func(m *log.MockLogger, input string) {
				m.EXPECT().Debug(input)
			},
		},
		{
			name:     "maxLevel=DemoteToDebug caps [WARN] to Debug",
			maxLevel: DemoteToDebug,
			input:    "[WARN] yamux: suppressed warn",
			expectLog: func(m *log.MockLogger, input string) {
				m.EXPECT().Debug(input)
			},
		},
		{
			name:     "maxLevel=DemoteToWarn caps [ERR] to Warn",
			maxLevel: DemoteToWarn,
			input:    "[ERR] yamux: demoted error",
			expectLog: func(m *log.MockLogger, input string) {
				m.EXPECT().Warn(input)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mock := log.NewMockLogger(ctrl)
			tc.expectLog(mock, tc.input)

			l := yamuxLogger{maxLogLevel: tc.maxLevel, logger: mock}
			l.parseLogLevel(tc.input)
		})
	}
}

func TestYamuxLogger_logAtLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		level     LogLevelCap
		expectLog func(mock *log.MockLogger)
	}{
		{"DemoteToDebug calls Debug", DemoteToDebug, func(m *log.MockLogger) { m.EXPECT().Debug("msg") }},
		{"DemoteToInfo calls Info", DemoteToInfo, func(m *log.MockLogger) { m.EXPECT().Info("msg") }},
		{"DemoteToWarn calls Warn", DemoteToWarn, func(m *log.MockLogger) { m.EXPECT().Warn("msg") }},
		{"DemoteToError calls Error", DemoteToError, func(m *log.MockLogger) { m.EXPECT().Error("msg") }},
		{"NoDemote calls Fatal", NoDemote, func(m *log.MockLogger) { m.EXPECT().Fatal("msg") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mock := log.NewMockLogger(ctrl)
			tc.expectLog(mock)

			l := yamuxLogger{maxLogLevel: NoDemote, logger: mock}
			l.logAtLevel("msg", tc.level)
		})
	}
}

func TestYamuxLogger_Print(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := log.NewMockLogger(ctrl)
	mock.EXPECT().Error("[ERR] yamux: bad")

	l := yamuxLogger{maxLogLevel: NoDemote, logger: mock}
	l.Print("[ERR] yamux: bad")
}

func TestYamuxLogger_Printf(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := log.NewMockLogger(ctrl)
	mock.EXPECT().Error("[ERR] yamux: code=42")

	l := yamuxLogger{maxLogLevel: NoDemote, logger: mock}
	l.Printf("[ERR] yamux: code=%d", 42)
}

func TestYamuxLogger_Println(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := log.NewMockLogger(ctrl)

	// Println uses fmt.Sprintln which appends a newline.
	mock.EXPECT().Error("[ERR] yamux: oops\n")

	l := yamuxLogger{maxLogLevel: NoDemote, logger: mock}
	l.Println("[ERR] yamux: oops")
}

func TestYamuxLogger_LogLevelCap_String(t *testing.T) {
	t.Parallel()

	// Ensure the iota values are ordered correctly (min() comparisons depend on ordering).
	require.Less(t, int(DemoteToDebug), int(DemoteToInfo))
	require.Less(t, int(DemoteToInfo), int(DemoteToWarn))
	require.Less(t, int(DemoteToWarn), int(DemoteToError))
	require.Less(t, int(DemoteToError), int(NoDemote))
}
