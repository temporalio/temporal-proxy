package logger

import (
	"sync"

	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

type (
	// TestLogger is a [Logger] that records every entry in memory so tests can
	// assert on what was logged. It is safe for concurrent use.
	//
	// Loggers derived via [TestLogger.With] share the recording store of the
	// logger they were derived from, so entries logged through a derived logger
	// are visible from the original.
	//
	// A TestLogger must be created with [NewTestLogger]; the zero value is not
	// usable.
	TestLogger struct {
		store *testLogStore
		tags  []tag.Tag
	}

	// testLogStore is the shared recording store backing a TestLogger and any
	// loggers derived from it via With.
	testLogStore struct {
		mu      sync.Mutex
		entries []testLogEntry
	}

	testLogEntry struct {
		level Level
		msg   string
		tags  []tag.Tag
	}
)

// NewTestLogger returns an empty [TestLogger].
func NewTestLogger() *TestLogger {
	return &TestLogger{store: new(testLogStore)}
}

// Contains reports whether an entry with the given message was logged,
// regardless of its level or tags.
func (t *TestLogger) Contains(msg string) bool {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()

	for _, entry := range t.store.entries {
		if entry.msg == msg {
			return true
		}
	}

	return false
}

// ContainsEntry reports whether an entry was logged with exactly the given
// level, message, and tags. Tags must match in order, and their values are
// compared by equality, so tag values must be comparable.
func (t *TestLogger) ContainsEntry(l Level, msg string, tags ...tag.Tag) bool {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()

	pred := testLogEntry{
		level: l,
		msg:   msg,
		tags:  tags,
	}

	for _, entry := range t.store.entries {
		if entry.equal(&pred) {
			return true
		}
	}

	return false
}

func (t *TestLogger) Debug(msg string, tags ...tag.Tag) { t.record(LevelDebug, msg, tags) }
func (t *TestLogger) Error(msg string, tags ...tag.Tag) { t.record(LevelError, msg, tags) }
func (t *TestLogger) Fatal(msg string, tags ...tag.Tag) { t.record(LevelError, msg, tags) }
func (t *TestLogger) Info(msg string, tags ...tag.Tag)  { t.record(LevelInfo, msg, tags) }
func (t *TestLogger) Warn(msg string, tags ...tag.Tag)  { t.record(LevelWarn, msg, tags) }

// With returns a derived [Logger] that shares the receiver's recording store
// and prepends the receiver's tags, followed by tags, to every entry it logs.
func (t *TestLogger) With(tags ...tag.Tag) Logger {
	return &TestLogger{
		store: t.store,
		tags:  mergeTags(t.tags, tags),
	}
}

func (t *TestLogger) record(l Level, msg string, tags []tag.Tag) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()

	t.store.entries = append(t.store.entries, testLogEntry{
		level: l,
		msg:   msg,
		tags:  mergeTags(t.tags, tags),
	})
}

func (e *testLogEntry) equal(other *testLogEntry) bool {
	if e.level != other.level || e.msg != other.msg || len(e.tags) != len(other.tags) {
		return false
	}

	for i := range e.tags {
		if e.tags[i] != other.tags[i] {
			return false
		}
	}

	return true
}

// mergeTags returns a new slice containing base followed by extra, sharing neither
// input's backing array.
func mergeTags(base, extra []tag.Tag) []tag.Tag {
	out := make([]tag.Tag, 0, len(base)+len(extra))
	out = append(out, base...)
	out = append(out, extra...)
	return out
}
