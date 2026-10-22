package creds_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
)

const (
	reloadedMsg = "reloaded TLS material"
	failedMsg   = "failed to reload TLS material, keeping the previous material"
)

type (
	attemptCall struct {
		credential string
		err        error
	}

	notAfterCall struct {
		credential, file string
		t                time.Time
	}

	// fakeReporter records every call it receives.
	fakeReporter struct {
		mu        sync.Mutex
		attempts  []attemptCall
		notAfters []notAfterCall
	}

	// countingLogger wraps a TestLogger and counts entries per level, since
	// TestLogger only answers whether an entry exists.
	countingLogger struct {
		logger.Logger
		counts *logCounts
	}

	logCounts struct {
		mu sync.Mutex
		n  map[logger.Level]int
	}
)

func TestReloader_ReloadsChangedFilesOnTick(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		certFile, keyFile := writeLeaf(t, t.TempDir(), now.Add(-time.Hour), now.Add(time.Hour))
		rep, log := new(fakeReporter), newCountingLogger()
		r := creds.NewReloader(creds.WithReporter(rep), creds.WithReloaderLogger(log))

		l := creds.NewListener(creds.WithCertificate(certFile, keyFile), creds.WithReloader(r, "gateway"))
		startReloader(t, r)
		require.Equal(t, []notAfterCall{{"gateway", "cert", now.Add(time.Hour).UTC()}}, rep.notAfterCalls())

		newAfter := now.Add(2 * time.Hour)
		writeLeafTo(t, certFile, keyFile, now.Add(-time.Hour), newAfter)

		time.Sleep(time.Minute + time.Second)
		synctest.Wait()

		require.Equal(t, []attemptCall{{"gateway", nil}}, rep.attemptCalls())
		require.Equal(t, notAfterCall{"gateway", "cert", newAfter.UTC()}, rep.notAfterCalls()[1])
		require.Equal(t, 1, log.count(logger.LevelInfo))
		require.Equal(t, 1, log.total())
		require.True(t, log.ContainsEntry(logger.LevelInfo, reloadedMsg,
			tag.Component("tlsreload"),
			tag.String("credential", "gateway"),
			tag.String("files", certFile+","+keyFile),
			tag.String("certNotAfter", newAfter.UTC().Format(time.RFC3339)),
		))

		got, _ := creds.ListenerSnapshot(l)
		require.True(t, newAfter.Equal(got))
	})
}

func TestReloader_UnchangedTicksAreSilent(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		certFile, keyFile := writeLeaf(t, t.TempDir(), now.Add(-time.Hour), now.Add(time.Hour))
		rep, log := new(fakeReporter), newCountingLogger()
		r := creds.NewReloader(creds.WithReporter(rep), creds.WithReloaderLogger(log))

		creds.NewListener(creds.WithCertificate(certFile, keyFile), creds.WithReloader(r, "gateway"))
		startReloader(t, r)

		time.Sleep(5 * time.Minute)
		synctest.Wait()

		require.Empty(t, rep.attemptCalls())
		require.Len(t, rep.notAfterCalls(), 1)
		require.Zero(t, log.total())
	})
}

func TestReloader_BrokenFileLogsOnceAndCountsEveryTick(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		certFile, keyFile := writeLeaf(t, t.TempDir(), now.Add(-time.Hour), now.Add(time.Hour))
		rep, log := new(fakeReporter), newCountingLogger()
		r := creds.NewReloader(creds.WithReporter(rep), creds.WithReloaderLogger(log))

		creds.NewListener(creds.WithCertificate(certFile, keyFile), creds.WithReloader(r, "gateway"))
		startReloader(t, r)

		require.NoError(t, os.WriteFile(keyFile, []byte("garbage"), 0o600))
		time.Sleep(5*time.Minute + time.Second)
		synctest.Wait()

		require.Len(t, rep.attemptCalls(), 5)
		require.Equal(t, 5, rep.failures())
		require.Equal(t, 1, log.count(logger.LevelError))

		tags := log.TagsOf(logger.LevelError, failedMsg)
		require.Equal(t, "gateway", tags["credential"])
		require.Equal(t, certFile+","+keyFile, tags["files"])
		require.NotEmpty(t, tags["error"])

		require.NoError(t, os.Remove(keyFile))
		time.Sleep(time.Minute)
		synctest.Wait()

		require.Equal(t, 6, rep.failures())
		require.Equal(t, 2, log.count(logger.LevelError))

		writeLeafTo(t, certFile, keyFile, now.Add(-time.Hour), now.Add(2*time.Hour))
		time.Sleep(time.Minute)
		synctest.Wait()

		attempts := rep.attemptCalls()
		require.Len(t, attempts, 7)
		require.Equal(t, attemptCall{"gateway", nil}, attempts[6])
		require.Equal(t, 1, log.count(logger.LevelInfo))
	})
}

func TestReloader_DeduplicatesByFileSet(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		certFile, keyFile := writeLeaf(t, t.TempDir(), now.Add(-time.Hour), now.Add(time.Hour))
		rep := new(fakeReporter)
		r := creds.NewReloader(creds.WithReporter(rep), creds.WithReloaderLogger(logger.NewNoopLogger()))

		creds.NewListener(creds.WithCertificate(certFile, keyFile), creds.WithReloader(r, "http"))
		creds.NewListener(creds.WithCertificate(certFile, keyFile), creds.WithReloader(r, "gateway"))
		creds.NewDialer(creds.WithCA(certFile), creds.WithReloader(r, "upstream/pinned"))
		startReloader(t, r)

		writeLeafTo(t, certFile, keyFile, now.Add(-time.Hour), now.Add(2*time.Hour))
		time.Sleep(time.Minute + time.Second)
		synctest.Wait()

		require.ElementsMatch(t, []attemptCall{{"gateway,http", nil}, {"upstream/pinned", nil}}, rep.attemptCalls())
	})
}

func TestReloader_StartFailsOnBadMaterial(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		certFile, _ := writeLeaf(t, t.TempDir(), now.Add(-time.Hour), now.Add(time.Hour))
		_, otherKey := writeLeaf(t, t.TempDir(), now.Add(-time.Hour), now.Add(time.Hour))
		caFile := writeCA(t, t.TempDir(), now.Add(time.Hour))
		r := creds.NewReloader(creds.WithReloaderLogger(logger.NewNoopLogger()))

		creds.NewDialer(
			creds.WithCertificate(certFile, otherKey),
			creds.WithCA(caFile),
			creds.WithReloader(r, "upstream/templated"),
		)

		require.ErrorContains(t, r.Start(t.Context()), "upstream/templated")
	})
}

func TestReloader_StopEndsLoop(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		certFile, keyFile := writeLeaf(t, t.TempDir(), now.Add(-time.Hour), now.Add(time.Hour))
		rep := new(fakeReporter)
		r := creds.NewReloader(creds.WithReporter(rep), creds.WithReloaderLogger(logger.NewNoopLogger()))

		creds.NewListener(creds.WithCertificate(certFile, keyFile), creds.WithReloader(r, "gateway"))
		require.NoError(t, r.Start(t.Context()))
		require.Error(t, r.Start(t.Context()))
		require.NoError(t, r.Stop(t.Context()))

		writeLeafTo(t, certFile, keyFile, now.Add(-time.Hour), now.Add(2*time.Hour))
		time.Sleep(3*time.Minute + time.Second)
		synctest.Wait()

		require.Empty(t, rep.attemptCalls())
		require.NoError(t, r.Stop(t.Context()))
	})
}

func TestReloader_StopBeforeStartIsNoop(t *testing.T) {
	t.Parallel()

	require.NoError(t, creds.NewReloader().Stop(t.Context()))
}

func TestReloader_IgnoresModesWithoutFiles(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		rep := new(fakeReporter)
		r := creds.NewReloader(creds.WithReporter(rep), creds.WithReloaderLogger(logger.NewNoopLogger()))

		creds.NewListener(creds.Insecure(), creds.WithReloader(r, "gateway"))
		creds.NewDialer(creds.WithReloader(r, "upstream/cloud"))
		require.Zero(t, creds.ReloaderEntryCount(r))
		startReloader(t, r)

		time.Sleep(time.Minute + time.Second)
		synctest.Wait()

		require.Empty(t, rep.attemptCalls())
		require.Empty(t, rep.notAfterCalls())
	})
}

func TestWithReloader_NilIsNoop(t *testing.T) {
	t.Parallel()

	certFile, keyFile := writeLeaf(t, t.TempDir(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	l := creds.NewListener(creds.WithCertificate(certFile, keyFile), creds.WithReloader(nil, "x"))

	cfg, err := l.TLSConfig()
	require.NoError(t, err)
	require.NotNil(t, cfg)
}

func (f *fakeReporter) Attempt(credential string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.attempts = append(f.attempts, attemptCall{credential, err})
}

func (f *fakeReporter) NotAfter(credential, file string, t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.notAfters = append(f.notAfters, notAfterCall{credential, file, t.UTC()})
}

func (f *fakeReporter) attemptCalls() []attemptCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]attemptCall(nil), f.attempts...)
}

func (f *fakeReporter) notAfterCalls() []notAfterCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]notAfterCall(nil), f.notAfters...)
}

func (f *fakeReporter) failures() int {
	n := 0

	for _, a := range f.attemptCalls() {
		if a.err != nil {
			n++
		}
	}

	return n
}

func (c *countingLogger) ContainsEntry(l logger.Level, msg string, tags ...tag.Tag) bool {
	return c.Logger.(*logger.TestLogger).ContainsEntry(l, msg, tags...)
}

func (c *countingLogger) Debug(msg string, tags ...tag.Tag) {
	c.counts.add(logger.LevelDebug)
	c.Logger.Debug(msg, tags...)
}

func (c *countingLogger) Error(msg string, tags ...tag.Tag) {
	c.counts.add(logger.LevelError)
	c.Logger.Error(msg, tags...)
}

func (c *countingLogger) Info(msg string, tags ...tag.Tag) {
	c.counts.add(logger.LevelInfo)
	c.Logger.Info(msg, tags...)
}

func (c *countingLogger) TagsOf(l logger.Level, msg string) map[string]any {
	return c.Logger.(*logger.TestLogger).TagsOf(l, msg)
}

func (c *countingLogger) Warn(msg string, tags ...tag.Tag) {
	c.counts.add(logger.LevelWarn)
	c.Logger.Warn(msg, tags...)
}

func (c *countingLogger) With(tags ...tag.Tag) logger.Logger {
	return &countingLogger{Logger: c.Logger.With(tags...), counts: c.counts}
}

func (c *countingLogger) count(l logger.Level) int {
	c.counts.mu.Lock()
	defer c.counts.mu.Unlock()

	return c.counts.n[l]
}

func (c *countingLogger) total() int {
	c.counts.mu.Lock()
	defer c.counts.mu.Unlock()

	n := 0
	for _, v := range c.counts.n {
		n += v
	}

	return n
}

func (lc *logCounts) add(l logger.Level) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.n[l]++
}

func newCountingLogger() *countingLogger {
	return &countingLogger{Logger: logger.NewTestLogger(), counts: &logCounts{n: map[logger.Level]int{}}}
}

// startReloader starts r and stops it before the synctest bubble ends, since
// the bubble waits for the polling goroutine to exit.
func startReloader(t *testing.T, r *creds.Reloader) {
	t.Helper()

	require.NoError(t, r.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, r.Stop(context.WithoutCancel(t.Context()))) })
}
