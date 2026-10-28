package creds

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
)

const defaultReloadInterval = time.Minute

type (
	// Reporter receives reload outcomes. credential is the label of the reloaded
	// file set: the sorted, comma-joined ids of every credential sharing it.
	Reporter interface {
		// Attempt records one reload attempt; a nil err means it succeeded.
		Attempt(credential string, err error)
		// NotAfter records the expiry of the material in use. file is "cert" or
		// "ca".
		NotAfter(credential, file string, t time.Time)
	}

	// ReloaderOption configures a [Reloader].
	ReloaderOption func(*Reloader)

	// Reloader polls the certificate, key, and CA files of every [Dialer] and
	// [Listener] registered with [WithReloader], and swaps in new material when
	// the files change. Credentials that reference the same files in the same
	// mode share one material, so each file set is read once per tick.
	Reloader struct {
		logger   logger.Logger
		reporter Reporter
		interval time.Duration

		mu      sync.Mutex // guards entries, started, stop, and done
		entries map[materialKey]*entry
		started bool
		stop    chan struct{}
		done    chan struct{}
	}

	// materialKey identifies a file set. The mode is part of the key because a
	// CA file validates differently as a mutual-TLS CA than as a pinned anchor.
	materialKey struct {
		mode          Mode
		ca, cert, key string
	}

	// entry is one shared material and the credentials using it. lastFailure is
	// the text of the last failure logged, touched only by the polling loop.
	entry struct {
		m           *material
		credentials []string
		lastFailure string
	}

	// labelled pairs an entry with its label, computed under the reloader lock.
	labelled struct {
		e     *entry
		label string
	}

	noopReporter struct{}
)

// NewReloader returns a Reloader that logs to [logger.Default], reports to
// nothing, and polls every minute unless opts say otherwise.
func NewReloader(opts ...ReloaderOption) *Reloader {
	r := &Reloader{
		logger:   logger.Default(),
		reporter: noopReporter{},
		interval: defaultReloadInterval,
		entries:  make(map[materialKey]*entry),
	}

	for _, opt := range opts {
		opt(r)
	}

	r.logger = r.logger.With(tag.Component("tlsreload"))

	return r
}

// WithReloaderLogger sets the logger reload outcomes are written to.
func WithReloaderLogger(l logger.Logger) ReloaderOption {
	return func(r *Reloader) { r.logger = l }
}

// WithReporter sets the [Reporter] that receives reload outcomes.
func WithReporter(rep Reporter) ReloaderOption {
	return func(r *Reloader) { r.reporter = rep }
}

// WithReloadInterval sets how often the files are polled.
func WithReloadInterval(d time.Duration) ReloaderOption {
	return func(r *Reloader) { r.interval = d }
}

// WithReloader registers the credential's files with r under the id
// credential. Modes without files (insecure, system-root TLS) register
// nothing, and a nil r is a no-op. The credential must be constructed before
// the Reloader starts.
func WithReloader(r *Reloader, credential string) Option {
	return optFunc(func(o *options) {
		if r == nil {
			return
		}

		o.reloader, o.credential = r, credential
	})
}

// Start loads every registered material, failing on the first error, reports
// each expiry, and starts the polling loop. A second call returns an error.
func (r *Reloader) Start(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return errors.New("creds: reloader already started")
	}

	for _, le := range r.labelledLocked() {
		if err := le.e.m.load(); err != nil {
			return fmt.Errorf("creds: failed to load TLS material for %s: %w", le.label, err)
		}

		r.reportNotAfter(le.label, le.e.m.current())
	}

	r.started = true
	r.stop, r.done = make(chan struct{}), make(chan struct{})

	go r.run(r.stop, r.done)

	return nil
}

// Stop ends the polling loop and waits for it to exit or for ctx to be done.
// Stopping a reloader that is not running is a no-op.
func (r *Reloader) Stop(ctx context.Context) error {
	r.mu.Lock()
	stop, done := r.stop, r.done
	r.stop = nil
	r.mu.Unlock()

	if stop == nil {
		return nil
	}

	close(stop)

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// material returns the shared material for o's file set in mode, creating it
// on first use, and records credential as one of its users.
func (r *Reloader) material(credential string, o *options, mode Mode) *material {
	key := materialKey{mode: mode, ca: o.ca, cert: o.cert, key: o.key}

	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.entries[key]
	if !ok {
		e = &entry{m: &material{opts: o, mode: mode}}
		r.entries[key] = e
	}

	if !slices.Contains(e.credentials, credential) {
		e.credentials = append(e.credentials, credential)
	}

	return e.m
}

// labelledLocked returns every entry with its label, sorted by label. Callers
// hold r.mu.
func (r *Reloader) labelledLocked() []labelled {
	out := make([]labelled, 0, len(r.entries))
	for _, e := range r.entries {
		ids := slices.Clone(e.credentials)
		slices.Sort(ids)
		out = append(out, labelled{e: e, label: strings.Join(ids, ",")})
	}

	slices.SortFunc(out, func(a, b labelled) int { return strings.Compare(a.label, b.label) })

	return out
}

// reportNotAfter reports the non-zero expiries of s.
func (r *Reloader) reportNotAfter(label string, s *snapshot) {
	if !s.certNotAfter.IsZero() {
		r.reporter.NotAfter(label, "cert", s.certNotAfter)
	}

	if !s.caNotAfter.IsZero() {
		r.reporter.NotAfter(label, "ca", s.caNotAfter)
	}
}

// run polls on every tick until stop is closed, then closes done.
func (r *Reloader) run(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			r.tick()
		}
	}
}

// tick runs one reload per entry. Unchanged sets are silent; every attempt is
// reported, and failures are logged only when their text changes.
func (r *Reloader) tick() {
	r.mu.Lock()
	entries := r.labelledLocked()
	r.mu.Unlock()

	for _, le := range entries {
		res := le.e.m.reload()
		if !res.attempted {
			continue
		}

		r.reporter.Attempt(le.label, res.err)

		files := tag.String("files", configuredFiles(le.e.m.opts, le.e.m.mode))

		if res.err != nil {
			if msg := res.err.Error(); msg != le.e.lastFailure {
				le.e.lastFailure = msg
				r.logger.Error("failed to reload TLS material, keeping the previous material",
					tag.String("credential", le.label), files, tag.Error(res.err))
			}

			continue
		}

		le.e.lastFailure = ""

		tags := []tag.Tag{tag.String("credential", le.label), files}
		if !res.snap.certNotAfter.IsZero() {
			tags = append(tags, tag.String("certNotAfter", res.snap.certNotAfter.UTC().Format(time.RFC3339)))
		}

		if !res.snap.caNotAfter.IsZero() {
			tags = append(tags, tag.String("caNotAfter", res.snap.caNotAfter.UTC().Format(time.RFC3339)))
		}

		r.logger.Info("reloaded TLS material", tags...)
		r.reportNotAfter(le.label, res.snap)
	}
}

func (noopReporter) Attempt(string, error)              {}
func (noopReporter) NotAfter(string, string, time.Time) {}

// newMaterial returns the material for a credential: shared through its
// reloader for modes that read files, private otherwise.
func newMaterial(o *options, mode Mode) *material {
	switch mode {
	case ModeCustomCA, ModeMutualTLS, ModeServerTLS:
		if o.reloader != nil {
			return o.reloader.material(o.credential, o, mode)
		}
	}

	return &material{opts: o, mode: mode}
}

// configuredFiles joins the paths mode reads, in cert, key, ca order.
func configuredFiles(o *options, mode Mode) string {
	switch mode {
	case ModeCustomCA:
		return o.ca
	case ModeServerTLS:
		return o.cert + "," + o.key
	case ModeMutualTLS:
		return o.cert + "," + o.key + "," + o.ca
	default:
		return ""
	}
}
