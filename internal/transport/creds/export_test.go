package creds

import "time"

// DialerReload runs one reload attempt against d's material.
func DialerReload(d *Dialer) (attempted bool, err error) {
	r := d.material.reload()

	return r.attempted, r.err
}

// DialerSnapshot reports the expiry times of the snapshot d currently uses.
func DialerSnapshot(d *Dialer) (certNotAfter, caNotAfter time.Time) {
	s := d.material.current()
	if s == nil {
		return time.Time{}, time.Time{}
	}

	return s.certNotAfter, s.caNotAfter
}

// ListenerReload runs one reload attempt against l's material.
func ListenerReload(l *Listener) (attempted bool, err error) {
	r := l.material.reload()

	return r.attempted, r.err
}

// ListenerSnapshot reports the expiry times of the snapshot l currently uses.
func ListenerSnapshot(l *Listener) (certNotAfter, caNotAfter time.Time) {
	s := l.material.current()
	if s == nil {
		return time.Time{}, time.Time{}
	}

	return s.certNotAfter, s.caNotAfter
}

// ReloaderEntryCount reports how many file sets are registered with r.
func ReloaderEntryCount(r *Reloader) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.entries)
}
