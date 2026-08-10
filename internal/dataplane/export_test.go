package dataplane

import (
	"net"
	"slices"
)

// Listeners is the set of listeners Start bound, exported for tests that break
// serving out from under a running plane. Nothing a caller can reach does that:
// Stop and cancelling the serving context are both clean shutdowns, and a taken
// port fails before serving starts, so closing these is the only way into the
// unexpected-exit path.
func (d *Dataplane) Listeners() []net.Listener {
	d.mu.Lock()
	defer d.mu.Unlock()

	return slices.Clone(d.listeners)
}
