package crypto

type (
	// CacheEvent describes a DEK cache access. Fields may be added over time
	// without breaking implementers, so callers accept the struct by value.
	CacheEvent struct {
		// Size is the number of entries in the DEK cache after the access.
		Size int
	}

	// Observer receives notifications about Vault-internal events for telemetry.
	// Implementations must be safe for concurrent use and must not block: a
	// Vault calls these on the Open path. A nil Observer is never used; the
	// Vault substitutes a no-op (see WithObserver).
	Observer interface {
		CacheHit(CacheEvent)
		CacheMiss(CacheEvent)
	}

	// nopObserver is the default Observer; it drops every event.
	nopObserver struct{}
)

func (nopObserver) CacheHit(CacheEvent)  {}
func (nopObserver) CacheMiss(CacheEvent) {}
