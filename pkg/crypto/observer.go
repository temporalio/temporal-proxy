package crypto

import "time"

const (
	// OpEncrypt is a Seal: the DEK encrypting a payload.
	OpEncrypt Operation = iota
	// OpDecrypt is an Open: the DEK decrypting a payload.
	OpDecrypt
)

const (
	// RotationScheduled is a rotation performed by [Vault.Refresh], off the
	// request path.
	RotationScheduled RotationReason = iota
	// RotationOnDemand is a rotation [Vault.Seal] performed because it found the
	// DEK already expired, which means Refresh has fallen behind.
	RotationOnDemand
	// RotationInitial is the first DEK for a namespace that had no explicit
	// [WithKeyConfig], created on its first Seal.
	RotationInitial
)

type (
	// Operation names the envelope operation an [EnvelopeEvent] describes.
	Operation uint8

	// RotationReason says why a namespace's DEK was replaced.
	RotationReason uint8

	// Event is one thing a Vault reports to an [Observer]. The set is closed:
	// only this package can define one, so an Observer's type switch can be
	// written against a known set of cases.
	Event interface {
		internalMarker()
	}

	// CacheEvent describes a DEK cache access.
	CacheEvent struct {
		// Hit is true when the wrapped DEK was already cached.
		Hit bool
		// Size is the number of entries in the DEK cache after the access.
		Size int
	}

	// EnvelopeEvent describes one completed envelope operation, successful or
	// not. Exactly one is reported per [Vault.Seal] and per [Vault.Open], on
	// every path including early failures.
	EnvelopeEvent struct {
		// Op is OpEncrypt for Seal, OpDecrypt for Open.
		Op Operation
		// Namespace is set on OpEncrypt. It is always empty on OpDecrypt: Open
		// selects its KEK by ID from the message material and never learns a
		// namespace.
		Namespace string
		// Err is the error the operation returned, or nil.
		Err error
		// CryptoAttempted reports whether the AES-256-GCM step ran. It is the
		// only reliable signal for that, and Crypto is not: a small payload can
		// complete inside the clock's resolution, so a step that did run can
		// still report a zero duration. Crypto and CryptoErr are meaningful only
		// when this is true.
		CryptoAttempted bool
		// CryptoErr is the error from the AES-256-GCM step alone, or nil when
		// that step succeeded. Err, by contrast, is the whole operation's error,
		// which may come from a KEK wrap or a cache-miss unwrap rather than from
		// AES, so the two differ whenever a Seal encrypts successfully and then
		// fails to wrap its DEK.
		CryptoErr error
		// Total covers the whole operation, including any KEK wrap on the first
		// Seal after a rotation and any unwrap on a cache miss.
		Total time.Duration
		// Crypto covers only the AES-256-GCM step, and may be zero for a step
		// that ran. Total minus Crypto is the cost of everything around AES, and
		// is meaningful only when CryptoAttempted is true.
		Crypto time.Duration
	}

	// RotationEvent reports that a namespace's DEK was replaced.
	RotationEvent struct {
		Namespace string
		Reason    RotationReason
	}

	// Observer receives notifications about Vault-internal events for
	// telemetry. It is called with a CacheEvent from Open only, an
	// EnvelopeEvent from both Seal and Open, and a RotationEvent from both
	// Seal and Refresh. Implementations must be safe for concurrent use, must
	// not block, and must not re-enter the operation that produced the event.
	// A nil Observer is never used; the Vault substitutes a no-op (see
	// WithObserver).
	Observer interface {
		Observe(Event)
	}

	// nopObserver implements Observer by dropping every event. It is the
	// Vault's default when no Observer is supplied, and what a nil Observer
	// passed to WithObserver is replaced with.
	nopObserver struct{}
)

// String returns the metric label value for o. An unrecognized value returns
// "unknown" so a label is never blank if a value is added without updating this
// method.
func (o Operation) String() string {
	switch o {
	case OpEncrypt:
		return "encrypt"
	case OpDecrypt:
		return "decrypt"
	default:
		return "unknown"
	}
}

// String returns the metric label value for r. An unrecognized value returns
// "unknown" so a label is never blank if a value is added without updating this
// method.
func (r RotationReason) String() string {
	switch r {
	case RotationScheduled:
		return "scheduled"
	case RotationOnDemand:
		return "on_demand"
	case RotationInitial:
		return "initial"
	default:
		return "unknown"
	}
}

func (CacheEvent) internalMarker() {}

func (EnvelopeEvent) internalMarker() {}

func (RotationEvent) internalMarker() {}

func (nopObserver) Observe(Event) {}
