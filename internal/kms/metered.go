package kms

import (
	"context"
	"time"

	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

type (
	// meteredKEK decorates a crypto.KEK, recording each wrap (Encrypt) and unwrap
	// (Decrypt) as a KEK operation. It embeds the wrapped KEK so ID and Close pass
	// through unchanged, and returns the wrapped call's result and error verbatim so
	// metering never alters behavior.
	meteredKEK struct {
		crypto.KEK
		provider string
		recorder kekRecorder
	}

	// kekRecorder records KEK wrap/unwrap operations. *Reporter satisfies it; the
	// interface lets meteredKEK be tested without a Prometheus-backed reporter.
	kekRecorder interface {
		KEKOp(provider, operation, result string, seconds float64)
	}
)

func (m *meteredKEK) Encrypt(ctx context.Context, ns string, b []byte) ([]byte, error) {
	start := time.Now()
	ct, err := m.KEK.Encrypt(ctx, ns, b)
	m.recorder.KEKOp(m.provider, "wrap", resultLabel(err), time.Since(start).Seconds())
	return ct, err
}

func (m *meteredKEK) Decrypt(ctx context.Context, b []byte) ([]byte, error) {
	start := time.Now()
	pt, err := m.KEK.Decrypt(ctx, b)
	m.recorder.KEKOp(m.provider, "unwrap", resultLabel(err), time.Since(start).Seconds())
	return pt, err
}

// newMeteredKEK wraps k so its KMS calls are recorded under provider.
func newMeteredKEK(k crypto.KEK, provider string, r kekRecorder) crypto.KEK {
	return &meteredKEK{KEK: k, provider: provider, recorder: r}
}

// providerForScheme maps a KMS URI scheme to a stable, low-cardinality provider
// label. Unknown schemes are returned unchanged so a new backend still produces
// a usable (if unrecognized) label rather than an empty one.
func providerForScheme(scheme string) string {
	switch scheme {
	case "awskms":
		return "aws"
	case "gcpkms":
		return "gcp"
	case "azurekeyvault":
		return "azure"
	case "testing", "base64key":
		return "testing"
	default:
		return scheme
	}
}

// resultLabel maps an error to the "result" metric label value.
func resultLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}
