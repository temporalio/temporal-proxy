package proxy

import (
	"context"
	"errors"
	"time"

	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

type (
	// Vault seals and opens payloads using envelope encryption scoped by namespace.
	// It is the subset of [crypto.Vault] the interceptor depends on.
	Vault interface {
		Seal(context.Context, string, []byte, ...crypto.SealOption) (*crypto.Message, error)
		Open(context.Context, *crypto.Message, ...crypto.OpenOption) ([]byte, error)
	}

	// cipher binds a Vault to a single request: the context its calls run under,
	// the scope that picks its DEK and binding, and the reporter its timings go
	// to. It is the [codec.Cipher] the encryption codec seals through.
	cipher struct {
		ctx context.Context
		s   scope
		v   Vault
		r   *Reporter
	}
)

// Encrypt seals data under the request's namespace, bound to the scope's label,
// recording the call through VaultOp whatever the outcome. The vault's error is
// returned as it arrived; the codec is what knows these bytes are a payload, so
// it supplies that context.
func (c *cipher) Encrypt(data []byte) (*crypto.Message, error) {
	start := time.Now()
	msg, err := c.v.Seal(c.ctx, c.s.ns, data, crypto.WithLabel(c.s.label))
	c.r.VaultOp(c.ctx, "encrypt", resultLabel(err), c.s.ns, time.Since(start).Seconds())

	return msg, err
}

// Decrypt opens m, recording the call through VaultOp whatever the outcome. It
// first counts m through UnboundOpen when it carries no binding, or through
// NamespaceMismatch when the scope checks bindings and m is bound elsewhere, and
// asks the vault to hold m to the scope's label.
func (c *cipher) Decrypt(m *crypto.Message) ([]byte, error) {
	var opts []crypto.OpenOption
	switch {
	case !m.Bound:
		c.r.UnboundOpen(c.ctx, c.s.ns)
	case c.s.check:
		if m.Namespace != c.s.label {
			c.r.NamespaceMismatch(c.ctx, c.s.ns)
		}
	}

	if c.s.check {
		opts = append(opts, crypto.ExpectNamespace(c.s.label))
	}

	start := time.Now()
	pt, err := c.v.Open(c.ctx, m, opts...)
	c.r.VaultOp(c.ctx, "decrypt", resultLabel(err), c.s.ns, time.Since(start).Seconds())

	return pt, err
}

// resultLabel maps an error to the "result" metric label value. An unknown key
// gets its own value: on a chained hop it is expected, and on a standalone proxy
// it means a KEK went missing, neither of which is a vault failure. A message
// strict refuses for lacking a namespace binding, or for being bound to another
// namespace, gets its own value too, since it is policy, not a failure.
func resultLabel(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, crypto.ErrUnknownKey):
		return "unknown_key"
	case errors.Is(err, crypto.ErrUnbound):
		return "unbound"
	case errors.Is(err, crypto.ErrNamespaceMismatch):
		return "mismatch"
	default:
		return "error"
	}
}
