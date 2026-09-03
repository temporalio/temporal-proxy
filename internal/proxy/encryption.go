package proxy

import (
	"context"
	"time"

	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

type (
	// Vault seals and opens payloads using envelope encryption scoped by namespace.
	// It is the subset of [crypto.Vault] the interceptor depends on.
	Vault interface {
		Seal(context.Context, string, []byte) (*crypto.Message, error)
		Open(context.Context, *crypto.Message) ([]byte, error)
	}

	// cipher binds a Vault to a single request: the context its calls run under,
	// the namespace whose DEK seals its payloads, and the reporter its timings go
	// to. It is the [codec.Cipher] the encryption codec seals through.
	cipher struct {
		ctx context.Context
		ns  string
		v   Vault
		r   *Reporter
	}
)

// Encrypt seals data under the request's namespace, recording the call through
// VaultOp whatever the outcome. The vault's error is returned as it arrived; the
// codec is what knows these bytes are a payload, so it supplies that context.
func (c *cipher) Encrypt(data []byte) (*crypto.Message, error) {
	start := time.Now()
	msg, err := c.v.Seal(c.ctx, c.ns, data)
	c.r.VaultOp(c.ctx, "encrypt", resultLabel(err), c.ns, time.Since(start).Seconds())

	return msg, err
}

// Decrypt opens m, recording the call through VaultOp whatever the outcome.
func (c *cipher) Decrypt(m *crypto.Message) ([]byte, error) {
	start := time.Now()
	pt, err := c.v.Open(c.ctx, m)
	c.r.VaultOp(c.ctx, "decrypt", resultLabel(err), c.ns, time.Since(start).Seconds())

	return pt, err
}

// resultLabel maps an error to the "result" metric label value.
func resultLabel(err error) string {
	if err != nil {
		return "error"
	}

	return "success"
}
