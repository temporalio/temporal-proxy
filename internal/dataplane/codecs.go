package dataplane

import (
	"errors"
	"fmt"

	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/pkg/crypto"
	"go.temporal.io/proxy/pkg/logger"
)

// codecParams collects what the payload codec chain is built from. Vault is nil
// when no encryption keys are configured, and Encryption is nil with it.
type codecParams struct {
	fx.In

	Config      *config.Config
	Vault       *crypto.Vault
	Limiter     *proxy.DecodeLimiter
	Compression *proxy.CompressionReporter
	Encryption  *proxy.Reporter
	Logger      logger.Logger
}

// newDecodeLimiter is the one pool of decoded bytes for every response,
// whichever upstream or the codec server it came through, so the bound holds
// for the proxy as a whole.
func newDecodeLimiter(cfg *config.Config) *proxy.DecodeLimiter {
	return proxy.NewDecodeLimiter(int64(cfg.Compression.InFlightLimit()))
}

// newCodecs builds the payload codec chain every upstream applies. The vault and
// the encryption switch are global, so nothing here varies per upstream, and
// providing it once is what lets the codec server apply the identical chain.
// Its response limit is the largest any upstream accepts, since the codec
// server serves them all; each forwarder bounds its own responses by its
// upstream's.
func newCodecs(p codecParams) (*proxy.Codecs, error) {
	enc := &p.Config.Encryption

	// Encryption is a security control: if it is enabled, or keys are configured
	// even while disabled for new traffic, but no vault was provided, fail fast.
	// Keys without a vault means payloads sealed earlier cannot be opened, which
	// is as dangerous as forwarding cleartext outbound. The check is on the
	// concrete pointer, before any conversion to an interface, since a nil
	// pointer in a non-nil interface would slip past it.
	if (enc.Enabled || enc.Default != nil) && p.Vault == nil {
		return nil, errors.New("encryption is enabled or keys are configured, but no vault was provided")
	}

	// With no keys there is no encryption codec, so the list has nothing to skip.
	if len(enc.SkipEncodings) > 0 && p.Vault == nil {
		p.Logger.Warn("encryption.skipEncodings is set but no encryption keys are configured, so it has no effect")
	}

	opts := proxy.CodecOptions{
		Encrypt:             enc.Enabled,
		EncodeFailures:      enc.Failures,
		SkipEncodings:       enc.SkipEncodings,
		Compression:         &p.Config.Compression,
		CompressionReporter: p.Compression,
		DecodeLimiter:       p.Limiter,
		ResponseLimit:       int64(p.Config.Upstreams.MaxResponseLimit()),
	}

	// Only assign the vault once it is known to be there. p.Vault is a concrete
	// pointer and the field is an interface, so assigning unconditionally would
	// hand over a non-nil interface holding a nil pointer, which reads as a vault
	// being present and panics the first time a payload is opened.
	if p.Vault != nil {
		opts.Vault = p.Vault
		opts.Reporter = p.Encryption
	}

	codecs, err := proxy.NewCodecs(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build payload codecs: %w", err)
	}

	return codecs, nil
}
