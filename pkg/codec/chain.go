package codec

import (
	"slices"

	"go.temporal.io/api/common/v1"
)

type (
	// Codec transforms payloads on their way to an upstream and back. The method
	// set matches the Temporal SDK's converter.PayloadCodec, so an implementation
	// satisfies both without this package depending on the SDK.
	Codec interface {
		Encode([]*common.Payload) ([]*common.Payload, error)
		Decode([]*common.Payload) ([]*common.Payload, error)
	}

	// Chain applies a set of codecs as one. Encode runs them in the order the
	// chain holds them and Decode runs them in reverse, so the last codec to
	// encode a payload is the first to decode it.
	// Note that this is the opposite of the SDK's own convention, where a
	// multi-codec list encodes last to first; a Chain is handed to the SDK whole,
	// as a single codec, so its order stays its own concern.
	Chain struct {
		codecs []Codec
	}

	// Option enables a codec in a [Chain].
	Option func(*options)

	options struct {
		cipher     Cipher
		encryptor  []EncryptorOption
		compressor *Compressor
		skip       map[string]struct{}
	}
)

// NewChain returns the Chain implied by opts, holding every codec they enable in
// the order it has to be applied. A Chain with no codecs is a no-op.
func NewChain(opts ...Option) Chain {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	// Application order, outbound. Compression belongs ahead of encryption
	// because ciphertext does not compress.
	var codecs []Codec
	if cmp := o.compressor; cmp != nil {
		// Only a Compressor that encodes can wrap a skipped payload, and the
		// copy is all this costs per chain.
		if len(o.skip) > 0 && cmp.enc != nil {
			cmp = cmp.skipping(o.skip)
		}

		codecs = append(codecs, cmp)
	}

	if o.cipher != nil {
		opts := o.encryptor
		if len(o.skip) > 0 {
			// Ahead of the caller's options, so WithSkipEncodings still wins.
			opts = append([]EncryptorOption{withSkipSet(o.skip)}, opts...)
		}

		codecs = append(codecs, NewEncryptor(o.cipher, opts...))
	}

	return Chain{codecs: codecs}
}

// WithCipher seals and opens payloads through c. A nil c is ignored, leaving the
// chain without encryption.
func WithCipher(c Cipher) Option {
	return func(o *options) {
		if c != nil {
			o.cipher = c
		}
	}
}

// WithCompression compresses payloads with c, and opts configure the
// [Compressor] it builds. A nil c still adds a Compressor, one that only
// decodes, so payloads compressed earlier are restored with compression off.
// The Compressor is built once, here, and holds no per-request state, so one
// option can be shared by every chain.
func WithCompression(c Compression, opts ...CompressorOption) Option {
	cmp := NewCompressor(c, opts...)
	return func(o *options) { o.compressor = cmp }
}

// WithSkip lists encodings already encrypted before they reach the chain, so
// Encode forwards them unchanged: neither compressed nor sealed. It sets the
// encryptor's list as [WithSkipEncodings] does, which overrides it when also
// given. The set is built here, once, so one option can be shared by every
// chain. A later use replaces an earlier one.
func WithSkip(encodings ...string) Option {
	set := encodingSet(encodings)
	return func(o *options) { o.skip = set }
}

// WithEncryptorOptions passes opts to the [Encryptor] the chain builds. Without
// [WithCipher] there is no encryptor, so they have no effect.
func WithEncryptorOptions(opts ...EncryptorOption) Option {
	return func(o *options) {
		o.encryptor = append(o.encryptor, opts...)
	}
}

// Encode runs payloads through every codec in order.
func (c Chain) Encode(payloads []*common.Payload) ([]*common.Payload, error) {
	var err error
	for _, cd := range c.codecs {
		if payloads, err = cd.Encode(payloads); err != nil {
			return nil, err
		}
	}

	return payloads, nil
}

// Decode reverses [Chain.Encode], running payloads back through every codec in
// reverse order.
func (c Chain) Decode(payloads []*common.Payload) ([]*common.Payload, error) {
	var err error
	for _, v := range slices.Backward(c.codecs) {
		if payloads, err = v.Decode(payloads); err != nil {
			return nil, err
		}
	}

	return payloads, nil
}
