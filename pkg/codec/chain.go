package codec

import (
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
	// chain holds them and Decode runs them in reverse, so a payload is
	// compressed before it is sealed and unsealed before it is decompressed.
	// Note that this is the opposite of the SDK's own convention, where a
	// multi-codec list encodes last to first; a Chain is handed to the SDK whole,
	// as a single codec, so its order stays its own concern.
	Chain struct {
		codecs []Codec
	}

	// Option enables a codec in a [Chain].
	Option func(*options)

	options struct {
		cipher Cipher
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
	if o.cipher != nil {
		codecs = append(codecs, NewEncryptor(o.cipher))
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
	for i := len(c.codecs) - 1; i >= 0; i-- {
		if payloads, err = c.codecs[i].Decode(payloads); err != nil {
			return nil, err
		}
	}

	return payloads, nil
}
