package codec

import (
	"fmt"
	"slices"

	"go.temporal.io/api/common/v1"
)

// These keys form the on-the-wire contract for a compressed payload: the
// encoding marker lets Decode recognize its own output, and the algorithm entry
// names what it takes to restore it.
const (
	// MetadataCompressionAlgorithm is the payload metadata key holding the name
	// of the algorithm that compressed the payload.
	MetadataCompressionAlgorithm = "compression-algorithm"

	// CompressionEncoding is the encoding a compressed payload is marked with.
	CompressionEncoding = "binary/compressed"
)

type (
	// Compressor compresses payloads and restores them again.
	Compressor struct {
		enc      Compression
		dec      map[string]Compression
		skip     map[string]struct{}
		minBytes int
	}

	// CompressorOption configures a [Compressor].
	CompressorOption func(*Compressor)

	// Compression compresses and decompresses bytes with one algorithm, which
	// Encoding names in the metadata of the payloads it compresses.
	Compression interface {
		Encoding() string
		Compress([]byte) ([]byte, error)
		Decompress([]byte) ([]byte, error)
	}
)

// NewCompressor returns a [Compressor] that compresses payloads with enc and
// can restore what enc compressed. A nil enc gives a Compressor that only
// decodes, for when compression is turned off.
func NewCompressor(enc Compression, opts ...CompressorOption) *Compressor {
	c := &Compressor{enc: enc}
	for _, opt := range opts {
		opt(c)
	}

	return c
}

// WithDecoders adds compressions [Compressor.Decode] can restore payloads with,
// beyond the one the Compressor compresses with. The set is built here, once,
// so one option can be shared by every Compressor built from it. A later use
// replaces an earlier one.
func WithDecoders(cs ...Compression) CompressorOption {
	dec := make(map[string]Compression, len(cs))
	for _, cmp := range cs {
		dec[cmp.Encoding()] = cmp
	}

	return func(c *Compressor) { c.dec = dec }
}

// WithMinBytes leaves payloads whose data is smaller than n bytes uncompressed,
// since framing overhead would outweigh any savings.
func WithMinBytes(n int) CompressorOption {
	return func(c *Compressor) { c.minBytes = n }
}

// Encode compresses each payload whole, metadata included, so
// [Compressor.Decode] restores it exactly. Payloads under [WithMinBytes], already
// compressed, under an encoding listed through [WithSkip], or that wouldn't come
// out smaller are returned as they are, as is everything when the Compressor
// only decodes.
func (c *Compressor) Encode(payloads []*common.Payload) ([]*common.Payload, error) {
	if c.enc == nil {
		return payloads, nil
	}

	// As in Decode, the input is only copied once a payload changes.
	res, copied := payloads, false
	for i, p := range payloads {
		// A payload compressed by an earlier proxy hop won't shrink again, and a
		// skipped one has to reach the encryptor under its own encoding.
		enc := string(p.GetMetadata()[MetadataEncoding])
		if len(p.GetData()) < c.minBytes || enc == CompressionEncoding {
			continue
		}

		if _, ok := c.skip[enc]; ok {
			continue
		}

		data, err := p.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %w", err)
		}

		compressed, err := c.enc.Compress(data)
		if err != nil {
			return nil, fmt.Errorf("failed to compress payload: %w", err)
		}

		cp := &common.Payload{
			Metadata: map[string][]byte{
				MetadataEncoding:             []byte(CompressionEncoding),
				MetadataCompressionAlgorithm: []byte(c.enc.Encoding()),
			},
			Data: compressed,
		}

		// Data that doesn't compress comes out bigger once framing and our
		// metadata are added, so forward the original instead.
		if cp.Size() >= len(data) {
			continue
		}

		if !copied {
			res, copied = slices.Clone(payloads), true
		}

		res[i] = cp
	}

	return res, nil
}

// Decode reverses [Compressor.Encode], restoring each payload with the algorithm
// its metadata names. Payloads without the full compressed-payload contract pass
// through unchanged; a named algorithm with no registered decoder is an error.
func (c *Compressor) Decode(payloads []*common.Payload) ([]*common.Payload, error) {
	// Most payloads aren't compressed, so the input is only copied once one is,
	// and the caller's slice is never modified.
	res, copied := payloads, false
	for i, p := range payloads {
		// The marker alone is not proof we compressed this; without an algorithm
		// entry there is nothing to restore it with.
		md := p.GetMetadata()
		if string(md[MetadataEncoding]) != CompressionEncoding || len(md[MetadataCompressionAlgorithm]) == 0 {
			continue
		}

		name := string(md[MetadataCompressionAlgorithm])
		cmp := c.decoder(name)
		if cmp == nil {
			return nil, fmt.Errorf("failed to decompress payload: unknown algorithm %q", name)
		}

		data, err := cmp.Decompress(p.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress payload: %w", err)
		}

		og := new(common.Payload)
		if err := og.Unmarshal(data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
		}

		if !copied {
			res, copied = slices.Clone(payloads), true
		}

		res[i] = og
	}

	return res, nil
}

// skipping returns a copy of c that forwards payloads under an encoding in set.
// The receiver is shared by every chain built from one option, so it is never
// modified.
func (c *Compressor) skipping(set map[string]struct{}) *Compressor {
	cp := *c
	cp.skip = set

	return &cp
}

// decoder returns the compression that restores payloads compressed with name,
// or nil when there is none. The shared decoder set never holds enc, so it is
// checked first.
func (c *Compressor) decoder(name string) Compression {
	if c.enc != nil && c.enc.Encoding() == name {
		return c.enc
	}

	return c.dec[name]
}
