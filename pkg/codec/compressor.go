package codec

import (
	"errors"
	"fmt"
	"slices"
	"time"

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

// The ops and skip reasons a [CompressionObserver] is told.
const (
	// CompressionOpCompress is the op for a payload Encode compressed.
	CompressionOpCompress = "compress"

	// CompressionOpDecompress is the op for a payload Decode restored.
	CompressionOpDecompress = "decompress"

	// CompressionSkipAlreadyCompressed is why Encode left a payload already
	// marked compressed, say by an earlier proxy hop.
	CompressionSkipAlreadyCompressed = "already_compressed"

	// CompressionSkipBelowMinBytes is why Encode left a payload whose data is
	// under [WithMinBytes].
	CompressionSkipBelowMinBytes = "below_min_bytes"

	// CompressionSkipEncoding is why Encode left a payload under an encoding
	// listed through [WithSkip].
	CompressionSkipEncoding = "skipped_encoding"

	// CompressionSkipNotSmaller is why Encode forwarded the original of a payload
	// that compressed to no smaller than it was.
	CompressionSkipNotSmaller = "not_smaller"
)

// ErrUnknownAlgorithm is returned by [Compressor.Decode] for a payload naming an
// algorithm it has no decoder for.
var ErrUnknownAlgorithm = errors.New("unknown algorithm")

type (
	// Compressor compresses payloads and restores them again.
	Compressor struct {
		enc      Compression
		dec      map[string]Compression
		skip     map[string]struct{}
		minBytes int
		observer CompressionObserver
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

	// CompressionObserver is told what a [Compressor] does with each payload. It
	// is called from Encode and Decode as they run, so it should be cheap.
	CompressionObserver interface {
		// Observe is told about each payload compressed or restored. op is
		// CompressionOpCompress or CompressionOpDecompress, in and out are the
		// bytes either side of the algorithm, and d is how long that took. On
		// decode, algorithm is whatever the payload names, so it is only as
		// trustworthy as whoever wrote the payload.
		Observe(op, algorithm string, in, out int, d time.Duration, err error)

		// Skipped is told about each payload Encode forwarded uncompressed, and
		// why, as one of the CompressionSkip reasons.
		Skipped(reason, algorithm string)
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

// WithCompressionObserver reports what the [Compressor] does with each payload
// to o. A nil o reports nothing.
func WithCompressionObserver(o CompressionObserver) CompressorOption {
	return func(c *Compressor) { c.observer = o }
}

// IsCompressed reports whether p carries the full compressed-payload contract,
// the payloads [Compressor.Decode] restores. The marker alone is not proof we
// compressed it; without an algorithm entry there is nothing to restore it with.
func IsCompressed(p *common.Payload) bool {
	md := p.GetMetadata()
	return string(md[MetadataEncoding]) == CompressionEncoding && len(md[MetadataCompressionAlgorithm]) > 0
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
		if enc == CompressionEncoding {
			c.skipped(CompressionSkipAlreadyCompressed)
			continue
		}

		if _, ok := c.skip[enc]; ok {
			c.skipped(CompressionSkipEncoding)
			continue
		}

		if len(p.GetData()) < c.minBytes {
			c.skipped(CompressionSkipBelowMinBytes)
			continue
		}

		data, err := p.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %w", err)
		}

		start := time.Now()
		compressed, err := c.enc.Compress(data)
		c.observe(CompressionOpCompress, c.enc.Encoding(), len(data), len(compressed), start, err)
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
			c.skipped(CompressionSkipNotSmaller)
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
		if !IsCompressed(p) {
			continue
		}

		name := string(p.GetMetadata()[MetadataCompressionAlgorithm])
		start := time.Now()
		og, n, err := c.restore(name, p.Data)
		c.observe(CompressionOpDecompress, name, len(p.Data), n, start, err)
		if err != nil {
			return nil, err
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

// restore decompresses data with the algorithm name and unmarshals the payload
// it held, returning it and the decompressed size.
func (c *Compressor) restore(name string, data []byte) (*common.Payload, int, error) {
	cmp := c.decoder(name)
	if cmp == nil {
		return nil, 0, fmt.Errorf("failed to decompress payload: %w %q", ErrUnknownAlgorithm, name)
	}

	raw, err := cmp.Decompress(data)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decompress payload: %w", err)
	}

	og := new(common.Payload)
	if err := og.Unmarshal(raw); err != nil {
		return nil, len(raw), fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return og, len(raw), nil
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

// observe tells the observer, if there is one, about a payload compressed or
// restored since start.
func (c *Compressor) observe(op, algorithm string, in, out int, start time.Time, err error) {
	if c.observer != nil {
		c.observer.Observe(op, algorithm, in, out, time.Since(start), err)
	}
}

// skipped tells the observer, if there is one, that Encode left a payload
// uncompressed.
func (c *Compressor) skipped(reason string) {
	if c.observer != nil {
		c.observer.Skipped(reason, c.enc.Encoding())
	}
}
