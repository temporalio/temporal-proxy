package compress

import (
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Zstd compresses data using the Zstandard algorithm, which offers higher
// ratios than [Snappy] at the cost of more CPU. It holds a reusable encoder
// and decoder and is safe for concurrent use; construct it with [NewZstd].
type Zstd struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

// NewZstd returns a [Zstd] compressor at the given zstd level, 1-22 (see
// [zstd.EncoderLevelFromZstd]), configured by opts. It returns an error if
// level or an option is invalid, or the encoder or decoder cannot be created.
func NewZstd(level int, opts ...Option) (*Zstd, error) {
	if level < 1 || level > 22 {
		return nil, fmt.Errorf("invalid Zstd level %d: must be 1-22", level)
	}

	o, err := buildOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create Zstd compressor: %w", err)
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
	if err != nil {
		return nil, fmt.Errorf("failed to create Zstd encoder (level %d): %w", level, err)
	}

	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(uint64(o.maxDecodedSize)))
	if err != nil {
		return nil, fmt.Errorf("failed to create Zstd decoder: %w", err)
	}

	return &Zstd{
		enc: enc,
		dec: dec,
	}, nil
}

// Encoding returns the algorithm name, zstd.
func (c *Zstd) Encoding() string {
	return "zstd"
}

// Compress returns the Zstandard-encoded form of data. It never fails and does
// not modify data.
func (c *Zstd) Compress(data []byte) ([]byte, error) {
	return c.enc.EncodeAll(data, make([]byte, 0, len(data))), nil
}

// Decompress returns the decoded form of Zstandard-encoded data, or an error
// if data is not a valid Zstandard frame or decodes to more than the maximum
// decoded size ([ErrTooLarge]). It does not modify data.
func (c *Zstd) Decompress(data []byte) ([]byte, error) {
	out, err := c.dec.DecodeAll(data, nil)
	// The decoder caps the declared window at the max decoded size too, so an
	// oversized window is also a too-large frame.
	if errors.Is(err, zstd.ErrDecoderSizeExceeded) || errors.Is(err, zstd.ErrWindowSizeExceeded) {
		return nil, fmt.Errorf("failed to decompress with Zstd: %w", ErrTooLarge)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to decompress with Zstd: %w", err)
	}

	return out, nil
}
