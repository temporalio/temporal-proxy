package compress

import (
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

// NewZstd returns a [Zstd] compressor configured at the given zstd level (see
// [zstd.EncoderLevelFromZstd]). It returns an error if the encoder or decoder
// cannot be created.
func NewZstd(level int) (*Zstd, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
	if err != nil {
		return nil, fmt.Errorf("failed to create Zstd encoder level: %d, %w", level, err)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Zstd decoder: %w", err)
	}

	return &Zstd{
		enc: enc,
		dec: dec,
	}, nil
}

// Encoding returns the content-encoding identifier for Zstandard data.
func (c *Zstd) Encoding() string {
	return "application/zstd"
}

// Compress returns the Zstandard-encoded form of data. It never fails and does
// not modify data.
func (c *Zstd) Compress(data []byte) ([]byte, error) {
	return c.enc.EncodeAll(data, make([]byte, 0, len(data))), nil
}

// Decompress returns the decoded form of Zstandard-encoded data, or an error
// if data is not a valid Zstandard frame. It does not modify data.
func (c *Zstd) Decompress(data []byte) ([]byte, error) {
	ed, err := c.dec.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress with zstd: %w", err)
	}

	return ed, nil
}
