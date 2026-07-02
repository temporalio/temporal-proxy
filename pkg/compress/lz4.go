package compress

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

// LZ4 compresses data using the LZ4 algorithm, which favors very fast
// compression and decompression over ratio. It is safe for concurrent use;
// construct it with [NewLZ4].
type LZ4 struct {
	maxDecodedSize int
}

// NewLZ4 returns an [LZ4] compressor configured by opts, or an error if an
// option is invalid.
func NewLZ4(opts ...Option) (*LZ4, error) {
	o, err := buildOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create LZ4 compressor: %w", err)
	}

	return &LZ4{maxDecodedSize: o.maxDecodedSize}, nil
}

// Encoding returns the algorithm name, lz4.
func (c *LZ4) Encoding() string {
	return "lz4"
}

// Compress returns the LZ4-encoded form of data. It does not modify data.
func (c *LZ4) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("failed to compress with LZ4: %w", err)
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("failed to compress with LZ4: %w", err)
	}

	return buf.Bytes(), nil
}

// Decompress returns the decoded form of LZ4-encoded data, or an error if data
// is not a valid LZ4 frame or decodes to more than the maximum decoded size
// ([ErrTooLarge]). It does not modify data.
func (c *LZ4) Decompress(data []byte) ([]byte, error) {
	// Read one byte past the limit so oversized output is detectable.
	r := io.LimitReader(lz4.NewReader(bytes.NewReader(data)), int64(c.maxDecodedSize)+1)
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress with LZ4: %w", err)
	}

	if len(out) > c.maxDecodedSize {
		return nil, fmt.Errorf("failed to decompress with LZ4: %w", ErrTooLarge)
	}

	return out, nil
}
