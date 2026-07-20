package compress

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

// LZ4 compresses data using the LZ4 algorithm, which favors very fast
// compression and decompression over ratio. A zero value is ready to use and
// safe for concurrent use.
type LZ4 struct{}

// NewLZ4 returns a ready-to-use [LZ4] compressor.
func NewLZ4() *LZ4 {
	return new(LZ4)
}

// Encoding returns the content-encoding identifier for LZ4-framed data.
func (c *LZ4) Encoding() string {
	return "application/x-lz4"
}

// Compress returns the LZ4-encoded form of data. It does not modify data.
func (c *LZ4) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("failed to compress with LZ4: %w", err)
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("failed to compress with LZ4: %w", err)
	}

	return buf.Bytes(), nil
}

// Decompress returns the decoded form of LZ4-encoded data, or an error if data
// is not a valid LZ4 frame. It does not modify data.
func (c *LZ4) Decompress(data []byte) ([]byte, error) {
	out, err := io.ReadAll(lz4.NewReader(bytes.NewReader(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to decompress with LZ4: %w", err)
	}

	return out, nil
}
