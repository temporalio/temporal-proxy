package compress

import (
	"fmt"

	"github.com/golang/snappy"
)

// Snappy compresses data using Google's Snappy algorithm, which favors speed
// over ratio. It is safe for concurrent use; construct it with [NewSnappy].
type Snappy struct {
	maxDecodedSize int
}

// NewSnappy returns a [Snappy] compressor configured by opts, or an error if
// an option is invalid.
func NewSnappy(opts ...Option) (*Snappy, error) {
	o, err := buildOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create Snappy compressor: %w", err)
	}

	return &Snappy{maxDecodedSize: o.maxDecodedSize}, nil
}

// Encoding returns the algorithm name, snappy.
func (c *Snappy) Encoding() string {
	return "snappy"
}

// Compress returns data encoded in the Snappy block format, which suits
// payloads already held whole in memory. It never fails and does not modify
// data.
func (c *Snappy) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

// Decompress returns the decoded form of Snappy block-encoded data, or an error
// if data is not a valid Snappy block or decodes to more than the maximum
// decoded size ([ErrTooLarge]). It does not modify data.
func (c *Snappy) Decompress(data []byte) ([]byte, error) {
	// The block header declares the decoded length, which Decode allocates up
	// front, so check it before decoding.
	n, err := snappy.DecodedLen(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress with Snappy: %w", err)
	}

	if n > c.maxDecodedSize {
		return nil, fmt.Errorf("failed to decompress with Snappy: %w", ErrTooLarge)
	}

	out, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress with Snappy: %w", err)
	}

	return out, nil
}
