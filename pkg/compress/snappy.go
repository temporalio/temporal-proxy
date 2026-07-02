package compress

import (
	"fmt"

	"github.com/golang/snappy"
)

// Snappy compresses data using Google's Snappy algorithm, which favors speed
// over ratio. A zero value is ready to use and safe for concurrent use.
type Snappy struct{}

// NewSnappy returns a ready-to-use [Snappy] compressor.
func NewSnappy() *Snappy {
	return new(Snappy)
}

// Encoding returns the content-encoding identifier for Snappy-framed data.
func (c *Snappy) Encoding() string {
	return "application/x-snappy-framed"
}

// Compress returns the Snappy-encoded form of data. It never fails and does
// not modify data.
func (c *Snappy) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

// Decompress returns the decoded form of Snappy-encoded data, or an error if
// data is not a valid Snappy frame. It does not modify data.
func (c *Snappy) Decompress(data []byte) ([]byte, error) {
	out, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress with snappy: %w", err)
	}

	return out, nil
}
