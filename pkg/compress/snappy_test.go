package compress_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/pkg/compress"
)

func TestSnappy(t *testing.T) {
	t.Parallel()

	runCompressorSuite(t, func(opts ...compress.Option) (compressor, error) {
		return compress.NewSnappy(opts...)
	})
}

func TestSnappyEncoding(t *testing.T) {
	t.Parallel()

	c, err := compress.NewSnappy()
	require.NoError(t, err)
	require.Equal(t, "snappy", c.Encoding())
}

func TestSnappyDeclaredLengthOverLimit(t *testing.T) {
	t.Parallel()

	c, err := compress.NewSnappy()
	require.NoError(t, err)

	// A bare block header declaring a 4GiB decoded length, with no body.
	_, err = c.Decompress([]byte{0xff, 0xff, 0xff, 0xff, 0x0f})
	require.ErrorIs(t, err, compress.ErrTooLarge)
}
