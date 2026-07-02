package compress_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/pkg/compress"
)

func TestLZ4(t *testing.T) {
	t.Parallel()

	runCompressorSuite(t, func(opts ...compress.Option) (compressor, error) {
		return compress.NewLZ4(opts...)
	})
}

func TestLZ4Encoding(t *testing.T) {
	t.Parallel()

	c, err := compress.NewLZ4()
	require.NoError(t, err)
	require.Equal(t, "lz4", c.Encoding())
}
