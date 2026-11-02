package compress_test

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/pkg/compress"
)

func TestNewZstd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		level   int
		wantErr bool
	}{
		{"fastest", 1, false},
		{"default", 3, false},
		{"best compression", 22, false},
		{"below range", 0, true},
		{"above range", 23, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, err := compress.NewZstd(tc.level)
			if tc.wantErr {
				require.ErrorContains(t, err, "invalid Zstd level")
				require.Nil(t, c)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, c)
		})
	}
}

func TestZstd(t *testing.T) {
	t.Parallel()

	runCompressorSuite(t, func(opts ...compress.Option) (compressor, error) {
		return compress.NewZstd(3, opts...)
	})
}

func TestZstdEncoding(t *testing.T) {
	t.Parallel()

	c, err := compress.NewZstd(3)
	require.NoError(t, err)
	require.Equal(t, "zstd", c.Encoding())
}

func TestZstdDeclaredWindowOverLimit(t *testing.T) {
	t.Parallel()

	// A multi-segment frame declares a window rather than its content size, so
	// this one is rejected on its header before anything is decoded.
	enc, err := zstd.NewWriter(nil, zstd.WithSingleSegment(false), zstd.WithWindowSize(1<<20))
	require.NoError(t, err)

	frame := enc.EncodeAll(bytes.Repeat([]byte("a"), 64<<10), nil)

	var h zstd.Header
	require.NoError(t, h.Decode(frame))
	require.Greater(t, h.WindowSize, uint64(64<<10))

	c, err := compress.NewZstd(3, compress.WithMaxDecodedSize(64<<10))
	require.NoError(t, err)

	_, err = c.Decompress(frame)
	require.ErrorIs(t, err, compress.ErrTooLarge)
}
