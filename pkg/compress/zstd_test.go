package compress_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/compress"
)

func TestNewZstdCompressor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		level int
	}{
		{"fastest", 1},
		{"default", 3},
		{"best compression", 19},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, err := compress.NewZstd(tc.level)
			require.NoError(t, err)
			require.NotNil(t, c)
		})
	}
}

func TestZstdCompressor(t *testing.T) {
	t.Parallel()

	c, err := compress.NewZstd(3)
	require.NoError(t, err)

	runCompressorSuite(t, c)
}

func TestZstdEncoding(t *testing.T) {
	t.Parallel()

	c, err := compress.NewZstd(3)
	require.NoError(t, err)
	require.Equal(t, "application/zstd", c.Encoding())
}
