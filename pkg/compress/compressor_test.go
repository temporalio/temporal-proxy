package compress_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type compressor interface {
	Compress([]byte) ([]byte, error)
	Decompress([]byte) ([]byte, error)
}

func runCompressorSuite(t *testing.T, c compressor) {
	t.Helper()

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			data []byte
		}{
			{"empty", []byte{}},
			{"ascii", []byte("hello, world")},
			{"binary", []byte{0x00, 0xFF, 0x42, 0x13}},
			{"large", bytes.Repeat([]byte("a"), 64*1024)},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				compressed, err := c.Compress(tc.data)
				require.NoError(t, err)

				got, err := c.Decompress(compressed)
				require.NoError(t, err)
				require.True(t, bytes.Equal(tc.data, got))
			})
		}
	})

	t.Run("compress", func(t *testing.T) {
		t.Parallel()

		t.Run("output differs from input", func(t *testing.T) {
			t.Parallel()

			data := bytes.Repeat([]byte("a"), 1024)
			compressed, err := c.Compress(data)
			require.NoError(t, err)
			require.NotEqual(t, data, compressed)
		})

		t.Run("compressible data shrinks", func(t *testing.T) {
			t.Parallel()

			data := bytes.Repeat([]byte("a"), 1024)
			compressed, err := c.Compress(data)
			require.NoError(t, err)
			require.Less(t, len(compressed), len(data))
		})

		t.Run("does not modify input", func(t *testing.T) {
			t.Parallel()

			data := []byte("hello, world")
			original := bytes.Clone(data)
			_, err := c.Compress(data)
			require.NoError(t, err)
			require.Equal(t, original, data)
		})
	})

	t.Run("decompress", func(t *testing.T) {
		t.Parallel()

		t.Run("invalid data", func(t *testing.T) {
			t.Parallel()

			_, err := c.Decompress([]byte("not compressed"))
			require.Error(t, err)
		})

		t.Run("truncated frame", func(t *testing.T) {
			t.Parallel()

			compressed, err := c.Compress([]byte("hello, world"))
			require.NoError(t, err)

			_, err = c.Decompress(compressed[:len(compressed)/2])
			require.Error(t, err)
		})

		t.Run("does not modify input", func(t *testing.T) {
			t.Parallel()

			compressed, err := c.Compress([]byte("hello, world"))
			require.NoError(t, err)

			original := bytes.Clone(compressed)
			_, err = c.Decompress(compressed)
			require.NoError(t, err)
			require.Equal(t, original, compressed)
		})
	})

	t.Run("concurrent", func(t *testing.T) {
		t.Parallel()

		const n = 20

		var wg sync.WaitGroup

		results := make([][]byte, n)
		errs := make([]error, n)

		for i := range n {
			wg.Go(func() {
				data := bytes.Repeat([]byte{byte(i)}, 512)
				compressed, compErr := c.Compress(data)
				if compErr != nil {
					errs[i] = compErr
					return
				}

				decompressed, decompErr := c.Decompress(compressed)
				if decompErr != nil {
					errs[i] = decompErr
					return
				}

				results[i] = decompressed
			})
		}

		wg.Wait()

		for i := range n {
			require.NoError(t, errs[i])
			require.Equal(t, bytes.Repeat([]byte{byte(i)}, 512), results[i])
		}
	})
}
