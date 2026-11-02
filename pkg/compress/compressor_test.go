package compress_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/pkg/compress"
)

type (
	compressor interface {
		Compress([]byte) ([]byte, error)
		Decompress([]byte) ([]byte, error)
	}

	newCompressor func(...compress.Option) (compressor, error)
)

func runCompressorSuite(t *testing.T, newC newCompressor) {
	t.Helper()

	c, err := newC()
	require.NoError(t, err)

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
				// bytes.Equal, not require.Equal: decompressing empty input may
				// return nil, which require.Equal treats as unequal to []byte{}.
				require.True(t, bytes.Equal(tc.data, got))
			})
		}
	})

	t.Run("compress", func(t *testing.T) {
		t.Parallel()

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

	t.Run("max decoded size", func(t *testing.T) {
		t.Parallel()

		t.Run("at limit", func(t *testing.T) {
			t.Parallel()

			c, err := newC(compress.WithMaxDecodedSize(64 << 10))
			require.NoError(t, err)

			data := bytes.Repeat([]byte("a"), 64<<10)
			compressed, err := c.Compress(data)
			require.NoError(t, err)

			got, err := c.Decompress(compressed)
			require.NoError(t, err)
			require.Equal(t, data, got)
		})

		t.Run("over limit", func(t *testing.T) {
			t.Parallel()

			c, err := newC(compress.WithMaxDecodedSize(64 << 10))
			require.NoError(t, err)

			compressed, err := c.Compress(bytes.Repeat([]byte("a"), 64<<10+1))
			require.NoError(t, err)

			_, err = c.Decompress(compressed)
			require.ErrorIs(t, err, compress.ErrTooLarge)
		})

		t.Run("default is 4MiB", func(t *testing.T) {
			t.Parallel()

			atLimit, err := c.Compress(bytes.Repeat([]byte("a"), 4<<20))
			require.NoError(t, err)

			_, err = c.Decompress(atLimit)
			require.NoError(t, err)

			overLimit, err := c.Compress(bytes.Repeat([]byte("a"), 4<<20+1))
			require.NoError(t, err)

			_, err = c.Decompress(overLimit)
			require.ErrorIs(t, err, compress.ErrTooLarge)
		})

		t.Run("minimum is 2KiB", func(t *testing.T) {
			t.Parallel()

			for _, n := range []int{-1, 0, 2<<10 - 1} {
				_, err := newC(compress.WithMaxDecodedSize(n))
				require.ErrorContains(t, err, "invalid max decoded size")
			}

			c, err := newC(compress.WithMaxDecodedSize(2 << 10))
			require.NoError(t, err)

			// A 1KiB payload declares a 2KiB Zstd window, the largest any small
			// payload needs, so it must decode under the minimum cap.
			data := bytes.Repeat([]byte("a"), 1<<10)
			compressed, err := c.Compress(data)
			require.NoError(t, err)

			got, err := c.Decompress(compressed)
			require.NoError(t, err)
			require.Equal(t, data, got)
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
