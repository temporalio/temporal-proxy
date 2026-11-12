package codec_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/pkg/codec"
)

func TestNewChainNoCodecs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []codec.Option
	}{
		{name: "no options"},
		{name: "nil cipher is ignored", opts: []codec.Option{codec.WithCipher(nil)}},
		{
			name: "encryptor options alone add no codec",
			opts: []codec.Option{codec.WithEncryptorOptions(codec.WithSkipEncodings("json/plain"))},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			chain := codec.NewChain(tc.opts...)
			payloads := []*common.Payload{testPayload("json/plain", `"hi"`)}

			// A chain with no codecs hands payloads straight back.
			encoded, err := chain.Encode(payloads)
			require.NoError(t, err)
			require.Len(t, encoded, 1)
			require.Same(t, payloads[0], encoded[0])

			decoded, err := chain.Decode(payloads)
			require.NoError(t, err)
			require.Len(t, decoded, 1)
			require.Same(t, payloads[0], decoded[0])
		})
	}
}

func TestNewChainWithCipher(t *testing.T) {
	t.Parallel()

	original := []*common.Payload{testPayload("json/plain", `"hi"`)}
	want := proto.Clone(original[0]).(*common.Payload)

	chain := codec.NewChain(codec.WithCipher(&fakeCipher{}))

	sealed, err := chain.Encode(original)
	require.NoError(t, err)
	require.Len(t, sealed, 1)

	// The chain seals exactly as the encryption codec alone does.
	direct, err := codec.NewEncryptor(&fakeCipher{}).Encode([]*common.Payload{want})
	require.NoError(t, err)
	require.True(t, proto.Equal(direct[0], sealed[0]))

	opened, err := chain.Decode(sealed)
	require.NoError(t, err)
	require.Len(t, opened, 1)
	require.True(t, proto.Equal(want, opened[0]))
}

func TestChainCipherErrors(t *testing.T) {
	t.Parallel()

	t.Run("encode", func(t *testing.T) {
		t.Parallel()

		chain := codec.NewChain(codec.WithCipher(&fakeCipher{encErr: errors.New("kms unavailable")}))

		_, err := chain.Encode([]*common.Payload{testPayload("json/plain", "x")})
		require.ErrorContains(t, err, "failed to encrypt payload")
	})

	t.Run("decode", func(t *testing.T) {
		t.Parallel()

		c := &fakeCipher{}
		chain := codec.NewChain(codec.WithCipher(c))

		sealed, err := chain.Encode([]*common.Payload{testPayload("json/plain", "x")})
		require.NoError(t, err)

		c.decErr = errors.New("unwrap failed")
		_, err = chain.Decode(sealed)
		require.ErrorContains(t, err, "failed to decrypt payload")
	})
}

// TestNewChainForwardsEncryptorOptions shows WithEncryptorOptions reaches the
// encryptor NewChain builds. Dropping o.encryptor from the NewEncryptor call in
// NewChain fails it.
func TestNewChainForwardsEncryptorOptions(t *testing.T) {
	t.Parallel()

	chain := codec.NewChain(
		codec.WithCipher(&fakeCipher{}),
		codec.WithEncryptorOptions(codec.WithSkipEncodings("acme/aes-gcm")),
	)

	p := testPayload("acme/aes-gcm", "ciphertext-from-a-worker")
	got, err := chain.Encode([]*common.Payload{p})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Same(t, p, got[0])
}

// TestNewChainCompressesBeforeEncrypting shows the chain compresses payloads
// before sealing them, since ciphertext does not compress. Appending the
// compressor after the encryptor in NewChain fails it.
func TestNewChainCompressesBeforeEncrypting(t *testing.T) {
	t.Parallel()

	original := []*common.Payload{compressiblePayload()}
	want := proto.Clone(original[0]).(*common.Payload)

	chain := codec.NewChain(
		codec.WithCipher(&fakeCipher{}),
		codec.WithCompression(newSnappy(t)),
	)

	sealed, err := chain.Encode(original)
	require.NoError(t, err)
	require.Len(t, sealed, 1)
	require.Equal(t, codec.EncryptionEncoding, string(sealed[0].Metadata[codec.MetadataEncoding]))

	// Opening the seal reveals the compressed payload, not the original.
	opened, err := codec.NewEncryptor(&fakeCipher{}).Decode(sealed)
	require.NoError(t, err)
	require.Equal(t, codec.CompressionEncoding, string(opened[0].Metadata[codec.MetadataEncoding]))

	restored, err := chain.Decode(sealed)
	require.NoError(t, err)
	require.Len(t, restored, 1)
	require.True(t, proto.Equal(want, restored[0]), "payload did not round-trip")
}

// TestNewChainDecodesWithCompressionOff shows a nil Compression still restores
// compressed payloads. Skipping the compressor when the Compression is nil, the
// way WithCipher treats a nil cipher, fails it.
func TestNewChainDecodesWithCompressionOff(t *testing.T) {
	t.Parallel()

	snappy := newSnappy(t)
	original := []*common.Payload{compressiblePayload()}
	want := proto.Clone(original[0]).(*common.Payload)

	compressed, err := codec.NewChain(codec.WithCompression(snappy)).Encode(original)
	require.NoError(t, err)

	restored, err := codec.NewChain(codec.WithCompression(nil, codec.WithDecoders(snappy))).Decode(compressed)
	require.NoError(t, err)
	require.Len(t, restored, 1)
	require.True(t, proto.Equal(want, restored[0]), "payload did not round-trip")
}

// TestNewChainSharesCompressionOption shows one WithCompression option, built
// once, can back chains built concurrently, the way a chain is built per
// request. A Compressor that writes to its own state while decoding fails it
// under the race detector.
func TestNewChainSharesCompressionOption(t *testing.T) {
	t.Parallel()

	snappy, zstd := newSnappy(t), newZstd(t)
	compressed, err := codec.NewChain(codec.WithCompression(zstd)).Encode([]*common.Payload{compressiblePayload()})
	require.NoError(t, err)

	opt := codec.WithCompression(snappy, codec.WithDecoders(zstd))

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			restored, err := codec.NewChain(opt).Decode(compressed)
			if err != nil || !proto.Equal(compressiblePayload(), restored[0]) {
				t.Errorf("payload did not round-trip: %v", err)
			}
		})
	}

	wg.Wait()
}

// TestNewChainSkipsBeforeCompressing shows a payload under a skipped encoding is
// neither compressed nor sealed, even when it would compress. Dropping the skip
// check from Compressor.Encode fails it, since the encryptor then sees
// CompressionEncoding instead of the skipped one.
func TestNewChainSkipsBeforeCompressing(t *testing.T) {
	t.Parallel()

	p := compressiblePayload()
	p.Metadata[codec.MetadataEncoding] = []byte("acme/aes-gcm")

	c := &fakeCipher{}
	chain := codec.NewChain(
		codec.WithCipher(c),
		codec.WithCompression(newSnappy(t)),
		codec.WithSkip("acme/aes-gcm"),
	)

	got, err := chain.Encode([]*common.Payload{p})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Same(t, p, got[0])
	require.Empty(t, c.encrypted)
}

// TestNewChainSkipDoesNotLeak shows WithSkip on one chain leaves another chain
// built from the same WithCompression option compressing as before. Setting the
// skip set on the shared Compressor instead of a copy fails it.
func TestNewChainSkipDoesNotLeak(t *testing.T) {
	t.Parallel()

	opt := codec.WithCompression(newSnappy(t))
	_ = codec.NewChain(opt, codec.WithSkip("acme/aes-gcm"))

	p := compressiblePayload()
	p.Metadata[codec.MetadataEncoding] = []byte("acme/aes-gcm")

	got, err := codec.NewChain(opt).Encode([]*common.Payload{p})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, codec.CompressionEncoding, string(got[0].Metadata[codec.MetadataEncoding]))
}

// TestNewChainSkipReachesEncryptor shows WithSkip sets the encryptor's list
// without WithEncryptorOptions, and that WithSkipEncodings given there replaces
// it. Not passing o.skip to NewEncryptor fails the first row; appending it after
// the caller's options instead of before fails the second.
func TestNewChainSkipReachesEncryptor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		opts     []codec.Option
		wantSkip bool
	}{
		{
			name:     "WithSkip alone",
			opts:     []codec.Option{codec.WithSkip("acme/aes-gcm")},
			wantSkip: true,
		},
		{
			name: "WithSkipEncodings overrides",
			opts: []codec.Option{
				codec.WithSkip("acme/aes-gcm"),
				codec.WithEncryptorOptions(codec.WithSkipEncodings("other/enc")),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := &fakeCipher{}
			chain := codec.NewChain(append([]codec.Option{codec.WithCipher(c)}, tc.opts...)...)

			p := testPayload("acme/aes-gcm", "ciphertext-from-a-worker")
			got, err := chain.Encode([]*common.Payload{p})
			require.NoError(t, err)
			require.Len(t, got, 1)

			if tc.wantSkip {
				require.Same(t, p, got[0])
				require.Empty(t, c.encrypted)

				return
			}

			require.Equal(t, codec.EncryptionEncoding, string(got[0].Metadata[codec.MetadataEncoding]))
		})
	}
}
