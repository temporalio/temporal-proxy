package codec_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/pkg/codec"
)

func TestNewChainNoCodecs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []codec.Option
	}{
		{name: "no options"},
		{name: "nil cipher is ignored", opts: []codec.Option{codec.WithCipher(nil)}},
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
