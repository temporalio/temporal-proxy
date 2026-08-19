package codec_test

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/pkg/codec"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

const (
	testKEKID  = "test-kek"
	testDEK    = "wrapped-dek"
	sealPrefix = "sealed:"
)

// fakeCipher is a reversible in-memory Cipher: Encrypt prefixes the plaintext
// with sealPrefix so ciphertext is observably distinct from cleartext, and
// Decrypt strips it. It records what it was handed, and errors or a fixed
// Decrypt result can be injected to exercise the failure paths. Recording is
// unsynchronized, so each test (or parallel subtest) needs its own.
type fakeCipher struct {
	encrypted [][]byte          // plaintexts passed to Encrypt, in call order
	decrypted []*crypto.Message // messages passed to Decrypt, in call order
	encErr    error             // when set, Encrypt returns it
	decErr    error             // when set, Decrypt returns it
	decReturn []byte            // when set, Decrypt returns these bytes instead of the unsealed plaintext
}

func TestEncryptorRoundtrip(t *testing.T) {
	t.Parallel()

	c := &fakeCipher{}
	original := []*common.Payload{
		testPayload("json/plain", `"first"`),
		testPayload("binary/null", ""),
	}
	want := []*common.Payload{
		proto.Clone(original[0]).(*common.Payload),
		proto.Clone(original[1]).(*common.Payload),
	}

	sealed, err := codec.NewEncryptor(c).Encode(original)
	require.NoError(t, err)
	require.Len(t, sealed, len(original))

	for i, p := range sealed {
		require.Equal(t, codec.EncryptionEncoding, string(p.Metadata[codec.MetadataEncoding]))
		require.Equal(t, testKEKID, string(p.Metadata[codec.MetadataEncryptionKeyID]))
		require.Equal(t, testDEK, string(p.Metadata[codec.MetadataEncryptionDEK]))
		// The data is ciphertext, never the payload's own plaintext.
		require.True(t, bytes.HasPrefix(p.Data, []byte(sealPrefix)))
		require.NotEqual(t, want[i].Data, p.Data)
	}

	opened, err := codec.NewEncryptor(c).Decode(sealed)
	require.NoError(t, err)
	require.Len(t, opened, len(want))

	// Each payload is restored whole, original metadata included.
	for i := range want {
		require.True(t, proto.Equal(want[i], opened[i]), "payload %d did not round-trip", i)
	}
}

func TestEncryptorEncodeSealsEachPayloadWhole(t *testing.T) {
	t.Parallel()

	c := &fakeCipher{}
	p := testPayload("json/plain", `"one"`)

	_, err := codec.NewEncryptor(c).Encode([]*common.Payload{p, p})
	require.NoError(t, err)

	// One Encrypt per payload, each handed the marshaled payload rather than
	// just its data, which is what lets Decode restore the metadata too.
	require.Len(t, c.encrypted, 2)
	for _, got := range c.encrypted {
		require.Equal(t, mustMarshal(t, p), got)
	}
}

func TestEncryptorEncodeEmpty(t *testing.T) {
	t.Parallel()

	c := &fakeCipher{}

	out, err := codec.NewEncryptor(c).Encode(nil)
	require.NoError(t, err)
	require.Empty(t, out)
	require.Empty(t, c.encrypted)
}

func TestEncryptorDecodePassesThrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload *common.Payload
	}{
		{
			name:    "another encoding",
			payload: testPayload("json/plain", `"visible"`),
		},
		{
			name:    "no encoding key",
			payload: &common.Payload{Data: []byte("raw")},
		},
		{
			name:    "no metadata at all",
			payload: &common.Payload{},
		},
		{
			name:    "encoding is a prefix of the marker",
			payload: testPayload("binary", "x"),
		},
		{
			name:    "marked with no key material at all",
			payload: markedPayload(nil),
		},
		{
			name: "marked with no key ID",
			payload: markedPayload(map[string][]byte{
				codec.MetadataEncryptionDEK: []byte(testDEK),
			}),
		},
		{
			name: "marked with no wrapped DEK",
			payload: markedPayload(map[string][]byte{
				codec.MetadataEncryptionKeyID: []byte(testKEKID),
			}),
		},
		{
			name: "marked with an empty key ID",
			payload: markedPayload(map[string][]byte{
				codec.MetadataEncryptionKeyID: {},
				codec.MetadataEncryptionDEK:   []byte(testDEK),
			}),
		},
		{
			name: "marked with an empty wrapped DEK",
			payload: markedPayload(map[string][]byte{
				codec.MetadataEncryptionKeyID: []byte(testKEKID),
				codec.MetadataEncryptionDEK:   {},
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := &fakeCipher{}
			out, err := codec.NewEncryptor(c).Decode([]*common.Payload{tc.payload})
			require.NoError(t, err)

			require.Len(t, out, 1)
			require.Same(t, tc.payload, out[0], "an unsealed payload should pass through untouched")
			require.Empty(t, c.decrypted, "Decrypt must not be called for an unsealed payload")
		})
	}
}

func TestEncryptorDecodeMixedBatch(t *testing.T) {
	t.Parallel()

	c := &fakeCipher{}
	orig := testPayload("json/plain", `"secret"`)
	plain := testPayload("json/plain", `"visible"`)

	sealed, err := codec.NewEncryptor(c).Encode([]*common.Payload{orig})
	require.NoError(t, err)

	out, err := codec.NewEncryptor(c).Decode([]*common.Payload{plain, sealed[0], plain})
	require.NoError(t, err)

	// Order is preserved and only the sealed payload is opened.
	require.Len(t, out, 3)
	require.Same(t, plain, out[0])
	require.True(t, proto.Equal(orig, out[1]))
	require.Same(t, plain, out[2])
	require.Len(t, c.decrypted, 1)
}

func TestEncryptorDecodePassesKeyMaterial(t *testing.T) {
	t.Parallel()

	c := &fakeCipher{}
	sealed := &common.Payload{
		Metadata: map[string][]byte{
			codec.MetadataEncoding:        []byte(codec.EncryptionEncoding),
			codec.MetadataEncryptionKeyID: []byte("kek-from-metadata"),
			codec.MetadataEncryptionDEK:   []byte("dek-from-metadata"),
		},
		Data: append([]byte(sealPrefix), mustMarshal(t, testPayload("json/plain", "x"))...),
	}

	_, err := codec.NewEncryptor(c).Decode([]*common.Payload{sealed})
	require.NoError(t, err)

	// The key material the cipher needs is read back off the payload, which is
	// what makes a sealed payload openable on its own.
	require.Len(t, c.decrypted, 1)
	require.Equal(t, sealed.Data, c.decrypted[0].Ciphertext)
	require.Equal(t, "kek-from-metadata", c.decrypted[0].KeyMaterial.KEKID)
	require.Equal(t, "dek-from-metadata", c.decrypted[0].KeyMaterial.EncryptedDEK)
}

func TestEncryptorErrors(t *testing.T) {
	t.Parallel()

	t.Run("encrypt error", func(t *testing.T) {
		t.Parallel()

		c := &fakeCipher{encErr: errors.New("kms unavailable")}

		_, err := codec.NewEncryptor(c).Encode([]*common.Payload{testPayload("json/plain", "x")})
		require.ErrorContains(t, err, "failed to encrypt payload")
		require.ErrorContains(t, err, "kms unavailable")
	})

	t.Run("decrypt error", func(t *testing.T) {
		t.Parallel()

		c := &fakeCipher{}
		sealed, err := codec.NewEncryptor(c).Encode([]*common.Payload{testPayload("json/plain", "x")})
		require.NoError(t, err)

		c.decErr = errors.New("unwrap failed")
		_, err = codec.NewEncryptor(c).Decode(sealed)
		require.ErrorContains(t, err, "failed to decrypt payload")
		require.ErrorContains(t, err, "unwrap failed")
	})

	t.Run("plaintext is not a payload", func(t *testing.T) {
		t.Parallel()

		c := &fakeCipher{}
		sealed, err := codec.NewEncryptor(c).Encode([]*common.Payload{testPayload("json/plain", "x")})
		require.NoError(t, err)

		c.decReturn = []byte{0xFF, 0xFF, 0xFF}
		_, err = codec.NewEncryptor(c).Decode(sealed)
		require.ErrorContains(t, err, "failed to unmarshal payload")
	})
}

func (f *fakeCipher) Encrypt(data []byte) (*crypto.Message, error) {
	if f.encErr != nil {
		return nil, f.encErr
	}

	f.encrypted = append(f.encrypted, bytes.Clone(data))

	return &crypto.Message{
		Ciphertext:  append([]byte(sealPrefix), data...),
		KeyMaterial: &crypto.DEKMaterial{KEKID: testKEKID, EncryptedDEK: testDEK},
	}, nil
}

func (f *fakeCipher) Decrypt(msg *crypto.Message) ([]byte, error) {
	f.decrypted = append(f.decrypted, msg)

	if f.decErr != nil {
		return nil, f.decErr
	}
	if f.decReturn != nil {
		return f.decReturn, nil
	}

	if !bytes.HasPrefix(msg.Ciphertext, []byte(sealPrefix)) {
		return nil, fmt.Errorf("ciphertext was not sealed by fakeCipher")
	}

	return bytes.TrimPrefix(msg.Ciphertext, []byte(sealPrefix)), nil
}

func testPayload(encoding, data string) *common.Payload {
	return &common.Payload{
		Metadata: map[string][]byte{codec.MetadataEncoding: []byte(encoding)},
		Data:     []byte(data),
	}
}

// markedPayload builds a payload carrying the sealed-payload encoding marker
// plus md, so a test can vary the key material a marked payload claims to have.
func markedPayload(md map[string][]byte) *common.Payload {
	meta := map[string][]byte{codec.MetadataEncoding: []byte(codec.EncryptionEncoding)}
	maps.Copy(meta, md)

	return &common.Payload{Metadata: meta, Data: []byte("ciphertext")}
}

func mustMarshal(t *testing.T, p *common.Payload) []byte {
	t.Helper()

	data, err := p.Marshal()
	require.NoError(t, err)

	return data
}
