package ext_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/api/ext/v1"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestKeyMaterial_MarshalRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		material *ext.KeyMaterial
	}{
		{
			name: "every field set",
			material: &ext.KeyMaterial{
				EncryptedDek: []byte{0x00, 0x01, 0xff, 0xfe},
				Version:      "2",
				Namespace:    "orders",
				Opaque:       []byte(`{"keyRing":"codec"}`),
			},
		},
		{
			name:     "a wrapped DEK on its own",
			material: &ext.KeyMaterial{EncryptedDek: []byte("wrapped")},
		},
		{
			// A server that identifies its key entirely through opaque leaves
			// both documented fields empty.
			name: "a wrapped DEK identified only by opaque",
			material: &ext.KeyMaterial{
				EncryptedDek: []byte("wrapped"),
				Opaque:       []byte{0x7f, 0x00, 0xff},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			packed, err := tt.material.Marshal()
			require.NoError(t, err)
			require.NotEmpty(t, packed)

			got, err := ext.UnmarshalKeyMaterial(packed)
			require.NoError(t, err)
			require.Equal(t, tt.material.EncryptedDek, got.EncryptedDek)
			require.Equal(t, tt.material.Version, got.Version)
			require.Equal(t, tt.material.Namespace, got.Namespace)
			require.Equal(t, tt.material.Opaque, got.Opaque)
		})
	}
}

func TestKeyMaterial_MarshalRejectsMissingDEK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		material *ext.KeyMaterial
	}{
		{
			name:     "no wrapped DEK",
			material: &ext.KeyMaterial{Namespace: "orders"},
		},
		{
			name:     "no key material at all",
			material: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			packed, err := tt.material.Marshal()
			require.Nil(t, packed)

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)
			require.Equal(t, validation.Errors{
				{Field: "encrypted_dek", Message: "must not be empty"},
			}, errs)
		})
	}
}

func TestUnmarshalKeyMaterial(t *testing.T) {
	t.Parallel()

	t.Run("rejects bytes that are not key material", func(t *testing.T) {
		t.Parallel()

		// Field number 0 is not legal on the wire, so this can never be any
		// message.
		got, err := ext.UnmarshalKeyMaterial([]byte{0x00, 0x01, 0x02})
		require.Error(t, err)
		require.Nil(t, got)
	})

	t.Run("accepts empty input, which then fails Validate", func(t *testing.T) {
		t.Parallel()

		// proto3 cannot tell "no fields set" from "not key material at all",
		// which is why Unmarshal leaves the checking to Validate.
		got, err := ext.UnmarshalKeyMaterial(nil)
		require.NoError(t, err)
		require.Empty(t, got.EncryptedDek)
		require.Error(t, got.Validate())
	})
}

func TestKeyMaterial_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		material *ext.KeyMaterial
		wantErrs []validation.Error
	}{
		{
			name: "every field set",
			material: &ext.KeyMaterial{
				EncryptedDek: []byte("wrapped"),
				Version:      "2",
				Namespace:    "orders",
				Opaque:       []byte("mine"),
			},
		},
		{
			name:     "a wrapped DEK on its own",
			material: &ext.KeyMaterial{EncryptedDek: []byte("wrapped")},
		},
		{
			name:     "no wrapped DEK",
			material: &ext.KeyMaterial{Version: "2", Namespace: "orders"},
			wantErrs: []validation.Error{
				{Field: "encrypted_dek", Message: "must not be empty"},
			},
		},
		{
			// An empty slice is as unusable as a missing one, so length is what
			// gets checked rather than nil-ness.
			name:     "an empty wrapped DEK",
			material: &ext.KeyMaterial{EncryptedDek: []byte{}},
			wantErrs: []validation.Error{
				{Field: "encrypted_dek", Message: "must not be empty"},
			},
		},
		{
			// opaque carries no meaning here, so it cannot stand in for the
			// wrapped DEK.
			name:     "opaque without a wrapped DEK",
			material: &ext.KeyMaterial{Opaque: []byte("mine")},
			wantErrs: []validation.Error{
				{Field: "encrypted_dek", Message: "must not be empty"},
			},
		},
		{
			// A nil message reports the same failure rather than panicking, so
			// a caller can validate whatever it was handed.
			name:     "no key material at all",
			material: nil,
			wantErrs: []validation.Error{
				{Field: "encrypted_dek", Message: "must not be empty"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.material.Validate()
			if len(tt.wantErrs) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)
			require.ElementsMatch(t, tt.wantErrs, []validation.Error(errs))
		})
	}
}
