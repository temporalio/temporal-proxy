package ext

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

// UnmarshalKeyMaterial decodes key material produced by [KeyMaterial.Marshal].
// It does not check what it decoded: proto3 happily accepts bytes that set none
// of the fields, so call [KeyMaterial.Validate] on anything whose framing you
// did not produce yourself.
func UnmarshalKeyMaterial(raw []byte) (*KeyMaterial, error) {
	km := &KeyMaterial{}
	if err := proto.Unmarshal(raw, km); err != nil {
		return nil, fmt.Errorf("failed to unmarshal key material: %w", err)
	}

	return km, nil
}

// Marshal validates km and returns its wire encoding, ready to hand back as an
// EncryptResponse ciphertext.
func (km *KeyMaterial) Marshal() ([]byte, error) {
	if err := km.Validate(); err != nil {
		return nil, err
	}

	packed, err := proto.Marshal(km)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal key material: %w", err)
	}

	return packed, nil
}

// Validate reports whether km carries a wrapped DEK, treating a nil km as one
// that does not. Every other field is optional: an extension server may version
// no keys, key off no namespace, and carry nothing of its own.
func (km *KeyMaterial) Validate() error {
	return validation.Validate(
		"",
		validation.Field("encrypted_dek", km.GetEncryptedDek(), func(v []byte) error {
			if len(v) == 0 {
				return errors.New("must not be empty")
			}

			return nil
		}),
	)
}
