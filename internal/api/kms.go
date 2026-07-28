package api

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/pkg/api/kms/v1"
)

type KMS struct {
	id  string
	kms kms.EncryptionServiceClient
}

func NewKMS(id string, cc grpc.ClientConnInterface) *KMS {
	return &KMS{
		id:  id,
		kms: kms.NewEncryptionServiceClient(cc),
	}
}

// Close is a no-op. The gRPC connection passed to NewKMS is owned by the caller
// that dialed it, which remains responsible for closing it; a KEKRegistry
// closing this KEK must not tear down a connection it does not own.
func (k *KMS) Close() error {
	return nil
}

// ID returns a unique ID for this KEK, e.g. a KMS ARN.
func (k *KMS) ID() string {
	return k.id
}

// Encrypt wraps a DEK via the remote provider, returning the ciphertext. The
// namespace is forwarded so the provider can select a per-namespace key.
func (k *KMS) Encrypt(ctx context.Context, ns string, pt []byte) ([]byte, error) {
	res, err := k.kms.Encrypt(ctx, &kms.EncryptRequest{
		Namespace: ns,
		Plaintext: pt,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt DEK, id: %s, err: %w", k.id, err)
	}

	return res.Ciphertext, nil
}

// Decrypt decrypts a DEK previously produced by Encrypt.
func (k *KMS) Decrypt(ctx context.Context, ct []byte) ([]byte, error) {
	res, err := k.kms.Decrypt(ctx, &kms.DecryptRequest{
		Ciphertext: ct,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt DEK, id: %s, err: %w", k.id, err)
	}

	return res.Plaintext, nil
}
