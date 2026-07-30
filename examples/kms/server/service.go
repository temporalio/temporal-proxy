package main

import (
	"context"
	"log"

	"github.com/temporalio/temporal-proxy/pkg/api/kms/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// service implements api.kms.v1.EncryptionService over a keyring. Embedding the
// generated unimplemented server is required by the generated interface and
// keeps the type compiling when the service gains methods.
type service struct {
	kms.UnimplementedEncryptionServiceServer

	keys *keyring
}

// newService returns a service that wraps DEKs with keys.
func newService(keys *keyring) *service {
	return &service{keys: keys}
}

// Encrypt wraps the DEK in the request under the requested namespace's key.
//
// An absent namespace is refused rather than defaulted: the namespace selects
// the key, so accepting an empty one would quietly file every tenant's DEKs
// under the same key.
func (s *service) Encrypt(_ context.Context, req *kms.EncryptRequest) (*kms.EncryptResponse, error) {
	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace is required")
	}

	ct, err := s.keys.wrap(req.Namespace, req.Plaintext)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to wrap key material: %v", err)
	}

	log.Printf("wrapped a DEK: namespace=%s plaintext=%dB ciphertext=%dB", req.Namespace, len(req.Plaintext), len(ct))

	return &kms.EncryptResponse{Ciphertext: ct}, nil
}

// Decrypt unwraps key material previously produced by Encrypt. The request
// carries no namespace, which is why wrap puts one inside the ciphertext.
func (s *service) Decrypt(_ context.Context, req *kms.DecryptRequest) (*kms.DecryptResponse, error) {
	pt, err := s.keys.unwrap(req.Ciphertext)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to unwrap key material: %v", err)
	}

	log.Printf("unwrapped a DEK: ciphertext=%dB plaintext=%dB", len(req.Ciphertext), len(pt))

	return &kms.DecryptResponse{Plaintext: pt}, nil
}
