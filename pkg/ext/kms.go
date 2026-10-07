package ext

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/kms/v1"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

type (
	// KMS wraps and unwraps the proxy's data encryption keys. Register an
	// implementation with [WithKMS].
	//
	// The plaintext handed to Wrap is a DEK, never a payload; the proxy caches
	// the DEK and does the bulk encryption itself, so each call may be a round
	// trip to an HSM.
	//
	// Wrap receives the namespace, so an implementation may hold a distinct key per
	// namespace. Unwrap gets only the ciphertext, so whatever identifies the key
	// has to be inside what Wrap returned, usually an opaque header framed around
	// it. That makes Unwrap's input a durable format worth versioning, and retiring
	// a key destroys every payload it wrapped.
	//
	// A [google.golang.org/grpc/status] error is passed through with its code
	// intact; any other error takes the code documented on the method that called
	// it. Implementations must be safe for concurrent use.
	KMS interface {
		Wrap(context.Context, string, []byte) ([]byte, error)
		Unwrap(context.Context, []byte) ([]byte, error)
	}

	// kmsService adapts a [KMS] to the generated service. As with [authService], the
	// embedded Unimplemented server is what lets a nil KMS answer Unimplemented.
	kmsService struct {
		kms.UnimplementedEncryptionServiceServer
		kms KMS
		log logger.Logger
	}
)

// Encrypt wraps a DEK for the namespace named in the request. An empty namespace
// is refused with InvalidArgument rather than defaulted, since it selects the
// key. A failure from the implementation is Internal unless the implementation
// chose a code of its own.
func (s *kmsService) Encrypt(ctx context.Context, req *kms.EncryptRequest) (*kms.EncryptResponse, error) {
	if s.kms == nil {
		return s.UnimplementedEncryptionServiceServer.Encrypt(ctx, req)
	}

	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace is required")
	}

	ct, err := s.kms.Wrap(ctx, req.Namespace, req.Plaintext)
	if err != nil {
		return nil, implError(err, codes.Internal, "failed to wrap key material")
	}

	s.log.Info("Wrapped a DEK")
	return &kms.EncryptResponse{Ciphertext: ct}, nil
}

// Decrypt unwraps a DEK previously produced by Encrypt. A failure from the
// implementation is InvalidArgument, marking the ciphertext as the likely cause,
// unless the implementation chose a code of its own, such as Unavailable for an
// unreachable backend.
func (s *kmsService) Decrypt(ctx context.Context, req *kms.DecryptRequest) (*kms.DecryptResponse, error) {
	if s.kms == nil {
		return s.UnimplementedEncryptionServiceServer.Decrypt(ctx, req)
	}

	pt, err := s.kms.Unwrap(ctx, req.Ciphertext)
	if err != nil {
		return nil, implError(err, codes.InvalidArgument, "failed to unwrap key material")
	}

	s.log.Info("Unwrapped DEK")
	return &kms.DecryptResponse{Plaintext: pt}, nil
}

// implError reports err from a [KMS] implementation, keeping its code when it
// picked one and falling back to code otherwise. Both handlers route through
// here so the two cannot drift. A status carrying codes.OK, which a hand-rolled
// GRPCStatus can produce, falls back as well: gRPC would write it as a success
// without a response.
func implError(err error, code codes.Code, msg string) error {
	if s, ok := status.FromError(err); ok && s.Code() != codes.OK {
		return err
	}

	return status.Errorf(code, "%s: %v", msg, err)
}
