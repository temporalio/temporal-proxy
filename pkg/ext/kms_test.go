package ext_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/kms/v1"
	"github.com/temporalio/temporal-proxy/pkg/ext"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

type (
	// stubKMS answers with fixed values and records what it was given. The response
	// and error fields are set at construction and never mutated.
	stubKMS struct {
		ciphertext []byte
		plaintext  []byte
		wrapErr    error
		unwrapErr  error

		mu       sync.Mutex
		wraps    int
		ns       string
		wrapPT   []byte
		unwrapCT []byte
	}

	// okStatus is a non-nil error reporting a gRPC status of OK. status.Error
	// cannot build one, since it returns nil for OK, so reaching this state takes a
	// hand-rolled GRPCStatus. It exists to prove the service refuses to forward it.
	okStatus struct{}
)

func TestEncrypt(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	stub := &stubKMS{ciphertext: []byte("wrapped")}
	cc := dial(t, serve(t, ext.WithKMS(stub), ext.WithLogger(log)), nil)

	res, err := kms.NewEncryptionServiceClient(cc).Encrypt(t.Context(), &kms.EncryptRequest{
		Namespace: "orders",
		Plaintext: []byte("dek"),
	})
	require.NoError(t, err)
	require.Equal(t, []byte("wrapped"), res.GetCiphertext())

	// The namespace reaches Wrap, which is what lets an implementation hold a
	// distinct key per namespace.
	ns, pt := stub.wrapped()
	require.Equal(t, "orders", ns)
	require.Equal(t, []byte("dek"), pt)

	require.True(t, log.Contains("Wrapped a DEK"), "the configured logger is the one the service uses")
}

func TestEncryptRequiresNamespace(t *testing.T) {
	t.Parallel()

	stub := &stubKMS{ciphertext: []byte("wrapped")}
	cc := dial(t, serve(t, ext.WithKMS(stub)), nil)

	_, err := kms.NewEncryptionServiceClient(cc).Encrypt(t.Context(), &kms.EncryptRequest{
		Plaintext: []byte("dek"),
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "namespace is required", status.Convert(err).Message())

	// Refused before the implementation saw it. An implementation keyed by
	// namespace would otherwise wrap under whatever its zero value selects, and
	// the result would be unrecoverable once the mistake was found.
	require.False(t, stub.wrapCalled(), "rejected before reaching the implementation")
}

func TestEncryptReportsWrapFailureAsInternal(t *testing.T) {
	t.Parallel()

	cc := dial(t, serve(t, ext.WithKMS(&stubKMS{wrapErr: errors.New("hsm unreachable")})), nil)

	_, err := kms.NewEncryptionServiceClient(cc).Encrypt(t.Context(), &kms.EncryptRequest{
		Namespace: "orders",
		Plaintext: []byte("dek"),
	})

	// Internal, because the fault is this server's rather than the request's.
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "hsm unreachable",
		"both ends are operator-run, so the cause travels")
}

func TestEncryptPreservesImplementationStatus(t *testing.T) {
	t.Parallel()

	implErr := status.Error(codes.Unavailable, "hsm is down")
	cc := dial(t, serve(t, ext.WithKMS(&stubKMS{wrapErr: implErr})), nil)

	_, err := kms.NewEncryptionServiceClient(cc).Encrypt(t.Context(), &kms.EncryptRequest{
		Namespace: "orders",
		Plaintext: []byte("dek"),
	})

	// The implementation picked a code, so it keeps it. Internal is only the
	// fallback for an implementation that did not say.
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, "hsm is down", status.Convert(err).Message())
}

func TestDecrypt(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	stub := &stubKMS{plaintext: []byte("dek")}
	cc := dial(t, serve(t, ext.WithKMS(stub), ext.WithLogger(log)), nil)

	res, err := kms.NewEncryptionServiceClient(cc).Decrypt(t.Context(), &kms.DecryptRequest{
		Ciphertext: []byte("wrapped"),
	})
	require.NoError(t, err)
	require.Equal(t, []byte("dek"), res.GetPlaintext())

	// Unwrap is handed the ciphertext and nothing else, which is why whatever
	// identifies the wrapping key has to be inside it.
	require.Equal(t, []byte("wrapped"), stub.unwrapped())

	require.True(t, log.Contains("Unwrapped DEK"))
}

func TestDecryptReportsUnwrapFailureAsInvalidArgument(t *testing.T) {
	t.Parallel()

	cc := dial(t, serve(t, ext.WithKMS(&stubKMS{unwrapErr: errors.New("unknown key id")})), nil)

	_, err := kms.NewEncryptionServiceClient(cc).Decrypt(t.Context(), &kms.DecryptRequest{
		Ciphertext: []byte("wrapped elsewhere"),
	})

	// InvalidArgument rather than Internal: the likely cause is the ciphertext
	// itself, and none of the ways that goes wrong improve on a retry.
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "unknown key id")
}

func TestDecryptPreservesImplementationStatus(t *testing.T) {
	t.Parallel()

	implErr := status.Error(codes.Unavailable, "hsm is down")
	cc := dial(t, serve(t, ext.WithKMS(&stubKMS{unwrapErr: implErr})), nil)

	_, err := kms.NewEncryptionServiceClient(cc).Decrypt(t.Context(), &kms.DecryptRequest{
		Ciphertext: []byte("wrapped"),
	})

	// This is the distinction InvalidArgument cannot make on its own: the
	// ciphertext may be perfectly good and the backend simply absent, which is
	// worth retrying where a bad ciphertext is not.
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, "hsm is down", status.Convert(err).Message())
}

func TestKMSRefusesToForwardStatusOK(t *testing.T) {
	t.Parallel()

	cc := dial(t, serve(t, ext.WithKMS(&stubKMS{wrapErr: okStatus{}})), nil)

	_, err := kms.NewEncryptionServiceClient(cc).Encrypt(t.Context(), &kms.EncryptRequest{
		Namespace: "orders",
		Plaintext: []byte("dek"),
	})

	// Forwarding this one reaches the proxy as a call that succeeded without a
	// response, and the cardinality violation it reports says nothing about what
	// went wrong. The fallback code keeps the implementation's message.
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "not really fine")
}

func TestDecryptAcceptsEmptyCiphertext(t *testing.T) {
	t.Parallel()

	stub := &stubKMS{plaintext: []byte("dek")}
	cc := dial(t, serve(t, ext.WithKMS(stub)), nil)

	// Unlike Encrypt's namespace, nothing is validated here. The ciphertext's
	// format belongs to the implementation, so the service is in no position to
	// judge it and does not try.
	_, err := kms.NewEncryptionServiceClient(cc).Decrypt(t.Context(), &kms.DecryptRequest{})
	require.NoError(t, err)
	require.Empty(t, stub.unwrapped())
}

func (okStatus) Error() string { return "not really fine" }

func (okStatus) GRPCStatus() *status.Status { return status.New(codes.OK, "not really fine") }

func (s *stubKMS) Wrap(_ context.Context, ns string, pt []byte) ([]byte, error) {
	s.mu.Lock()
	s.wraps++
	s.ns, s.wrapPT = ns, pt
	s.mu.Unlock()

	if s.wrapErr != nil {
		return nil, s.wrapErr
	}

	return s.ciphertext, nil
}

func (s *stubKMS) Unwrap(_ context.Context, ct []byte) ([]byte, error) {
	s.mu.Lock()
	s.unwrapCT = ct
	s.mu.Unlock()

	if s.unwrapErr != nil {
		return nil, s.unwrapErr
	}

	return s.plaintext, nil
}

func (s *stubKMS) wrapCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.wraps > 0
}

func (s *stubKMS) wrapped() (string, []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ns, s.wrapPT
}

func (s *stubKMS) unwrapped() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.unwrapCT
}
