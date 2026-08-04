package ext_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/auth/v1"
	"github.com/temporalio/temporal-proxy/pkg/api/kms/v1"
	"github.com/temporalio/temporal-proxy/pkg/ext"
)

// stubAuth records what the service handed it and answers with a fixed error.
// err is set at construction and never mutated, so it needs no locking.
type stubAuth struct {
	err error

	mu    sync.Mutex
	calls int
	creds []*auth.CallerCredential
	md    metadata.MD
}

func TestAuthServiceDelegates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		authErr  error
		wantCode codes.Code
	}{
		{
			name:     "nil admits the caller",
			authErr:  nil,
			wantCode: codes.OK,
		},
		{
			name:     "a status error keeps its code",
			authErr:  status.Error(codes.PermissionDenied, "not for you"),
			wantCode: codes.PermissionDenied,
		},
		{
			name:     "unauthenticated survives the trip",
			authErr:  status.Error(codes.Unauthenticated, "bad token"),
			wantCode: codes.Unauthenticated,
		},
		{
			// The reason the godoc asks for a status error: gRPC has nowhere to put a
			// plain error's code, so the proxy is told nothing about whether the
			// caller can fix this.
			name:     "a plain error degrades to Unknown",
			authErr:  errors.New("backend unreachable"),
			wantCode: codes.Unknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := &stubAuth{err: tt.authErr}
			cc := dial(t, serve(t, ext.WithAuth(stub)), nil)

			res, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
			require.Equal(t, tt.wantCode, status.Code(err))

			if tt.wantCode == codes.OK {
				require.NotNil(t, res, "an admitted caller still gets a response")
			}
		})
	}
}

func TestAuthServiceForwardsCredentialsAndMetadata(t *testing.T) {
	t.Parallel()

	stub := &stubAuth{}
	cc := dial(t, serve(t, ext.WithAuth(stub)), nil)

	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-caller", "worker-1"))
	_, err := auth.NewAuthServiceClient(cc).Auth(ctx, &auth.AuthRequest{
		Credentials: []*auth.CallerCredential{
			{Header: "authorization", Values: []string{"Bearer caller-token"}},
			{Header: "x-api-key", Values: []string{"one", "two"}},
		},
	})
	require.NoError(t, err)

	// The request carries the caller's credentials, verbatim and in order, so an
	// implementation can tell which header vouched for which value.
	creds := stub.credentials()
	require.Len(t, creds, 2)
	require.Equal(t, "authorization", creds[0].GetHeader())
	require.Equal(t, []string{"Bearer caller-token"}, creds[0].GetValues())
	require.Equal(t, "x-api-key", creds[1].GetHeader())
	require.Equal(t, []string{"one", "two"}, creds[1].GetValues())

	// Metadata reaches the handler alongside them, which is what lets an
	// implementation weigh context the credentials do not carry.
	require.Equal(t, []string{"worker-1"}, stub.metadata().Get("x-caller"))
}

func TestAuthServiceReceivesNoCredentials(t *testing.T) {
	t.Parallel()

	stub := &stubAuth{}
	cc := dial(t, serve(t, ext.WithAuth(stub)), nil)

	// A caller that presented none of the declared headers. The handler is still
	// consulted rather than short-circuited, because whether that is a denial is
	// the implementation's call to make.
	_, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
	require.NoError(t, err)

	require.True(t, stub.called())
	require.Empty(t, stub.credentials())
}

func TestServerAuth(t *testing.T) {
	t.Parallel()

	// One server for the whole table: the guard is a property of the server, and
	// what varies is what the caller presents.
	addr := serve(t,
		ext.WithAuth(&stubAuth{}),
		ext.WithServerAuth(extHeader, acceptExtToken),
	)
	cc := dial(t, addr, nil)

	tests := []struct {
		name     string
		md       metadata.MD
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "the expected credential is admitted",
			md:       metadata.Pairs(extHeader, extToken),
			wantCode: codes.OK,
		},
		{
			name:     "an absent header is refused",
			md:       metadata.MD{},
			wantCode: codes.Unauthenticated,
			wantMsg:  "missing credentials",
		},
		{
			name:     "the wrong value is refused",
			md:       metadata.Pairs(extHeader, "guess"),
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid credentials",
		},
		{
			// Refused rather than searched. Accepting any one of several offered
			// values would let a caller spray guesses in a single call, and it reads
			// as "missing" because a header sent twice is not a credential.
			name:     "a repeated header is refused even when one value is right",
			md:       metadata.Pairs(extHeader, extToken, extHeader, "guess"),
			wantCode: codes.Unauthenticated,
			wantMsg:  "missing credentials",
		},
		{
			name:     "an empty value is refused",
			md:       metadata.Pairs(extHeader, ""),
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid credentials",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := metadata.NewOutgoingContext(t.Context(), tt.md)
			_, err := auth.NewAuthServiceClient(cc).Auth(ctx, &auth.AuthRequest{})

			require.Equal(t, tt.wantCode, status.Code(err))
			if tt.wantMsg != "" {
				require.Equal(t, tt.wantMsg, status.Convert(err).Message())
			}
		})
	}
}

func TestServerAuthGuardsEveryService(t *testing.T) {
	t.Parallel()

	stub := &stubAuth{}
	addr := serve(t,
		ext.WithAuth(stub),
		ext.WithKMS(&stubKMS{ciphertext: []byte("wrapped")}),
		ext.WithServerAuth(extHeader, acceptExtToken),
	)
	cc := dial(t, addr, nil)

	// Not just the auth service: an unauthenticated caller cannot reach the KMS
	// service either, which is the one that would otherwise unwrap key material for
	// anyone who found the port.
	_, err := kms.NewEncryptionServiceClient(cc).Decrypt(t.Context(), &kms.DecryptRequest{
		Ciphertext: []byte("ct"),
	})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	// Rejected in the interceptor, so the handler never ran.
	require.False(t, stub.called(), "the guard runs before the service")

	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(extHeader, extToken))
	_, err = kms.NewEncryptionServiceClient(cc).Encrypt(ctx, &kms.EncryptRequest{
		Namespace: "ns",
		Plaintext: []byte("dek"),
	})
	require.NoError(t, err, "the same credential admits every service")
}

func TestServerAuthNilCheckLeavesServerOpen(t *testing.T) {
	t.Parallel()

	// Documented behaviour rather than desirable behaviour: the option reads as
	// configured at the call site, so a nil fn is worth pinning down.
	cc := dial(t, serve(t,
		ext.WithAuth(&stubAuth{}),
		ext.WithServerAuth(extHeader, nil),
	), nil)

	_, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
	require.NoError(t, err, "a nil check admits everyone")
}

func (s *stubAuth) Authenticate(ctx context.Context, creds []*auth.CallerCredential) error {
	md, _ := metadata.FromIncomingContext(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	s.creds, s.md = creds, md

	return s.err
}

func (s *stubAuth) called() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls > 0
}

func (s *stubAuth) credentials() []*auth.CallerCredential {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.creds
}

func (s *stubAuth) metadata() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.md
}
