package ext_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	health "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/auth/v1"
	"github.com/temporalio/temporal-proxy/pkg/api/kms/v1"
	"github.com/temporalio/temporal-proxy/pkg/ext"
)

// stubAuth records what the service handed it and answers with a fixed response
// and error. Both are set at construction and never mutated, so they need no
// locking.
type stubAuth struct {
	resp *auth.AuthResponse
	err  error

	mu    sync.Mutex
	calls int
	req   *auth.AuthRequest
	md    metadata.MD
}

func TestAllow(t *testing.T) {
	t.Parallel()

	resp := ext.Allow()
	require.Equal(t, auth.AuthResponse_DECISION_ALLOW, resp.GetDecision())
	require.Empty(t, resp.GetReason())
}

func TestDeny(t *testing.T) {
	t.Parallel()

	resp := ext.Deny("holds reader, needs writer")
	require.Equal(t, auth.AuthResponse_DECISION_DENY, resp.GetDecision())
	require.Equal(t, "holds reader, needs writer", resp.GetReason())
}

// TestDenyWithoutReason pins that an absent reason still denies: the decision
// carries the verdict, so a caller is refused whether or not anyone said why.
func TestDenyWithoutReason(t *testing.T) {
	t.Parallel()

	require.Equal(t, auth.AuthResponse_DECISION_DENY, ext.Deny("").GetDecision())
}

func TestBearerToken(t *testing.T) {
	t.Parallel()

	creds := func(header string, values ...string) []*auth.Credential {
		return []*auth.Credential{{Header: header, Values: values}}
	}

	tests := []struct {
		name  string
		req   *auth.AuthRequest
		hdr   string
		want  string
		error string
	}{
		{
			name: "returns the token",
			req:  &auth.AuthRequest{Credentials: creds("authorization", "Bearer abc.def")},
			hdr:  "authorization",
			want: "abc.def",
		},
		{
			name: "matches the header case-insensitively",
			req:  &auth.AuthRequest{Credentials: creds("authorization", "Bearer abc")},
			hdr:  "Authorization",
			want: "abc",
		},
		{
			name: "matches the scheme case-insensitively",
			req:  &auth.AuthRequest{Credentials: creds("authorization", "bearer abc")},
			hdr:  "authorization",
			want: "abc",
		},
		{
			name: "keeps whitespace inside the token",
			req:  &auth.AuthRequest{Credentials: creds("authorization", "Bearer  abc")},
			hdr:  "authorization",
			want: " abc",
		},
		{
			name: "finds the header among others",
			req: &auth.AuthRequest{Credentials: []*auth.Credential{
				{Header: "x-api-key", Values: []string{"opaque"}},
				{Header: "authorization", Values: []string{"Bearer abc"}},
			}},
			hdr:  "authorization",
			want: "abc",
		},
		{
			name:  "rejects a request with no credentials",
			req:   &auth.AuthRequest{},
			hdr:   "authorization",
			error: "no credential presented on authorization",
		},
		{
			name:  "rejects a nil request",
			req:   nil,
			hdr:   "authorization",
			error: "no credential presented on authorization",
		},
		{
			name:  "rejects a different header",
			req:   &auth.AuthRequest{Credentials: creds("x-api-key", "Bearer abc")},
			hdr:   "authorization",
			error: "no credential presented on authorization",
		},
		{
			name:  "rejects a repeated value rather than choosing one",
			req:   &auth.AuthRequest{Credentials: creds("authorization", "Bearer abc", "Bearer xyz")},
			hdr:   "authorization",
			error: "authorization carries 2 values, want exactly one",
		},
		{
			name:  "rejects a header with no values",
			req:   &auth.AuthRequest{Credentials: creds("authorization")},
			hdr:   "authorization",
			error: "authorization carries 0 values, want exactly one",
		},
		{
			name:  "rejects another scheme",
			req:   &auth.AuthRequest{Credentials: creds("authorization", "Basic abc")},
			hdr:   "authorization",
			error: "authorization is not a Bearer credential",
		},
		{
			name:  "rejects a value shorter than the scheme",
			req:   &auth.AuthRequest{Credentials: creds("authorization", "Bear")},
			hdr:   "authorization",
			error: "authorization is not a Bearer credential",
		},
		{
			name:  "rejects a bare token with no scheme",
			req:   &auth.AuthRequest{Credentials: creds("authorization", "abc.def")},
			hdr:   "authorization",
			error: "authorization is not a Bearer credential",
		},
		{
			name:  "rejects the scheme with no token behind it",
			req:   &auth.AuthRequest{Credentials: creds("authorization", "Bearer ")},
			hdr:   "authorization",
			error: "authorization carries no token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ext.BearerToken(tt.req, tt.hdr)
			if tt.error != "" {
				require.Empty(t, got)
				require.Equal(t, codes.Unauthenticated, status.Code(err))
				require.Equal(t, tt.error, status.Convert(err).Message())

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsHealthCheckMethod(t *testing.T) {
	t.Parallel()

	const workflowService = "/temporal.api.workflowservice.v1.WorkflowService/"

	tests := []struct {
		name string
		full string
		want bool
	}{
		{name: "health check", full: health.Health_Check_FullMethodName, want: true},
		{name: "health watch", full: health.Health_Watch_FullMethodName, want: true},
		{name: "get system info", full: workflowService + "GetSystemInfo", want: true},
		// GetClusterInfo reports on the service too, but nothing needs it to connect,
		// and Temporal's own authorizer charges it as a cluster-scoped read.
		{name: "get cluster info", full: workflowService + "GetClusterInfo", want: false},
		{name: "start workflow", full: workflowService + "StartWorkflowExecution", want: false},
		{name: "poll", full: workflowService + "PollWorkflowTaskQueue", want: false},
		{name: "empty", full: "", want: false},
		// Matched exactly: a method name is not caller-supplied, so there is nothing
		// to normalize and a near miss is a bug rather than a spelling to accept.
		{name: "wrong case", full: workflowService + "getsysteminfo", want: false},
		{name: "no leading slash", full: "temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, ext.IsHealthCheckMethod(tt.full))
		})
	}
}

func TestAuthServiceDelegates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resp     *auth.AuthResponse
		authErr  error
		wantCode codes.Code
	}{
		{
			name:     "an allow decision admits the caller",
			resp:     &auth.AuthResponse{Decision: auth.AuthResponse_DECISION_ALLOW},
			wantCode: codes.OK,
		},
		{
			// A denial is an ordinary answer, not a failure: it travels as a
			// response so the reason can travel with it.
			name: "a deny decision is not an error",
			resp: &auth.AuthResponse{
				Decision: auth.AuthResponse_DECISION_DENY,
				Reason:   "not in group admins",
			},
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

			stub := &stubAuth{resp: tt.resp, err: tt.authErr}
			cc := dial(t, serve(t, ext.WithAuth(stub)), nil)

			res, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
			require.Equal(t, tt.wantCode, status.Code(err))

			if tt.wantCode == codes.OK {
				require.NotNil(t, res, "an admitted caller still gets a response")
			}
		})
	}
}

func TestAuthServiceForwardsTarget(t *testing.T) {
	t.Parallel()

	// The implementation is handed the request whole, so what the caller is
	// addressing arrives alongside who it is. This is also why the interface takes
	// the message: a field added to it reaches implementations without a break.
	stub := allowingStubAuth()
	cc := dial(t, serve(t, ext.WithAuth(stub)), nil)

	_, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{
		Target: &auth.Target{
			FullName:  "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
			Namespace: "orders",
		},
	})
	require.NoError(t, err)

	got := stub.request().GetTarget()
	require.Equal(
		t,
		"/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
		got.GetFullName(),
	)
	require.Equal(t, "orders", got.GetNamespace())
}

func TestAuthServiceReportsMissingAnswer(t *testing.T) {
	t.Parallel()

	// An implementation that returns neither a response nor an error has not
	// decided anything. Saying so is better than sending an empty message the
	// proxy would read as an absent verdict and blame on the wire.
	stub := &stubAuth{}
	cc := dial(t, serve(t, ext.WithAuth(stub)), nil)

	_, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestAuthServiceForwardsCredentialsAndMetadata(t *testing.T) {
	t.Parallel()

	stub := allowingStubAuth()
	cc := dial(t, serve(t, ext.WithAuth(stub)), nil)

	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-caller", "worker-1"))
	_, err := auth.NewAuthServiceClient(cc).Auth(ctx, &auth.AuthRequest{
		Credentials: []*auth.Credential{
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

	stub := allowingStubAuth()
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
		ext.WithAuth(allowingStubAuth()),
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
		ext.WithAuth(allowingStubAuth()),
		ext.WithServerAuth(extHeader, nil),
	), nil)

	_, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
	require.NoError(t, err, "a nil check admits everyone")
}

func (s *stubAuth) Authenticate(ctx context.Context, req *auth.AuthRequest) (*auth.AuthResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	s.req, s.md = req, md

	return s.resp, s.err
}

func (s *stubAuth) request() *auth.AuthRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.req
}

func (s *stubAuth) called() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls > 0
}

func (s *stubAuth) credentials() []*auth.Credential {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.req.GetCredentials()
}

func (s *stubAuth) metadata() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.md
}

// allowingStubAuth admits every caller, for the tests where the verdict is not
// what is under test.
func allowingStubAuth() *stubAuth {
	return &stubAuth{resp: &auth.AuthResponse{Decision: auth.AuthResponse_DECISION_ALLOW}}
}
