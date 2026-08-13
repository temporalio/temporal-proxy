package api_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/auth/outbound"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	authv1 "github.com/temporalio/temporal-proxy/pkg/api/auth/v1"
)

type (
	// fakeAuthConn is a grpc.ClientConnInterface that records the context of the
	// last unary call, letting us assert which metadata the Auth client puts on
	// the wire without running a real extension server.
	fakeAuthConn struct {
		gotCtx   context.Context
		gotReq   any
		invoke   error
		decision authv1.AuthResponse_Decision
		reason   string
	}

	// recordingAuthServer is a real AuthService implementation that records the
	// metadata each call arrives with, so a test can see what actually reached
	// the extension server rather than what the client meant to send.
	recordingAuthServer struct {
		authv1.UnimplementedAuthServiceServer

		mu    sync.Mutex
		md    metadata.MD
		creds map[string][]string
	}

	// proxyCredential stands in for the credential the proxy presents to an
	// extension server, so the test can exercise the real outbound.DialOptions
	// without the TLS the production provider demands.
	proxyCredential struct {
		header string
		value  string
	}
)

func TestAuthForwardsCallerMetadata(t *testing.T) {
	t.Parallel()

	conn := allowingConn()
	a := api.NewAuth(conn, nil)

	md := metadata.Pairs("authorization", "Bearer caller-token", "x-caller", "worker-1")
	require.NoError(t, a.Authenticate(t.Context(), meta.Target{}, md))

	// The request body is empty by design, so metadata is the only thing the
	// extension server has to authenticate. Without this the server is asked to
	// judge a caller it cannot see.
	got, ok := metadata.FromOutgoingContext(conn.gotCtx)
	require.True(t, ok, "expected the call to carry outgoing metadata")
	require.Equal(t, []string{"Bearer caller-token"}, got.Get("authorization"))
	require.Equal(t, []string{"worker-1"}, got.Get("x-caller"))
}

func TestAuthSendsDeclaredCredentialsInRequest(t *testing.T) {
	t.Parallel()

	conn := allowingConn()
	a := api.NewAuth(conn, []string{"authorization", "x-api-key"})

	// x-api-key repeats: gRPC allows it, so every value is carried rather than
	// the proxy picking one on the provider's behalf.
	require.NoError(t, a.Authenticate(t.Context(), meta.Target{}, metadata.Pairs(
		"authorization", "Bearer caller-token",
		"x-api-key", "key-1",
		"x-api-key", "key-2",
		"x-caller", "worker-1",
	)))

	require.Equal(t, map[string][]string{
		"authorization": {"Bearer caller-token"},
		"x-api-key":     {"key-1", "key-2"},
	}, conn.sentCredentials(t))
}

func TestAuthWithholdsDeclaredCredentialsFromForwardedMetadata(t *testing.T) {
	t.Parallel()

	conn := allowingConn()
	a := api.NewAuth(conn, []string{"authorization"})

	require.NoError(t, a.Authenticate(t.Context(), meta.Target{}, metadata.Pairs(
		"authorization", "Bearer caller-token",
		"x-caller", "worker-1",
	)))

	// A credential belongs in the request or nowhere. Left on the metadata it
	// would reach the server twice, and on whichever header the proxy uses for
	// its own credential the server could no longer tell the two apart.
	got, ok := metadata.FromOutgoingContext(conn.gotCtx)
	require.True(t, ok)
	require.Empty(t, got.Get("authorization"), "a declared credential header must not be forwarded")
	require.Equal(t, []string{"worker-1"}, got.Get("x-caller"), "other context must still be forwarded")
}

func TestAuthDoesNotMutateCallerMetadata(t *testing.T) {
	t.Parallel()

	conn := allowingConn()
	a := api.NewAuth(conn, []string{"authorization"})

	// The interceptor forwards this same map upstream after the verdict, so
	// withholding a header from the request must not strip it from the caller's.
	md := metadata.Pairs("authorization", "Bearer caller-token")
	require.NoError(t, a.Authenticate(t.Context(), meta.Target{}, md))

	require.Equal(t, []string{"Bearer caller-token"}, md.Get("authorization"))
}

func TestAuthWithholdsProviderReasonFromCaller(t *testing.T) {
	t.Parallel()

	// A provider's message is written for whoever runs it and can name internal
	// systems or subjects, so the caller gets a generic one while the reason goes
	// to the proxy's log.
	conn := &fakeAuthConn{invoke: status.Error(codes.PermissionDenied, "subject bob@corp not in group admins")}
	a := api.NewAuth(conn, []string{"authorization"})

	err := a.Authenticate(t.Context(), meta.Target{}, metadata.Pairs("authorization", "Bearer caller-token"))
	require.Error(t, err)

	st := status.Convert(err)
	require.Equal(t, codes.PermissionDenied, st.Code(), "the provider's code still reaches the caller")
	require.NotContains(t, st.Message(), "bob@corp", "the provider's reason must not reach the caller")
	require.Contains(t, err.Error(), "subject bob@corp not in group admins", "but it must reach the log")
}

func TestAuthSendsTarget(t *testing.T) {
	t.Parallel()

	// The provider decides on what the caller is addressing, not just on who it
	// is, which is what makes an authorization decision possible at all.
	conn := allowingConn()
	a := api.NewAuth(conn, nil)

	target := meta.Target{
		FullName:  "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
		Namespace: "orders",
	}
	require.NoError(t, a.Authenticate(t.Context(), target, metadata.MD{}))

	got := conn.sentTarget(t)
	require.Equal(t, target.FullName, got.GetFullName())
	require.Equal(t, target.Namespace, got.GetNamespace())
}

func TestAuthHonorsDecision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		decision authv1.AuthResponse_Decision
		wantCode codes.Code
	}{
		{
			name:     "allow admits the caller",
			decision: authv1.AuthResponse_DECISION_ALLOW,
			wantCode: codes.OK,
		},
		{
			// A denial arrives as a decision rather than an error status, and a
			// decision carries no code, so the proxy supplies one. The provider
			// judged the caller, which is PermissionDenied.
			name:     "deny rejects the caller",
			decision: authv1.AuthResponse_DECISION_DENY,
			wantCode: codes.PermissionDenied,
		},
		{
			// The zero value is not a verdict. A provider that answers without
			// setting one is broken, and a broken provider must not admit: this is
			// the whole reason the enum reserves zero. Internal rather than
			// PermissionDenied because the fault is the provider's, not the caller's.
			name:     "an unset decision denies rather than admits",
			decision: authv1.AuthResponse_DECISION_UNSPECIFIED,
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := api.NewAuth(&fakeAuthConn{decision: tt.decision}, nil)

			err := a.Authenticate(t.Context(), meta.Target{FullName: "/pkg.Svc/M"}, metadata.MD{})
			require.Equal(t, tt.wantCode, status.Code(err))
		})
	}
}

func TestAuthWithholdsDenialReasonFromCaller(t *testing.T) {
	t.Parallel()

	// Same contract a rejection by error status gets: the provider's reason is
	// written for whoever operates it and can name internal systems or subjects, so
	// it goes to the log while the caller is told something generic.
	conn := &fakeAuthConn{
		decision: authv1.AuthResponse_DECISION_DENY,
		reason:   "subject bob@corp not in group admins",
	}
	a := api.NewAuth(conn, nil)

	err := a.Authenticate(t.Context(), meta.Target{FullName: "/pkg.Svc/M"}, metadata.MD{})
	require.Error(t, err)
	require.NotContains(t, status.Convert(err).Message(), "bob@corp", "the reason must not reach the caller")
	require.Contains(t, err.Error(), "subject bob@corp not in group admins", "but it must reach the log")
}

func TestAuthDeniesWhenServerFails(t *testing.T) {
	t.Parallel()

	// Every error is a denial, including one that reports the provider's own
	// health rather than a verdict on the caller. The status must survive the
	// wrapping so the caller and the rejection log see Unavailable, not Unknown.
	conn := &fakeAuthConn{invoke: status.Error(codes.Unavailable, "backend down")}
	a := api.NewAuth(conn, nil)

	err := a.Authenticate(t.Context(), meta.Target{}, metadata.Pairs("authorization", "Bearer caller-token"))
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestAuthCanonicalizesDeclaredHeaders(t *testing.T) {
	t.Parallel()

	// gRPC lowercases metadata keys on the wire, so a mixed-case configured header
	// has to be canonicalized before it is matched against one or reported as a
	// header to strip. The built-in authenticators do this at construction too.
	conn := allowingConn()
	a := api.NewAuth(conn, []string{"Authorization", "X-API-Key"})

	require.Equal(t, []string{"authorization", "x-api-key"}, a.SecureHeaders())

	require.NoError(t, a.Authenticate(t.Context(), meta.Target{}, metadata.Pairs("authorization", "Bearer caller-token")))
	require.Equal(t, map[string][]string{"authorization": {"Bearer caller-token"}}, conn.sentCredentials(t))
}

func TestAuthCopiesDeclaredHeadersFromCaller(t *testing.T) {
	t.Parallel()

	// The strip list is a security control, so whoever supplied it must not be
	// able to shorten it after construction.
	hdrs := []string{"authorization"}
	a := api.NewAuth(&fakeAuthConn{}, hdrs)

	hdrs[0] = "x-not-a-credential"

	require.Equal(t, []string{"authorization"}, a.SecureHeaders())
}

func TestAuthSecureHeadersResistsMutation(t *testing.T) {
	t.Parallel()

	// The strip list decides which caller credentials stay off an upstream, so
	// handing out the backing array would let a caller shorten it in place.
	a := api.NewAuth(&fakeAuthConn{}, []string{"authorization"})

	got := a.SecureHeaders()
	got[0] = "x-not-a-credential"

	require.Equal(t, []string{"authorization"}, a.SecureHeaders())
}

// TestAuthCallerCredentialSurvivesExtensionCredentialOnSameHeader is the case the
// request field exists for. The proxy authenticates itself to the extension
// server on "authorization", which is also where the caller's credential arrives,
// and outbound.DialOptions strips that header from forwarded metadata so the two
// cannot collide. Carrying the caller's credential in the request puts it out of
// that strip's reach and tells the server whose value is whose.
//
// It takes a real connection: those interceptors come from dial options that a
// fake grpc.ClientConnInterface never runs, so a fake cannot show this working.
func TestAuthCallerCredentialSurvivesExtensionCredentialOnSameHeader(t *testing.T) {
	t.Parallel()

	srv := &recordingAuthServer{}
	conn := dialRecordingAuthServer(t, srv, &proxyCredential{
		header: "authorization",
		value:  "Bearer proxy-to-extension",
	})

	a := api.NewAuth(conn, []string{"authorization"})
	require.NoError(t, a.Authenticate(t.Context(), meta.Target{}, metadata.Pairs(
		"authorization", "Bearer caller-token",
		"x-caller", "worker-1",
	)))

	require.Equal(t, map[string][]string{"authorization": {"Bearer caller-token"}}, srv.credentials(),
		"the caller's credential must reach the server in the request")

	got := srv.metadata()
	require.Equal(t, []string{"Bearer proxy-to-extension"}, got.Get("authorization"),
		"the header carries only the proxy's own credential, so the server cannot confuse the two")
	require.Equal(t, []string{"worker-1"}, got.Get("x-caller"), "other context still arrives as metadata")
}

// TestAuthDeclaresNothingWhenNoCredentialHeadersConfigured covers the other half
// of the pair. With no headers declared the proxy cannot tell which metadata is a
// credential, so it lifts nothing and withholds nothing: the request carries no
// credentials and the server is left to treat the caller as unauthenticated.
func TestAuthDeclaresNothingWhenNoCredentialHeadersConfigured(t *testing.T) {
	t.Parallel()

	srv := &recordingAuthServer{}
	conn := dialRecordingAuthServer(t, srv, nil)

	a := api.NewAuth(conn, nil)
	require.NoError(t, a.Authenticate(t.Context(), meta.Target{}, metadata.Pairs(
		"authorization", "Bearer caller-token",
		"x-caller", "worker-1",
	)))

	require.Empty(t, srv.credentials(), "nothing declared means nothing vouched for")

	got := srv.metadata()
	require.Equal(t, []string{"worker-1"}, got.Get("x-caller"))
	require.Equal(t, []string{"Bearer caller-token"}, got.Get("authorization"))
}

func dialRecordingAuthServer(
	t *testing.T, srv authv1.AuthServiceServer, cp outbound.CredentialProvider,
) *grpc.ClientConn {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	gs := grpc.NewServer()
	authv1.RegisterAuthServiceServer(gs, srv)

	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	// A nil provider mirrors an extension server with no credentials of its own,
	// which is what extensionConn builds when its credentials block is absent.
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if cp != nil {
		opts = append(outbound.DialOptions(cp), opts...)
	}

	cc, err := grpc.NewClient(lis.Addr().String(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	return cc
}

func (s *recordingAuthServer) Auth(ctx context.Context, req *authv1.AuthRequest) (*authv1.AuthResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	creds := make(map[string][]string, len(req.GetCredentials()))
	for _, c := range req.GetCredentials() {
		creds[c.GetHeader()] = c.GetValues()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.md, s.creds = md, creds

	return &authv1.AuthResponse{Decision: authv1.AuthResponse_DECISION_ALLOW}, nil
}

func (s *recordingAuthServer) metadata() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.md
}

func (s *recordingAuthServer) credentials() map[string][]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.creds
}

func (c *proxyCredential) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{c.header: c.value}, nil
}

func (c *proxyCredential) Header() string { return c.header }

// RequireTransportSecurity reports false so the test can dial in the clear. The
// real StaticCredentialProvider requires TLS; that is orthogonal to which
// headers reach the server.
func (c *proxyCredential) RequireTransportSecurity() bool { return false }

func (f *fakeAuthConn) Invoke(ctx context.Context, _ string, args, reply any, _ ...grpc.CallOption) error {
	f.gotCtx = ctx
	f.gotReq = args

	if f.invoke != nil {
		return f.invoke
	}

	if resp, ok := reply.(*authv1.AuthResponse); ok {
		resp.Decision = f.decision
		resp.Reason = f.reason
	}

	return nil
}

// sentTarget returns the Target the recorded request carried.
func (f *fakeAuthConn) sentTarget(t *testing.T) *authv1.Target {
	t.Helper()

	req, ok := f.gotReq.(*authv1.AuthRequest)
	require.True(t, ok, "expected an *AuthRequest, got %T", f.gotReq)

	return req.GetTarget()
}

// sentCredentials returns the credentials the recorded request carried, flattened
// to a comparable map. Proto messages carry internal state that defeats
// reflect-based equality, so the assertions compare this instead.
func (f *fakeAuthConn) sentCredentials(t *testing.T) map[string][]string {
	t.Helper()

	req, ok := f.gotReq.(*authv1.AuthRequest)
	require.True(t, ok, "expected an *AuthRequest, got %T", f.gotReq)

	out := make(map[string][]string, len(req.GetCredentials()))
	for _, c := range req.GetCredentials() {
		out[c.GetHeader()] = c.GetValues()
	}

	return out
}

func (f *fakeAuthConn) NewStream(
	context.Context, *grpc.StreamDesc, string, ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, errors.New("streaming not supported")
}

// allowingConn is a provider that admits, which is what the tests covering
// credential and metadata plumbing need: the verdict is not what they are about.
func allowingConn() *fakeAuthConn {
	return &fakeAuthConn{decision: authv1.AuthResponse_DECISION_ALLOW}
}
