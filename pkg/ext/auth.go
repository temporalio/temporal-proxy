package ext

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/auth/v1"
)

type (
	// authService adapts an [Auth] to the generated service. The embedded
	// Unimplemented server is what lets a nil Auth answer Unimplemented.
	authService struct {
		auth.UnimplementedAuthServiceServer
		auth Auth
	}

	// Auth decides whether an inbound caller of the proxy may proceed. Register an
	// implementation with [WithAuth].
	//
	// The request carries what the proxy knows about the call. Its credentials are
	// what the proxy lifted from the caller's stream, one entry per configured
	// credential header the caller actually sent, so an empty slice means it
	// presented none; its target is what the call is addressing. The proxy's own
	// credential to this server is not among the credentials and stays in the
	// request metadata, alongside the caller's other metadata.
	//
	// Answer with a response whose Decision is set: only DECISION_ALLOW admits, so
	// an unset decision denies rather than admits by accident. Reason is for
	// whoever operates this server, and the proxy keeps it out of what the rejected
	// caller is told. Return an error only when no verdict was reached, such as an
	// unreachable backend; the proxy denies either way, but an error keeps its
	// status code, so [google.golang.org/grpc/codes.Unavailable] tells a worker to
	// retry where a denial does not.
	//
	// Implementations must be safe for concurrent use and must not block
	// indefinitely, since a caller is waiting and the proxy denies on timeout.
	Auth interface {
		Authenticate(context.Context, *auth.AuthRequest) (*auth.AuthResponse, error)
	}

	// CredentialCheck reports whether a credential presented to this server is
	// valid. [WithServerAuth] installs it and describes the checks made first.
	CredentialCheck func(string) bool
)

// Auth answers api.auth.v1.AuthService via the configured [Auth], which owns the
// verdict: the response carries the decision and the error is reserved for having
// reached none.
func (a *authService) Auth(ctx context.Context, req *auth.AuthRequest) (*auth.AuthResponse, error) {
	if a.auth == nil {
		return a.UnimplementedAuthServiceServer.Auth(ctx, req)
	}

	resp, err := a.auth.Authenticate(ctx, req)
	if err != nil {
		return nil, err
	}

	// Neither a response nor an error is not a verdict. Reporting it here names the
	// implementation as the problem, rather than sending an empty message the proxy
	// would read as an absent decision and attribute to the wire.
	if resp == nil {
		return nil, status.Error(codes.Internal, "auth: implementation returned no response and no error")
	}

	return resp, nil
}

// unaryGuard returns the interceptor [WithServerAuth] installs, which documents
// the contract. [Serve] installs it only for a non-nil check. Rejections are
// Unauthenticated, and the message separates an absent credential from a rejected
// one: both ends here are operator-run, so telling "wrong header" from "wrong
// value" is worth more than withholding it.
func unaryGuard(hdr string, check CredentialCheck) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		got := md.Get(hdr)
		if len(got) != 1 {
			return nil, status.Error(codes.Unauthenticated, "missing credentials")
		}

		if !check(got[0]) {
			return nil, status.Error(codes.Unauthenticated, "invalid credentials")
		}

		return handler(ctx, req)
	}
}
