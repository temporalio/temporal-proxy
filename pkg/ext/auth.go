package ext

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/auth/v1"
)

// healthPrefix matches every method of the gRPC health service, which
// [unaryGuard] lets through. Taken from the service descriptor rather than
// spelled out, so it cannot drift from what was registered.
var healthPrefix = "/" + grpc_health_v1.Health_ServiceDesc.ServiceName + "/"

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
	// The credentials are what the proxy lifted from the caller's stream, one entry
	// per configured credential header the caller actually sent, so an empty slice
	// means it presented none. The proxy's own credential to this server is not
	// among them and stays in the request metadata, alongside the caller's other
	// metadata.
	//
	// Return nil to admit and an error to deny, and make it a
	// [google.golang.org/grpc/status] error: the proxy keeps the code and discards
	// the message, which it assumes was written for whoever operates this server.
	// Implementations must be safe for concurrent use and must not block
	// indefinitely, since a caller is waiting and the proxy denies on timeout.
	Auth interface {
		Authenticate(context.Context, []*auth.CallerCredential) error
	}

	// CredentialCheck reports whether a credential presented to this server is
	// valid. [WithServerAuth] installs it and describes the checks made first.
	CredentialCheck func(string) bool
)

// Auth answers api.auth.v1.AuthService via the configured [Auth]. The verdict is
// carried entirely by the error; the response is empty.
func (a *authService) Auth(ctx context.Context, req *auth.AuthRequest) (*auth.AuthResponse, error) {
	if a.auth == nil {
		return a.UnimplementedAuthServiceServer.Auth(ctx, req)
	}

	if err := a.auth.Authenticate(ctx, req.Credentials); err != nil {
		return nil, err
	}

	return &auth.AuthResponse{}, nil
}

// unaryGuard returns the interceptor [WithServerAuth] installs, which documents
// the contract. [Serve] installs it only for a non-nil check. Rejections are
// Unauthenticated, and the message separates an absent credential from a rejected
// one: both ends here are operator-run, so telling "wrong header" from "wrong
// value" is worth more than withholding it.
//
// The health service is exempt. Guarding it would break every probe, which has no
// credential to present and in Kubernetes' native gRPC prober cannot send
// metadata at all, and would withhold nothing in exchange: Watch reports the same
// status over a stream, which this interceptor does not see.
func unaryGuard(hdr string, check CredentialCheck) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if strings.HasPrefix(info.FullMethod, healthPrefix) {
			return handler(ctx, req)
		}

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
