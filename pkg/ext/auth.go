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

// bearerScheme is the credential scheme [BearerToken] strips, matched
// case-insensitively.
const bearerScheme = "Bearer "

var (
	// healthPrefix matches every method of the gRPC health service, which
	// [unaryGuard] lets through. Taken from the service descriptor rather than
	// spelled out, so it cannot drift from what was registered.
	healthPrefix = "/" + grpc_health_v1.Health_ServiceDesc.ServiceName + "/"

	// healthCheckMethods is the set [IsHealthCheckMethod] reports on: methods
	// callers of the proxy invoke, not methods of this server. GetSystemInfo is
	// not a health check but is an SDK client's first call on connect, and
	// Health_Watch is included because the proxy serves gRPC's standard health
	// server.
	healthCheckMethods = map[string]struct{}{
		grpc_health_v1.Health_Check_FullMethodName:                       {},
		grpc_health_v1.Health_Watch_FullMethodName:                       {},
		"/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo": {},
	}
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
	// The request's credentials hold one entry per configured credential header
	// the caller sent, so an empty slice means it presented none; its target is
	// what the call is addressing. A gRPC call sets the target's full_name; an
	// HTTP request to one of the proxy's route groups, such as the codec server,
	// leaves it empty and sets http instead. The proxy's own credential to this
	// server is not among the credentials and stays in the request metadata.
	//
	// Only a Decision of DECISION_ALLOW admits; an unset decision denies. Reason
	// is for whoever operates this server and is withheld from the rejected
	// caller. Return an error only when no verdict was reached, such as an
	// unreachable backend; the proxy denies either way, but an error keeps its
	// status code, so [google.golang.org/grpc/codes.Unavailable] tells a worker to
	// retry where a denial does not.
	//
	// Implementations must be safe for concurrent use and must not block
	// indefinitely; the proxy denies on timeout.
	Auth interface {
		Authenticate(context.Context, *auth.AuthRequest) (*auth.AuthResponse, error)
	}

	// CredentialCheck reports whether a credential presented to this server is
	// valid. [WithServerAuth] installs it and describes the checks made first.
	CredentialCheck func(string) bool
)

// Allow returns the response that admits a caller, and is the only response that
// does. Prefer it to building one by hand: a response whose Decision is unset
// denies, so a zero value is a refusal rather than an oversight.
func Allow() *auth.AuthResponse {
	return &auth.AuthResponse{Decision: auth.AuthResponse_DECISION_ALLOW}
}

// Deny returns the response that refuses a caller. The reason is recorded by the
// proxy and withheld from the caller, so it may name subjects and internal
// systems. Return an error instead when no verdict was reached; both deny, but
// an error keeps its status code, which tells a worker whether to retry.
func Deny(reason string) *auth.AuthResponse {
	return &auth.AuthResponse{Decision: auth.AuthResponse_DECISION_DENY, Reason: reason}
}

// BearerToken returns the token the caller presented on header, with the "Bearer "
// scheme stripped. header is matched case-insensitively, as is the scheme.
//
// Every failure is an [google.golang.org/grpc/codes.Unauthenticated] status error,
// ready to return from [Auth.Authenticate]: no credential on that header, more
// than one value, or a value carrying some other scheme or no token. An
// implementation that would rather answer [Deny], or that accepts a credential
// with no scheme at all, should read req.GetCredentials() directly.
func BearerToken(req *auth.AuthRequest, header string) (string, error) {
	hdr := strings.ToLower(header)

	for _, c := range req.GetCredentials() {
		if !strings.EqualFold(c.GetHeader(), hdr) {
			continue
		}

		vals := c.GetValues()
		if len(vals) != 1 {
			return "", status.Errorf(codes.Unauthenticated, "%s carries %d values, want exactly one", hdr, len(vals))
		}

		v := vals[0]
		if len(v) < len(bearerScheme) || !strings.EqualFold(v[:len(bearerScheme)], bearerScheme) {
			return "", status.Errorf(codes.Unauthenticated, "%s is not a %s credential",
				hdr, strings.TrimSpace(bearerScheme))
		}

		// The scheme with nothing behind it is not a credential. Returning it as an
		// empty token would leave every implementation to notice that for itself.
		tok := v[len(bearerScheme):]
		if tok == "" {
			return "", status.Errorf(codes.Unauthenticated, "%s carries no token", hdr)
		}

		return tok, nil
	}

	return "", status.Errorf(codes.Unauthenticated, "no credential presented on %s", hdr)
}

// IsHealthCheckMethod reports whether full, a gRPC full method name as it arrives
// in [api.auth.v1.Target], is one an implementation will usually admit without a
// credential: the gRPC health methods, and GetSystemInfo, the first call an SDK
// client makes on connect. Whether to admit them is the implementation's call;
// refusing them makes the proxy look unhealthy to probes and makes an
// unauthenticated client fail at dial instead of on its first real call.
func IsHealthCheckMethod(full string) bool {
	_, ok := healthCheckMethods[full]

	return ok
}

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
// one. The health service is exempt, since probes have no credential to present
// and Watch, a stream, is not seen by this interceptor anyway.
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
