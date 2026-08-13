package api

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/rpc"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/api/auth/v1"
)

// Auth authenticates an inbound stream by delegating the decision to an
// extension server implementing api.auth.v1.AuthService. It is the escape hatch
// for identity systems the built-in authenticators do not cover: the proxy
// asks, the operator's server decides.
//
// The headers name the metadata carrying the caller's credentials. They are
// declared rather than discovered because a verdict reports only admit-or-deny
// and says nothing about which headers mattered, and the proxy needs to know two
// things: which values to lift into the request, and which to report as
// [Auth.SecureHeaders] so they are stripped from the stream before it reaches an
// upstream, where a caller credential would collide with the proxy's own.
type Auth struct {
	client  auth.AuthServiceClient
	headers []string
}

// NewAuth returns an Auth that consults the AuthService reachable over cc and
// reports secureHeaders as the headers to strip from an admitted stream. As with
// [NewKMS], cc is not owned here: it is shared with anything else configured on
// the same extension server and closed with the application.
func NewAuth(cc grpc.ClientConnInterface, secureHeaders []string) *Auth {
	headers := slices.Clone(secureHeaders)
	for i, h := range headers {
		headers[i] = strings.ToLower(h)
	}

	return &Auth{
		client:  auth.NewAuthServiceClient(cc),
		headers: headers,
	}
}

// Authenticate asks the extension server whether the caller may proceed. Only an
// explicit DECISION_ALLOW admits the stream: an error, a denial, and an answer
// carrying no verdict all deny it, so a server that is down, misconfigured, or
// newer than this build fails the request closed rather than opening the gateway
// to everyone for as long as it is that way.
//
// A denial reaches the caller as an [rpc.Reject], whose message is generic while
// the provider's reason becomes the server-side detail: a provider writes that
// reason for whoever operates it, not for the caller it just turned away. An
// error keeps the provider's status code, since it tells a worker whether to fix
// its credential or retry; a decision carries no code, so the proxy supplies one.
//
// The declared credential headers are lifted into the request and withheld from
// the forwarded metadata, so each credential reaches the server in exactly one
// place. That separation is what lets the proxy hold a credential of its own to
// this server: metadata carries the proxy's, the request carries the caller's,
// and neither has to be told apart from the other on a shared header. It also
// puts the caller's credential out of reach of the interceptor [outbound.DialOptions]
// installs, which deletes the proxy's credential header from forwarded metadata
// and cannot tell that on this one call that header is the subject of the request
// rather than incidental cargo.
//
// The caller's remaining metadata is forwarded so the server can weigh context
// such as the method being invoked. gRPC drops reserved keys (":authority",
// "user-agent", "content-type", "grpc-*") when writing the request, so a caller
// cannot reach the extension server's transport this way.
func (a *Auth) Authenticate(ctx context.Context, target meta.Target, md metadata.MD) error {
	req := &auth.AuthRequest{
		Target: &auth.Target{FullName: target.FullName, Namespace: target.Namespace},
	}
	fwd := md.Copy()

	for _, h := range a.headers {
		if vals := fwd.Get(h); len(vals) > 0 {
			req.Credentials = append(req.Credentials, &auth.Credential{Header: h, Values: vals})
		}

		// Withheld from the forwarded metadata even when absent, so a credential
		// lives in exactly one place and the server is never weighing a value the
		// request did not vouch for.
		fwd.Delete(h)
	}

	// Replaces rather than merges any outgoing metadata already on ctx: md comes
	// from the inbound stream and is what the server is being asked about.
	resp, err := a.client.Auth(metadata.NewOutgoingContext(ctx, fwd), req)
	if err != nil {
		// Returned unwrapped, and with the provider's message demoted to the
		// detail: gRPC would otherwise send the caller err.Error(), which is the
		// reason this rejection exists to keep server-side.
		st := status.Convert(err)

		return rpc.Reject(st.Code(), clientMessageFor(st.Code()), "external auth: "+st.Message())
	}

	switch d := resp.GetDecision(); d {
	case auth.AuthResponse_DECISION_ALLOW:
		return nil
	case auth.AuthResponse_DECISION_DENY:
		// The proxy supplies the code, since a decision carries none: the provider
		// judged the caller rather than its credential, which is PermissionDenied.
		return rpc.Reject(
			codes.PermissionDenied,
			clientMessageFor(codes.PermissionDenied),
			"external auth: "+cmp.Or(resp.GetReason(), "denied without a reason"),
		)
	default:
		// DECISION_UNSPECIFIED, or a value added to the enum after this build. A
		// provider that answers without a verdict is misconfigured or too new, and
		// neither is a reason to admit a caller. Internal rather than
		// PermissionDenied because the fault is the provider's, not the caller's.
		return rpc.Reject(
			codes.Internal,
			clientMessageFor(codes.Internal),
			fmt.Sprintf("external auth: provider returned no usable decision (%s)", d),
		)
	}
}

// SecureHeaders returns the credential headers the proxy must strip before
// forwarding an admitted stream upstream. The result is a copy: the strip list
// is a security control, so a caller inspecting it cannot quietly shorten it.
func (a *Auth) SecureHeaders() []string { return slices.Clone(a.headers) }

// clientMessageFor returns what a rejected caller is told for code. The code
// already says whether to fix a credential or retry later, so the message only
// has to avoid repeating the provider's own reason, which is written for whoever
// operates it and may name internal systems or subjects.
func clientMessageFor(code codes.Code) string {
	switch code {
	case codes.Unauthenticated:
		return "invalid credentials"
	case codes.PermissionDenied:
		return "caller is not permitted"
	default:
		return "authentication temporarily unavailable"
	}
}
