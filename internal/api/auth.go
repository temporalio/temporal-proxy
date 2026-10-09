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
// extension server implementing api.auth.v1.AuthService. Its headers name the
// metadata carrying the caller's credentials: their values are lifted into the
// request, and they are reported as [Auth.SecureHeaders] so they are stripped
// from the stream before it reaches an upstream.
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
// explicit DECISION_ALLOW admits the stream; an error, a denial, or an answer
// carrying no verdict denies it, so the request fails closed. Every rejection is
// an [rpc.Reject] with a generic message and the provider's reason as the
// server-side detail. An error keeps the provider's status code; a denial is
// PermissionDenied and a missing verdict is Internal.
//
// The declared credential headers are lifted into the request and withheld from
// the forwarded metadata. The caller's remaining metadata is forwarded, except
// the reserved keys gRPC drops (":authority", "user-agent", "content-type",
// "grpc-*"), so a caller cannot reach the extension server's transport this way.
func (a *Auth) Authenticate(ctx context.Context, target meta.Target, md metadata.MD) error {
	req := &auth.AuthRequest{Target: protoTarget(target)}
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

// protoTarget converts the gateway's view of a request into the wire Target an
// extension server decides on.
func protoTarget(t meta.Target) *auth.Target {
	pt := &auth.Target{FullName: t.FullName, Namespace: t.Namespace}
	if t.HTTP != nil {
		pt.Http = &auth.HTTPTarget{
			Group:  protoHTTPGroup(t.HTTP.Group),
			Method: t.HTTP.Method,
			Path:   t.HTTP.Path,
		}
	}

	return pt
}

// protoHTTPGroup maps a route group onto its wire value. A group with no case
// here maps to GROUP_UNSPECIFIED, which TestAuthMapsEveryHTTPGroup catches.
func protoHTTPGroup(g meta.HTTPGroup) auth.HTTPTarget_Group {
	switch g {
	case meta.HTTPGroupCodecServer:
		return auth.HTTPTarget_GROUP_CODEC_SERVER
	default:
		return auth.HTTPTarget_GROUP_UNSPECIFIED
	}
}
