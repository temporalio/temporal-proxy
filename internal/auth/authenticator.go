package auth

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

const (
	defaultHeader = "authorization"
	defaultScheme = "Bearer"
)

type (
	// Authenticator authenticates an inbound request from its metadata. It
	// returns nil to allow the request, or a gRPC status error to reject it.
	// SecureHeaders reports the metadata headers the authenticator consumes, so
	// the proxy can strip the caller's credentials before forwarding upstream;
	// it returns nil when the authenticator consumes no header. An
	// authenticator may name more than one because it need not own the header
	// it reads: an external one is told which headers its server consumes.
	Authenticator interface {
		Authenticate(ctx context.Context, md metadata.MD) error
		SecureHeaders() []string
	}

	defaultAuthenticator struct{}

	// strippedStream overrides Context so a downstream handler sees metadata with a
	// consumed credential header removed.
	strippedStream struct {
		grpc.ServerStream
		ctx context.Context
	}
)

// StreamServerInterceptor adapts an Authenticator to a gRPC stream server
// interceptor, logging each rejection's detailed reason (never the token) via
// log. a must be non-nil; callers get one from the auth module, where the
// unconfigured case is the admit-all default. A nil log falls back to the
// default logger.
func StreamServerInterceptor(a Authenticator, log logger.Logger) grpc.StreamServerInterceptor {
	if log == nil {
		log = logger.Default()
	}

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		if err := a.Authenticate(ss.Context(), md); err != nil {
			log.Warn(
				"inbound authentication rejected",
				tag.String("method", info.FullMethod),
				tag.String("code", status.Code(err).String()),
				tag.String("reason", err.Error()),
			)

			return err
		}

		// The proxy terminates inbound auth, so strip the headers it consumed:
		// the caller's credential must not be forwarded upstream, where it would
		// otherwise collide with (or leak alongside) an outbound credential on
		// the same header.
		if hdrs := a.SecureHeaders(); len(hdrs) > 0 {
			stripped := md.Copy()
			for _, hdr := range hdrs {
				stripped.Delete(hdr)
			}

			ss = &strippedStream{
				ServerStream: ss,
				ctx:          metadata.NewIncomingContext(ss.Context(), stripped),
			}
		}

		return handler(srv, ss)
	}
}

func (a *defaultAuthenticator) Authenticate(_ context.Context, _ metadata.MD) error {
	return nil
}

// SecureHeaders reports that the admit-all default consumes no header, so
// StreamServerInterceptor strips nothing and the transparent relay is
// preserved.
func (a *defaultAuthenticator) SecureHeaders() []string { return nil }

// Context returns the context carrying the stripped incoming metadata.
func (s *strippedStream) Context() context.Context { return s.ctx }

// canonicalHeader returns the metadata header to use for a credential: the
// default when h is blank, otherwise h lowercased. gRPC canonicalizes metadata
// keys to lowercase, so normalizing here keeps a mixed-case configured header
// matching what md lookups, strips, and per-RPC credentials actually send.
func canonicalHeader(h string) string {
	if h == "" {
		return defaultHeader
	}

	return strings.ToLower(h)
}

// extractToken returns the credential carried in md under header, stripping the
// scheme prefix (case-insensitive). It returns ok=false when the header is
// absent or the scheme does not match. A blank scheme returns the raw value.
func extractToken(md metadata.MD, header, scheme string) (string, bool) {
	vals := md.Get(header)
	if len(vals) == 0 {
		return "", false
	}

	v := vals[0]
	if scheme == "" {
		return v, true
	}

	prefix := scheme + " "
	if len(v) < len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
		return "", false
	}

	return v[len(prefix):], true
}
