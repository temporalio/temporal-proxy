package auth

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	// Header reports the metadata header the authenticator consumes, so the
	// proxy can strip the caller's credential before forwarding upstream; it
	// returns "" when the authenticator consumes no header.
	Authenticator interface {
		Authenticate(ctx context.Context, md metadata.MD) error
		Header() string
	}

	// rejection is an authentication failure that reports a generic, client-safe
	// gRPC status to the caller while carrying a detailed reason for server-side
	// logging. The detail must never contain secrets (tokens or key material).
	rejection struct {
		st     *status.Status
		detail string
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

		// The proxy terminates inbound auth, so strip the header it consumed:
		// the caller's credential must not be forwarded upstream, where it would
		// otherwise collide with (or leak alongside) an outbound credential on
		// the same header.
		if header := a.Header(); header != "" {
			stripped := md.Copy()
			stripped.Delete(header)
			ss = &strippedStream{
				ServerStream: ss,
				ctx:          metadata.NewIncomingContext(ss.Context(), stripped),
			}
		}

		return handler(srv, ss)
	}
}

// Error returns the detailed, server-side rejection reason. It must never be
// sent to the client directly; gRPC surfaces GRPCStatus() instead.
func (r *rejection) Error() string { return r.detail }

// GRPCStatus lets gRPC surface the client-safe status while Error keeps the
// detail server-side.
func (r *rejection) GRPCStatus() *status.Status { return r.st }

func (a *defaultAuthenticator) Authenticate(_ context.Context, _ metadata.MD) error {
	return nil
}

// Header reports that the admit-all default consumes no header, so
// StreamServerInterceptor strips nothing and the transparent relay is
// preserved.
func (a *defaultAuthenticator) Header() string { return "" }

// Context returns the context carrying the stripped incoming metadata.
func (s *strippedStream) Context() context.Context { return s.ctx }

// reject builds a rejection carrying a client-safe code+message and a
// server-side detail for logging.
func reject(code codes.Code, clientMsg, detail string) error {
	return &rejection{st: status.New(code, clientMsg), detail: detail}
}

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
