package auth

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

type (
	// HTTPOption configures [HTTPMiddleware].
	HTTPOption func(*httpOptions)

	httpOptions struct {
		status func(codes.Code) int
	}
)

// HTTPMiddleware adapts a to an HTTP route group. Each request is authenticated
// against a target naming group, its method and path, and the namespace the
// group's resolver returns ("" for unknown). An admitted request reaches next
// with the target on its context and a's secure headers removed; a rejected one
// is answered with a generic body and a status mapped from the rejection's gRPC
// code (401, 403 for PermissionDenied, 503 for Unavailable or DeadlineExceeded),
// unless [WithRejectionStatus] says otherwise. A nil log falls back to the
// default logger. Panics if group is [meta.HTTPGroupUnspecified].
func HTTPMiddleware(
	a Authenticator,
	group meta.HTTPGroup,
	namespace func(*http.Request) string,
	log logger.Logger,
	opts ...HTTPOption,
) func(http.Handler) http.Handler {
	if group == meta.HTTPGroupUnspecified {
		panic("auth: HTTPMiddleware requires a route group")
	}

	if log == nil {
		log = logger.Default()
	}

	o := httpOptions{status: defaultRejectionStatus}
	for _, opt := range opts {
		opt(&o)
	}

	secure := a.SecureHeaders()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			target := meta.Target{
				Namespace: namespace(r),
				HTTP:      &meta.HTTPTarget{Group: group, Method: r.Method, Path: r.URL.Path},
			}

			if err := a.Authenticate(r.Context(), target, headerMetadata(r.Header)); err != nil {
				code := status.Code(err)
				log.Warn(
					"inbound authentication rejected",
					tag.String("group", group.String()),
					tag.String("method", r.Method),
					tag.String("path", r.URL.Path),
					tag.String("code", code.String()),
					tag.String("reason", err.Error()),
				)

				st := o.status(code)
				http.Error(w, http.StatusText(st), st)

				return
			}

			// Cloned so the caller's request keeps its headers, and stripped so a
			// consumed credential never reaches a handler that might forward it.
			admitted := r.Clone(meta.WithTarget(r.Context(), target))
			for _, h := range secure {
				admitted.Header.Del(h)
			}

			next.ServeHTTP(w, admitted)
		})
	}
}

// WithRejectionStatus replaces the mapping from a rejection's gRPC code to the
// HTTP status sent, for a group whose callers need different answers.
func WithRejectionStatus(f func(codes.Code) int) HTTPOption {
	return func(o *httpOptions) { o.status = f }
}

// defaultRejectionStatus maps a rejection to an HTTP status: retryable provider
// failures are 503, a refused caller is 403, and everything else is 401.
func defaultRejectionStatus(c codes.Code) int {
	switch c {
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unavailable, codes.DeadlineExceeded:
		return http.StatusServiceUnavailable
	default:
		return http.StatusUnauthorized
	}
}

// headerMetadata converts HTTP headers into gRPC metadata, which is what an
// Authenticator reads. metadata.MD.Set lowercases each key, matching how an
// authenticator names the header it wants.
func headerMetadata(h http.Header) metadata.MD {
	md := metadata.MD{}
	for key, values := range h {
		md.Set(key, values...)
	}

	return md
}
