package auth_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

type (
	// httpFakeAuth records what it was asked and rejects when err is set.
	httpFakeAuth struct {
		err    error
		secure []string
		target meta.Target
		md     metadata.MD
	}

	// capture records the request the wrapped handler received.
	capture struct {
		req *http.Request
	}
)

func TestHTTPMiddlewareAdmits(t *testing.T) {
	t.Parallel()

	a := &httpFakeAuth{secure: []string{"authorization"}}
	next := &capture{}
	mw := auth.HTTPMiddleware(a, meta.HTTPGroupCodecServer,
		func(*http.Request) string { return "payments" }, logger.NewNoopLogger())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/payments/decode", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	req.Header.Set("X-Caller", "ui")

	rec := httptest.NewRecorder()
	mw(next).ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)

	want := meta.Target{
		Namespace: "payments",
		HTTP:      &meta.HTTPTarget{Group: meta.HTTPGroupCodecServer, Method: "POST", Path: "/payments/decode"},
	}
	require.Equal(t, want, a.target)
	require.Equal(t, []string{"Bearer s3cret"}, a.md.Get("authorization"))

	t.Run("the next handler sees the target", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, want, meta.TargetFrom(next.req.Context()))
	})

	t.Run("secure headers are removed regardless of case", func(t *testing.T) {
		t.Parallel()

		require.Empty(t, next.req.Header.Values("Authorization"))
		require.Equal(t, "ui", next.req.Header.Get("X-Caller"))
	})

	t.Run("the original request is untouched", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "Bearer s3cret", req.Header.Get("Authorization"))
		require.Equal(t, meta.Target{}, meta.TargetFrom(req.Context()))
	})
}

func TestHTTPMiddlewareRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "unauthenticated", err: status.Error(codes.Unauthenticated, "no"), want: http.StatusUnauthorized},
		{name: "permission denied", err: status.Error(codes.PermissionDenied, "no"), want: http.StatusForbidden},
		{name: "unavailable", err: status.Error(codes.Unavailable, "down"), want: http.StatusServiceUnavailable},
		{name: "deadline", err: status.Error(codes.DeadlineExceeded, "slow"), want: http.StatusServiceUnavailable},
		{name: "plain error", err: fmt.Errorf("bad token"), want: http.StatusUnauthorized},
		{name: "internal", err: status.Error(codes.Internal, "broken"), want: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			next := &capture{}
			mw := auth.HTTPMiddleware(&httpFakeAuth{err: tt.err}, meta.HTTPGroupCodecServer,
				func(*http.Request) string { return "" }, logger.NewNoopLogger())

			rec := httptest.NewRecorder()
			mw(next).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/decode", nil))

			require.Equal(t, tt.want, rec.Code)
			require.Nil(t, next.req, "a rejected request must not reach the next handler")
			require.NotContains(t, rec.Body.String(), status.Convert(tt.err).Message(),
				"the reason stays server-side")
		})
	}
}

func TestHTTPMiddlewareRejectionStatusOverride(t *testing.T) {
	t.Parallel()

	mw := auth.HTTPMiddleware(&httpFakeAuth{err: status.Error(codes.Unavailable, "down")},
		meta.HTTPGroupCodecServer, func(*http.Request) string { return "" }, logger.NewNoopLogger(),
		auth.WithRejectionStatus(func(codes.Code) int { return http.StatusTeapot }))

	rec := httptest.NewRecorder()
	mw(&capture{}).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/decode", nil))

	require.Equal(t, http.StatusTeapot, rec.Code)
}

func TestHTTPMiddlewareLogsWithoutTheCredential(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	mw := auth.HTTPMiddleware(&httpFakeAuth{err: status.Error(codes.Unauthenticated, "token expired")},
		meta.HTTPGroupCodecServer, func(*http.Request) string { return "" }, log)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/decode", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	mw(&capture{}).ServeHTTP(httptest.NewRecorder(), req)

	tags := log.TagsOf(logger.LevelWarn, "inbound authentication rejected")
	require.NotEmpty(t, tags)
	require.Equal(t, "codecServer", tags["group"])
	for k, v := range tags {
		require.NotContains(t, fmt.Sprint(v), "s3cret", "tag %q leaked the credential", k)
	}
}

func TestHTTPMiddlewareRequiresAGroup(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() {
		auth.HTTPMiddleware(&httpFakeAuth{}, meta.HTTPGroupUnspecified,
			func(*http.Request) string { return "" }, logger.NewNoopLogger())
	})
}

func (f *httpFakeAuth) Authenticate(_ context.Context, target meta.Target, md metadata.MD) error {
	f.target, f.md = target, md

	return f.err
}

func (f *httpFakeAuth) SecureHeaders() []string { return f.secure }

func (c *capture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.req = r
	w.WriteHeader(http.StatusNoContent)
}
