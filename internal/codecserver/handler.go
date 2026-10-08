package codecserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.temporal.io/api/common/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// This file implements the codec server's HTTP routes and the order the checks
// on them run in. That order is the security posture, not a style choice, so it
// is spelled out here rather than left to be reconstructed from the code.
//
// CORS is outermost because a preflight arrives as OPTIONS, matches no route,
// and carries no credentials, so it has to be answered before authentication.
// Every matched request is then reported, rejections included. Authentication
// wraps each route inside the mux, because the namespace it authorizes on can
// come from the {ns} path segment, which the mux sets only once it has
// matched. The namespace is resolved once, ahead of authentication, so the
// route acts on the namespace that was authorized even when the authenticator
// strips the header naming it. Authentication precedes request validation and
// reading the body, so an unauthenticated caller learns nothing about the
// request and cannot make the process allocate or unmarshal.

const (
	// defaultMaxBodyBytes bounds a request body. This service unwraps a DEK on
	// demand, so it does not read an unbounded one.
	defaultMaxBodyBytes int64 = 4 << 20

	contentTypeJSON = "application/json"

	// namespaceHeader is where the Temporal UI and CLI name the namespace. The
	// CLI can also put it in the request path.
	namespaceHeader = "X-Namespace"

	encodeRoute   = "/encode"
	decodeRoute   = "/decode"
	downloadRoute = "/download"
)

type (
	// Codecs transforms payloads on their way to and from an upstream. It is the
	// subset of [github.com/temporalio/temporal-proxy/internal/proxy.Codecs] the
	// handler needs, and an implementation must be the same value the upstream
	// forwarders apply, or a payload will transform differently depending on
	// which path it travelled.
	//
	// Implementations must be safe for concurrent use, must return the same
	// number of payloads they were given in the same order, and must not mutate
	// the slice they were passed.
	Codecs interface {
		// Decode opens payloads that arrived from an upstream. ns is the local
		// namespace name and may be empty, since opening a sealed payload
		// selects its key by the ID recorded in the payload itself.
		//
		// Returns an error if a payload cannot be opened. The handler never
		// forwards that error's text to the caller.
		Decode(ctx context.Context, ns string, payloads []*common.Payload) ([]*common.Payload, error)

		// Encode seals payloads bound for an upstream. ns is the local namespace
		// name and selects the key policy, so an empty ns takes the default
		// policy rather than a namespace's own.
		//
		// Returns an error if a payload cannot be sealed. The handler never
		// forwards that error's text to the caller.
		Encode(ctx context.Context, ns string, payloads []*common.Payload) ([]*common.Payload, error)
	}

	// Namespaces maps the namespace name a caller sends to the one the codecs
	// key on. It is satisfied by [OverrideMap].
	//
	// Implementations must be safe for concurrent use and must be total: there
	// is no error path, because a name the mapping does not know is a name that
	// needs no translation.
	Namespaces interface {
		// Local returns the local namespace name for remote, or remote unchanged
		// when it maps no override.
		Local(remote string) string
	}

	// Option configures a [Handler].
	Option func(*options)

	options struct {
		auth              auth.Authenticator
		log               logger.Logger
		origins           []string
		credentials       bool
		maxBodyBytes      int64
		namespaceRequired bool
	}

	handler struct {
		codecs   Codecs
		ns       Namespaces
		reporter *Reporter
		opts     options
	}

	// resolvedNamespace is the namespace a request addresses, resolved once
	// before authentication so the route acts on the namespace that was
	// authorized, even when the authenticator strips the header naming it.
	resolvedNamespace struct {
		name  string
		named bool
	}

	// namespaceKey carries a resolvedNamespace on a request's context.
	namespaceKey struct{}

	// statusRecorder remembers the status a handler wrote, so the request is
	// reported with what the caller actually received.
	statusRecorder struct {
		http.ResponseWriter
		status int
	}
)

// Handler returns the codec server's routes, each served both bare and under a
// namespace path segment. Use it when a client reaches a Temporal Service
// without passing through the gateway. An unknown path answers 404, and a known
// path used with any method but POST answers 405 with an Allow header, where
// the SDK's own handler answers 404.
//
// Panics if r is nil. The returned handler is safe for concurrent use.
func Handler(c Codecs, n Namespaces, r *Reporter, opts ...Option) http.Handler {
	if r == nil {
		panic("codecserver: Handler requires a non-nil reporter")
	}

	o := options{log: logger.NewNoopLogger(), maxBodyBytes: defaultMaxBodyBytes}
	for _, opt := range opts {
		opt(&o)
	}

	h := &handler{codecs: c, ns: n, reporter: r, opts: o}

	routes := []struct {
		path string
		fn   http.HandlerFunc
	}{
		{encodeRoute, h.payloads(encodeRoute, h.codecs.Encode, o.namespaceRequired)},
		{decodeRoute, h.payloads(decodeRoute, h.codecs.Decode, false)},
		{downloadRoute, h.download},
	}

	authenticate := func(next http.Handler) http.Handler { return next }
	if o.auth != nil {
		authenticate = auth.HTTPMiddleware(o.auth, meta.HTTPGroupCodecServer, namespaceName, o.log,
			auth.WithRejectionStatus(rejectionStatus))
	}

	mux := http.NewServeMux()
	for _, route := range routes {
		served := h.instrument(route.path, h.resolveNamespace(authenticate(route.fn)))
		mux.Handle("POST "+route.path, served)
		mux.Handle("POST /{ns}"+route.path, served)
	}

	// Outermost, so a preflight is answered without reaching a route or the
	// authenticator.
	return cors(o.origins, o.credentials, mux)
}

// WithAuth requires every request to satisfy a. A rejection is answered with a
// 4xx, never a 5xx a browser would retry. A preflight is answered before a
// reaches it. Omitting it serves every request unauthenticated, which
// configuration only permits on a loopback bind.
func WithAuth(a auth.Authenticator) Option {
	return func(o *options) { o.auth = a }
}

// WithCORS answers preflight requests and allows the listed origins, which any
// browser-based caller such as the Temporal Cloud UI requires. Omitting it
// leaves OPTIONS unhandled, so the route answers 405 and a browser call fails
// before it reaches a payload.
func WithCORS(origins []string, credentials bool) Option {
	return func(o *options) {
		o.origins = origins
		o.credentials = credentials
	}
}

// WithLogger logs what the handler withholds from the caller, namely the cause
// of a codec failure, which is usually a KMS permission or key problem only the
// operator can fix. Omitting it discards those causes.
func WithLogger(l logger.Logger) Option {
	return func(o *options) { o.log = l }
}

// WithMaxBodyBytes bounds how much of a request body is read, answering 400
// once a caller exceeds it. A value of zero or less keeps the default of 4 MiB.
func WithMaxBodyBytes(n int64) Option {
	return func(o *options) {
		if n > 0 {
			o.maxBodyBytes = n
		}
	}
}

// WithNamespaceRequired makes /encode answer 400 when a request names no
// namespace; it never constrains /decode. Set it when a per-namespace key
// policy is configured, or a payload naming no namespace is silently sealed
// under the default policy.
func WithNamespaceRequired(required bool) Option {
	return func(o *options) { o.namespaceRequired = required }
}

// payloads returns the handler for a route that transforms payloads with fn.
func (h *handler) payloads(
	route string,
	fn func(context.Context, string, []*common.Payload) ([]*common.Payload, error),
	namespaceRequired bool,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ns, ok := h.prologue(w, r, namespaceRequired)
		if !ok {
			return
		}

		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, contentTypeJSON) {
			fail(w, http.StatusBadRequest, fmt.Sprintf(
				"expected content-type %s, got %q",
				contentTypeJSON,
				ct,
			))

			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.opts.maxBodyBytes))
		if err != nil {
			fail(w, http.StatusBadRequest, "failed to read request body: "+err.Error())
			return
		}

		var in common.Payloads
		if err := protojson.Unmarshal(body, &in); err != nil {
			fail(w, http.StatusBadRequest, "failed to parse payloads: "+err.Error())
			return
		}

		// The detail of a codec failure is never sent to the caller: this surface
		// holds KMS unwrap permission, so a provider error can carry key ARNs,
		// ciphertext metadata, or vault messages that must not reach an HTTP
		// response body. It is logged instead, since the operator needs it.
		out, err := fn(r.Context(), ns, in.Payloads)
		if err != nil {
			h.opts.log.Error("Codec server failed to transform payloads",
				tag.String("route", route),
				tag.String("namespace", ns),
				tag.Error(err),
			)

			fail(w, http.StatusBadRequest, "failed to transform payloads")
			return
		}

		res, err := protojson.Marshal(&common.Payloads{Payloads: out})
		if err != nil {
			fail(w, http.StatusBadRequest, "failed to serialize payloads: "+err.Error())
			return
		}

		w.Header().Set("Content-Type", contentTypeJSON)
		_, _ = w.Write(res)
	}
}

// download reports that it has nothing to retrieve. The route exists because
// the contract has it, but external storage references are only produced by a
// client that offloads oversized payloads, which the proxy does not do, so no
// request reaching here can carry one; the message is the one the SDK's own
// handler returns for a request carrying no references.
func (h *handler) download(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.prologue(w, r, false); !ok {
		return
	}

	fail(w, http.StatusBadRequest, "all payloads must be storage references")
}

// prologue resolves the namespace and enforces that one was named when the
// route requires it. It writes the response and reports ok=false when the
// request may not proceed.
func (h *handler) prologue(w http.ResponseWriter, r *http.Request, namespaceRequired bool) (string, bool) {
	ns := requestNamespace(r)
	if namespaceRequired && !ns.named {
		fail(w, http.StatusBadRequest,
			"a namespace is required: send it as the "+namespaceHeader+" header or in the request path")

		return "", false
	}

	return ns.name, true
}

// namespace returns the local namespace name the request addresses and whether
// it named one at all. The path segment wins over the header, since a caller
// that put it in the URL was explicit about it.
func (h *handler) namespace(r *http.Request) (string, bool) {
	name := r.PathValue("ns")
	if name == "" {
		name = r.Header.Get(namespaceHeader)
	}

	if name == "" {
		return "", false
	}

	return h.ns.Local(name), true
}

// instrument reports every request reaching route with the status the caller
// received and how long it took.
func (h *handler) instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		h.reporter.Request(route, rec.status, time.Since(start).Seconds())
	})
}

// resolveNamespace resolves the namespace the request addresses and carries it
// on the context for everything after it. It runs inside the mux, since the
// {ns} path segment is only set once a route has matched.
func (h *handler) resolveNamespace(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, named := h.namespace(r)
		ctx := context.WithValue(r.Context(), namespaceKey{}, resolvedNamespace{name: name, named: named})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// WriteHeader records status before writing it.
func (s *statusRecorder) WriteHeader(status int) {
	s.status = status
	s.ResponseWriter.WriteHeader(status)
}

// Unwrap returns the wrapped writer, so http.ResponseController can reach it.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// fail writes status with message as the body. Every failure here is a 4xx,
// since a browser retries a 5xx three times but never a 4xx, and reporting a
// misconfiguration as a server error would triple its own load.
func fail(w http.ResponseWriter, status int, message string) {
	http.Error(w, message, status)
}

// namespaceName returns the local namespace the request addresses, or "" when
// it named none, for the authenticator to authorize on.
func namespaceName(r *http.Request) string {
	return requestNamespace(r).name
}

// rejectionStatus keeps every authentication failure a 4xx: a browser retries
// a 5xx three times, which would triple a provider outage's load.
func rejectionStatus(c codes.Code) int {
	if c == codes.PermissionDenied {
		return http.StatusForbidden
	}

	return http.StatusUnauthorized
}

// requestNamespace returns the namespace resolveNamespace put on r's context.
// Panics if it is absent, since a route reached without it would act on no
// namespace at all.
func requestNamespace(r *http.Request) resolvedNamespace {
	ns, ok := r.Context().Value(namespaceKey{}).(resolvedNamespace)
	if !ok {
		panic("codecserver: request reached a route without a resolved namespace")
	}

	return ns
}
