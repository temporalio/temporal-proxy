package codecserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.temporal.io/api/common/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

// This file implements the codec server's HTTP routes and the order the checks
// on them run in. That order is the security posture, not a style choice, so it
// is spelled out here rather than left to be reconstructed from the code.
//
// CORS is outermost because a preflight arrives as OPTIONS, matches no route,
// and carries no credentials, so it has to be answered before authentication.
// Namespace resolution precedes authentication because the authenticator
// authorizes on the resolved local name. Authentication precedes reading the
// body so an unauthenticated caller cannot make the process allocate or
// unmarshal.

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
	// handler needs, and an implementation must be the same value the
	// per-upstream proxies apply, or a payload will transform differently
	// depending on which path it travelled.
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
		// when it maps no override. Returning the input is deliberate rather
		// than a fallback: the vault resolves an unknown namespace to the
		// default key policy, which is the correct policy for a namespace that
		// has none of its own.
		Local(remote string) string
	}

	// Authenticator decides whether a request may proceed. It is satisfied by
	// [github.com/temporalio/temporal-proxy/internal/auth.Authenticator].
	//
	// Implementations must be safe for concurrent use. Note that the built-in
	// authenticators ignore target and authorize on the credential alone, so a
	// token that passes authorizes every namespace; scoping a token to one
	// namespace requires an implementation that reads target.
	Authenticator interface {
		// Authenticate reports whether the request described by target, carrying
		// the credentials in md, may proceed. target.Namespace is the resolved
		// local name and is empty when the caller named none. target.FullName is
		// the matched route rather than a gRPC method, since this caller serves
		// HTTP.
		//
		// Returns a non-nil error to deny the request. The handler answers 401
		// and does not relay the error's text.
		Authenticate(ctx context.Context, target meta.Target, md metadata.MD) error
	}

	// Option configures a [Handler].
	Option func(*options)

	options struct {
		auth              Authenticator
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
)

// Handler returns the codec server's routes, each served both bare and under a
// namespace path segment because a caller may name the namespace either way.
// Use it when a client reaches a Temporal Service without passing through the
// gateway; a client that goes through the gateway needs no codec server at all.
//
// Returns an http.Handler wrapping an http.ServeMux. An unknown path answers
// 404; a known path used with any method but POST answers 405 with an Allow
// header, which diverges from the SDK's own handler answering 404 there.
//
// Panics if r is nil. A missing reporter is a wiring mistake, and failing at
// construction beats a nil dereference on the first request, which would answer
// 500 on a surface that otherwise only ever answers 4xx.
//
// The returned handler is safe for concurrent use.
func Handler(c Codecs, n Namespaces, r *Reporter, opts ...Option) http.Handler {
	if r == nil {
		panic("codecserver: Handler requires a non-nil reporter")
	}

	o := options{maxBodyBytes: defaultMaxBodyBytes}
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

	mux := http.NewServeMux()
	for _, route := range routes {
		mux.HandleFunc("POST "+route.path, route.fn)
		mux.HandleFunc("POST /{ns}"+route.path, route.fn)
	}

	// Outermost, so a preflight is answered without reaching a route or the
	// authenticator.
	return cors(o.origins, o.credentials, mux)
}

// WithAuth requires every request to satisfy a, which answers 401 for any
// request a denies. Omitting it serves every request unauthenticated, which is
// only defensible on a loopback bind, and is why configuration rejects an
// enabled codec server bound beyond loopback with no auth block.
//
// A preflight is answered before a reaches it, since a browser sends no
// credentials on one.
func WithAuth(a Authenticator) Option {
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

// WithMaxBodyBytes bounds how much of a request body is read, answering 400
// once a caller exceeds it. The cap matters more here than on an ordinary
// endpoint because this surface unwraps a DEK on demand, so an unbounded body
// is an unbounded amount of work for an unauthenticated-at-the-edge caller.
//
// A value of zero or less keeps the default of 4 MiB.
func WithMaxBodyBytes(n int64) Option {
	return func(o *options) {
		if n > 0 {
			o.maxBodyBytes = n
		}
	}
}

// WithNamespaceRequired makes /encode answer 400 when a request names no
// namespace. Set it when a per-namespace key policy is configured: sealing a
// payload that cannot be attributed to a namespace would silently take the
// default policy rather than the one the operator wrote, which is a policy
// violation that no later error reveals.
//
// It never constrains /decode, which does not need a namespace to open a
// payload.
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
		start := time.Now()
		code := http.StatusOK
		defer func() { h.reporter.Request(route, code, time.Since(start).Seconds()) }()

		ns, c, ok := h.prologue(w, r, route, namespaceRequired)
		if !ok {
			code = c
			return
		}

		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, contentTypeJSON) {
			code = fail(w, http.StatusBadRequest, fmt.Sprintf(
				"expected content-type %s, got %q",
				contentTypeJSON,
				ct,
			))

			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.opts.maxBodyBytes))
		if err != nil {
			code = fail(w, http.StatusBadRequest, "failed to read request body: "+err.Error())
			return
		}

		var in common.Payloads
		if err := protojson.Unmarshal(body, &in); err != nil {
			code = fail(w, http.StatusBadRequest, "failed to parse payloads: "+err.Error())
			return
		}

		// The detail of a codec failure is never sent to the caller: this surface
		// holds KMS unwrap permission, so a provider error can carry key ARNs,
		// ciphertext metadata, or vault messages that must not reach an HTTP
		// response body.
		out, err := fn(r.Context(), ns, in.Payloads)
		if err != nil {
			code = fail(w, http.StatusBadRequest, "failed to transform payloads")
			return
		}

		res, err := protojson.Marshal(&common.Payloads{Payloads: out})
		if err != nil {
			code = fail(w, http.StatusBadRequest, "failed to serialize payloads: "+err.Error())
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
	start := time.Now()
	code := http.StatusOK
	defer func() { h.reporter.Request(downloadRoute, code, time.Since(start).Seconds()) }()

	if _, c, ok := h.prologue(w, r, downloadRoute, false); !ok {
		code = c
		return
	}

	code = fail(w, http.StatusBadRequest, "all payloads must be storage references")
}

// prologue resolves the namespace and authenticates, the checks every route
// shares. It writes the response and reports ok=false when the request may not
// proceed, and returns the status it wrote as code.
func (h *handler) prologue(
	w http.ResponseWriter,
	r *http.Request,
	route string,
	namespaceRequired bool,
) (ns string, code int, ok bool) {
	ns, named := h.namespace(r)
	if namespaceRequired && !named {
		code = fail(w, http.StatusBadRequest,
			"a namespace is required: send it as the "+namespaceHeader+" header or in the request path")

		return "", code, false
	}

	if h.opts.auth != nil {
		target := meta.Target{FullName: "POST " + route, Namespace: ns}
		if err := h.opts.auth.Authenticate(r.Context(), target, headerMetadata(r.Header)); err != nil {
			code = fail(w, http.StatusUnauthorized, "unauthenticated")

			return "", code, false
		}
	}

	return ns, http.StatusOK, true
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

// fail writes status with message as the body and returns status, so a caller
// can record what it sent in one statement. Every failure here is a 4xx, since
// a browser retries a 5xx three times but never a 4xx, and reporting a
// misconfiguration as a server error would triple its own load.
func fail(w http.ResponseWriter, status int, message string) int {
	http.Error(w, message, status)

	return status
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
