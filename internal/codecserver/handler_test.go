package codecserver_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/temporalio/temporal-proxy/internal/codecserver"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

type (
	// fakeCodecs records the namespace it was handed and marks the payload, so a
	// test can tell an encode from a decode without any real codec.
	fakeCodecs struct {
		namespaces []string
		err        error
	}

	// fakeNamespaces maps one name and passes everything else through, mirroring
	// OverrideMap without depending on config.
	fakeNamespaces map[string]string

	// fakeAuth rejects when err is set and records the target it saw.
	fakeAuth struct {
		err     error
		targets []meta.Target
	}
)

func (f *fakeCodecs) Decode(_ context.Context, ns string, ps []*common.Payload) ([]*common.Payload, error) {
	return f.mark(ns, ps, "decoded:")
}

func (f *fakeCodecs) Encode(_ context.Context, ns string, ps []*common.Payload) ([]*common.Payload, error) {
	return f.mark(ns, ps, "encoded:")
}

func (f *fakeCodecs) mark(ns string, ps []*common.Payload, prefix string) ([]*common.Payload, error) {
	f.namespaces = append(f.namespaces, ns)
	if f.err != nil {
		return nil, f.err
	}

	out := make([]*common.Payload, len(ps))
	for i, p := range ps {
		out[i] = &common.Payload{Metadata: p.Metadata, Data: append([]byte(prefix), p.Data...)}
	}

	return out, nil
}

func (f fakeNamespaces) Local(remote string) string {
	if local, ok := f[remote]; ok {
		return local
	}

	return remote
}

func (f *fakeAuth) Authenticate(_ context.Context, target meta.Target, _ metadata.MD) error {
	f.targets = append(f.targets, target)

	return f.err
}

func TestHandlerRoutes(t *testing.T) {
	t.Parallel()

	body := func(data ...string) string {
		ps := make([]*common.Payload, len(data))
		for i, d := range data {
			ps[i] = &common.Payload{
				Metadata: map[string][]byte{"encoding": []byte("json/plain")},
				Data:     []byte(d),
			}
		}

		out, err := protojson.Marshal(&common.Payloads{Payloads: ps})
		require.NoError(t, err)

		return string(out)
	}

	tests := []struct {
		name     string
		method   string
		path     string
		headers  map[string]string
		body     string
		wantCode int
		wantBody string   // substring, checked on error responses only
		wantData []string // decoded payload data, checked on success responses only
		wantNS   []string
	}{
		{
			name:     "decode via header namespace",
			method:   http.MethodPost,
			path:     "/decode",
			headers:  map[string]string{"Content-Type": "application/json", "X-Namespace": "payments.a8x72"},
			body:     body(`"hi"`),
			wantCode: http.StatusOK,
			wantData: []string{`decoded:"hi"`},
			wantNS:   []string{"payments"},
		},
		{
			name:     "decode via path namespace",
			method:   http.MethodPost,
			path:     "/payments.a8x72/decode",
			headers:  map[string]string{"Content-Type": "application/json"},
			body:     body(`"hi"`),
			wantCode: http.StatusOK,
			wantNS:   []string{"payments"},
		},
		{
			name:     "path namespace wins over the header",
			method:   http.MethodPost,
			path:     "/payments.a8x72/decode",
			headers:  map[string]string{"Content-Type": "application/json", "X-Namespace": "other"},
			body:     body(`"hi"`),
			wantCode: http.StatusOK,
			wantNS:   []string{"payments"},
		},
		{
			name:     "decode with no namespace is allowed",
			method:   http.MethodPost,
			path:     "/decode",
			headers:  map[string]string{"Content-Type": "application/json"},
			body:     body(`"hi"`),
			wantCode: http.StatusOK,
			wantNS:   []string{""},
		},
		{
			name:     "unknown namespace passes through",
			method:   http.MethodPost,
			path:     "/decode",
			headers:  map[string]string{"Content-Type": "application/json", "X-Namespace": "orders.a8x72"},
			body:     body(`"hi"`),
			wantCode: http.StatusOK,
			wantNS:   []string{"orders.a8x72"},
		},
		{
			name:     "encode",
			method:   http.MethodPost,
			path:     "/encode",
			headers:  map[string]string{"Content-Type": "application/json", "X-Namespace": "payments.a8x72"},
			body:     body(`"hi"`),
			wantCode: http.StatusOK,
			wantData: []string{`encoded:"hi"`},
			wantNS:   []string{"payments"},
		},
		{
			name:     "unknown query parameters are ignored",
			method:   http.MethodPost,
			path:     "/decode?preserveStorageRefs=true",
			headers:  map[string]string{"Content-Type": "application/json"},
			body:     body(`"hi"`),
			wantCode: http.StatusOK,
			wantNS:   []string{""},
		},
		{
			name:     "download has nothing to retrieve",
			method:   http.MethodPost,
			path:     "/download",
			headers:  map[string]string{"Content-Type": "application/json"},
			body:     body(`"hi"`),
			wantCode: http.StatusBadRequest,
			wantBody: "all payloads must be storage references",
		},
		{
			name:     "wrong content type",
			method:   http.MethodPost,
			path:     "/decode",
			headers:  map[string]string{"Content-Type": "text/plain"},
			body:     body(`"hi"`),
			wantCode: http.StatusBadRequest,
			wantBody: "content-type",
		},
		{
			name:     "malformed body",
			method:   http.MethodPost,
			path:     "/decode",
			headers:  map[string]string{"Content-Type": "application/json"},
			body:     "{not json",
			wantCode: http.StatusBadRequest,
			wantBody: "failed to parse payloads",
		},
		{
			// Go's method-aware ServeMux answers a matched path used with the
			// wrong method as 405 with an Allow header, not 404. No codec-server
			// client sends anything but POST, so this divergence from sdk-go's
			// own handler (which answers 404 here) is deliberate.
			name:     "GET is not served",
			method:   http.MethodGet,
			path:     "/decode",
			wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:     "unknown route",
			method:   http.MethodPost,
			path:     "/nope",
			headers:  map[string]string{"Content-Type": "application/json"},
			body:     body(`"hi"`),
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			codecs := &fakeCodecs{}
			h := codecserver.Handler(
				codecs,
				fakeNamespaces{"payments.a8x72": "payments"},
				testReporter(t),
			)

			rec := serve(t, h, tc.method, tc.path, tc.body, tc.headers)

			require.Equal(t, tc.wantCode, rec.Code, "body: %s", rec.Body.String())
			if tc.wantBody != "" {
				require.Contains(t, rec.Body.String(), tc.wantBody)
			}
			if tc.wantData != nil {
				require.Equal(t, tc.wantData, decodedData(t, rec.Body.Bytes()))
			}
			if tc.wantNS != nil {
				require.Equal(t, tc.wantNS, codecs.namespaces)
			}
		})
	}
}

func TestHandlerPreservesPayloadCountAndOrder(t *testing.T) {
	t.Parallel()

	// The SDK's remote codec client rejects a response whose payload count does
	// not match the request's, so this is a contract, not a nicety.
	h := codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, testReporter(t))

	in := &common.Payloads{Payloads: []*common.Payload{
		{Data: []byte("first")},
		{Data: []byte("second")},
		{Data: []byte("third")},
	}}
	raw, err := protojson.Marshal(in)
	require.NoError(t, err)

	rec := serve(t, h, http.MethodPost, "/decode", string(raw),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"decoded:first", "decoded:second", "decoded:third"}, decodedData(t, rec.Body.Bytes()))
}

func TestHandlerNamespaceRequiredForEncode(t *testing.T) {
	t.Parallel()

	raw, err := protojson.Marshal(&common.Payloads{Payloads: []*common.Payload{{Data: []byte("x")}}})
	require.NoError(t, err)

	t.Run("encode without a namespace is rejected", func(t *testing.T) {
		t.Parallel()

		h := codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, testReporter(t),
			codecserver.WithNamespaceRequired(true))

		rec := serve(t, h, http.MethodPost, "/encode", string(raw),
			map[string]string{"Content-Type": "application/json"})
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "a namespace is required")
	})

	t.Run("decode without a namespace is still allowed", func(t *testing.T) {
		t.Parallel()

		h := codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, testReporter(t),
			codecserver.WithNamespaceRequired(true))

		rec := serve(t, h, http.MethodPost, "/decode", string(raw),
			map[string]string{"Content-Type": "application/json"})
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestHandlerAuth(t *testing.T) {
	t.Parallel()

	raw, err := protojson.Marshal(&common.Payloads{Payloads: []*common.Payload{{Data: []byte("x")}}})
	require.NoError(t, err)

	t.Run("a rejected token is 401 and never reaches the codecs", func(t *testing.T) {
		t.Parallel()

		codecs := &fakeCodecs{}
		h := codecserver.Handler(codecs, fakeNamespaces{}, testReporter(t),
			codecserver.WithAuth(&fakeAuth{err: errors.New("bad token")}))

		rec := serve(t, h, http.MethodPost, "/decode", string(raw),
			map[string]string{"Content-Type": "application/json"})
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Empty(t, codecs.namespaces)
	})

	t.Run("the authenticator sees the resolved local namespace", func(t *testing.T) {
		t.Parallel()

		a := &fakeAuth{}
		h := codecserver.Handler(&fakeCodecs{}, fakeNamespaces{"payments.a8x72": "payments"}, testReporter(t),
			codecserver.WithAuth(a))

		rec := serve(t, h, http.MethodPost, "/decode", string(raw), map[string]string{
			"Content-Type": "application/json",
			"X-Namespace":  "payments.a8x72",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		require.Len(t, a.targets, 1)
		require.Equal(t, "payments", a.targets[0].Namespace)
		require.Equal(t, "POST /decode", a.targets[0].FullName)
	})

	t.Run("a rejected token is 401 on download too", func(t *testing.T) {
		t.Parallel()

		h := codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, testReporter(t),
			codecserver.WithAuth(&fakeAuth{err: errors.New("bad token")}))

		rec := serve(t, h, http.MethodPost, "/download", string(raw),
			map[string]string{"Content-Type": "application/json"})
		require.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("a rejected token is 401 even with a malformed body", func(t *testing.T) {
		t.Parallel()

		// A body this handler cannot parse would 400 on its own, so this only
		// means something if auth ran first: it pins that auth runs before body
		// parsing, not just before the codec call.
		h := codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, testReporter(t),
			codecserver.WithAuth(&fakeAuth{err: errors.New("bad token")}))

		rec := serve(t, h, http.MethodPost, "/decode", "{not json",
			map[string]string{"Content-Type": "application/json"})
		require.Equal(t, http.StatusUnauthorized, rec.Code)
	})
}

func TestHandlerCORS(t *testing.T) {
	t.Parallel()

	newHandler := func(t *testing.T) http.Handler {
		t.Helper()

		return codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, testReporter(t),
			codecserver.WithCORS([]string{"https://cloud.temporal.io"}, true),
			// A preflight carries no credentials, so it must be answered ahead of auth.
			codecserver.WithAuth(&fakeAuth{err: errors.New("bad token")}))
	}

	t.Run("preflight is answered before auth", func(t *testing.T) {
		t.Parallel()

		rec := serve(t, newHandler(t), http.MethodOptions, "/decode", "", map[string]string{
			"Origin":                         "https://cloud.temporal.io",
			"Access-Control-Request-Method":  "POST",
			"Access-Control-Request-Headers": "authorization,authorization-extras,x-namespace",
		})

		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Equal(t, "https://cloud.temporal.io", rec.Header().Get("Access-Control-Allow-Origin"))
		require.Equal(t, "true", rec.Header().Get("Access-Control-Allow-Credentials"))
		require.Contains(t, rec.Header().Get("Access-Control-Allow-Methods"), "POST")

		// The UI sends an ID token in Authorization-Extras whenever one is present;
		// omitting it here fails the preflight in the browser.
		allow := rec.Header().Get("Access-Control-Allow-Headers")
		for _, want := range []string{"Content-Type", "X-Namespace", "Authorization", "Authorization-Extras"} {
			require.Contains(t, allow, want)
		}
	})

	t.Run("a disallowed origin gets no allow-origin header but the response still varies by origin", func(t *testing.T) {
		t.Parallel()

		rec := serve(t, newHandler(t), http.MethodOptions, "/decode", "",
			map[string]string{"Origin": "https://evil.test"})
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))

		// Vary: Origin is set unconditionally, not only for an allowed origin, so
		// a shared cache never replays this header-less response to a request
		// from an allowed origin. Its presence here, on a handler with CORS
		// configured, also pins that the middleware ran rather than being absent.
		require.Equal(t, "Origin", rec.Header().Get("Vary"))
	})

	t.Run("a successful POST from an allowed origin carries the allow-origin header", func(t *testing.T) {
		t.Parallel()

		h := codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, testReporter(t),
			codecserver.WithCORS([]string{"https://cloud.temporal.io"}, true))

		raw, err := protojson.Marshal(&common.Payloads{Payloads: []*common.Payload{{Data: []byte("x")}}})
		require.NoError(t, err)

		rec := serve(t, h, http.MethodPost, "/decode", string(raw), map[string]string{
			"Content-Type": "application/json",
			"Origin":       "https://cloud.temporal.io",
		})

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "https://cloud.temporal.io", rec.Header().Get("Access-Control-Allow-Origin"))
	})
}

func TestHandlerRejectsOversizedBody(t *testing.T) {
	t.Parallel()

	// A body of arbitrary bytes, like strings.Repeat("a", 512), is not valid
	// JSON: it 400s on the parse path regardless of the body cap, which leaves
	// the cap itself untested. Both cases here send valid protojson so a
	// passing "over the cap" case can only mean the cap did its job, and the
	// "under the cap" companion pins the boundary from the other side.
	const limit int64 = 64

	payload := func(t *testing.T, dataLen int) string {
		t.Helper()

		raw, err := protojson.Marshal(&common.Payloads{
			Payloads: []*common.Payload{{Data: []byte(strings.Repeat("a", dataLen))}},
		})
		require.NoError(t, err)

		return string(raw)
	}

	t.Run("a valid body over the cap is rejected for exceeding it", func(t *testing.T) {
		t.Parallel()

		h := codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, testReporter(t),
			codecserver.WithMaxBodyBytes(limit))

		body := payload(t, 256)
		require.Greater(t, int64(len(body)), limit, "fixture must exceed the cap for this case to mean anything")

		rec := serve(t, h, http.MethodPost, "/decode", body,
			map[string]string{"Content-Type": "application/json"})
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "failed to read request body")
	})

	t.Run("a valid body under the cap is served", func(t *testing.T) {
		t.Parallel()

		h := codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, testReporter(t),
			codecserver.WithMaxBodyBytes(limit))

		body := payload(t, 2)
		require.LessOrEqual(t, int64(len(body)), limit, "fixture must stay under the cap for this case to mean anything")

		rec := serve(t, h, http.MethodPost, "/decode", body,
			map[string]string{"Content-Type": "application/json"})
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestHandlerRequiresReporter(t *testing.T) {
	t.Parallel()

	// A nil reporter would otherwise panic inside the deferred metrics call on
	// the first request, which surfaces as a 500 and contradicts the all-4xx
	// property; failing at construction catches the wiring error instead.
	require.PanicsWithValue(t, "codecserver: Handler requires a non-nil reporter", func() {
		codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, nil)
	})
}

func TestHandlerCodecFailureHidesDetail(t *testing.T) {
	t.Parallel()

	// A codec failure can carry a KMS key ARN, ciphertext metadata, or a vault
	// message; none of that may reach the response body.
	sensitive := "kms: AccessDenied unwrapping arn:aws:kms:us-east-1:111122223333:key/abcd-ef01"
	codecs := &fakeCodecs{err: errors.New(sensitive)}
	h := codecserver.Handler(codecs, fakeNamespaces{}, testReporter(t))

	raw, err := protojson.Marshal(&common.Payloads{Payloads: []*common.Payload{{Data: []byte("x")}}})
	require.NoError(t, err)

	rec := serve(t, h, http.MethodPost, "/decode", string(raw),
		map[string]string{"Content-Type": "application/json"})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "failed to transform payloads")
	require.NotContains(t, rec.Body.String(), "kms")
	require.NotContains(t, rec.Body.String(), "arn:aws")
}

// TestHandlerReportsRouteAsThePatternNotThePath drives a real Handler wired to
// a real Prometheus registry, rather than calling Reporter.Request directly,
// so it catches a route label carrying the request path instead of the
// matched pattern. A Namespace-prefixed path is unbounded cardinality in a
// label value, which is exactly what the route label is designed to avoid.
func TestHandlerReportsRouteAsThePatternNotThePath(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	r := codecserver.NewReporter(metrics.New("test", promauto.With(reg)).ForSubsystem("codec_server"))
	h := codecserver.Handler(&fakeCodecs{}, fakeNamespaces{}, r)

	raw, err := protojson.Marshal(&common.Payloads{Payloads: []*common.Payload{{Data: []byte("x")}}})
	require.NoError(t, err)

	rec := serve(t, h, http.MethodPost, "/payments.a8x72/decode", string(raw),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, rec.Code)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	var routeValues []string
	for _, mf := range mfs {
		if mf.GetName() != "test_codec_server_requests_total" {
			continue
		}

		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "route" {
					routeValues = append(routeValues, l.GetValue())
				}
			}
		}
	}

	require.Equal(t, []string{"/decode"}, routeValues)
}

func serve(
	t *testing.T,
	h http.Handler,
	method, path, body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), method, path, bytes.NewBufferString(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	return rec
}

func testReporter(t *testing.T) *codecserver.Reporter {
	t.Helper()

	return codecserver.NewReporter(
		metrics.New("test", promauto.With(prometheus.NewRegistry())).ForSubsystem("codec_server"),
	)
}

// decodedData unmarshals a protojson-encoded Payloads response, which is the
// only correct way to inspect it: its whitespace is unstable and an empty list
// marshals to "{}" rather than "{"payloads":[]}".
func decodedData(t *testing.T, raw []byte) []string {
	t.Helper()

	var out common.Payloads
	require.NoError(t, protojson.Unmarshal(raw, &out))

	data := make([]string, len(out.Payloads))
	for i, p := range out.Payloads {
		data[i] = string(p.Data)
	}

	return data
}
