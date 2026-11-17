package proxy_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/internal/transport/meta"
	"go.temporal.io/proxy/pkg/codec"
	"go.temporal.io/proxy/pkg/compress"
)

// TestCodecInterceptorCompressesPerNamespace shows outbound payloads follow the
// settings of the request's namespace. Looking up the settings by anything but
// the request's namespace fails the staging and precompressed cases.
func TestCodecInterceptorCompressesPerNamespace(t *testing.T) {
	t.Parallel()

	opts := compressionOpts(t, config.Compression{
		Enabled: true,
		Default: &config.CompressionPolicy{Algorithm: config.CompressionZstd, MinBytes: new(64)},
		Overrides: map[string]config.CompressionPolicy{
			"staging":       {Algorithm: config.CompressionLZ4},
			"precompressed": {Enabled: new(false)},
		},
	})

	tests := []struct {
		name    string
		ns      string
		payload *common.Payload
		want    string // the algorithm the sent payload names, or "" when sent as is
	}{
		{name: "default", ns: "orders", payload: compressiblePayload(), want: "zstd"},
		{name: "override algorithm", ns: "staging", payload: compressiblePayload(), want: "lz4"},
		{name: "override opts out", ns: "precompressed", payload: compressiblePayload()},
		{name: "under minBytes", ns: "orders", payload: testPayload("json/plain", `"small"`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			interceptor, err := proxy.CodecInterceptor(opts)
			require.NoError(t, err)

			var sent *common.Payload
			invoker := func(_ context.Context, _ string, req, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				sent = req.(*workflowservice.StartWorkflowExecutionRequest).Input.Payloads[0]
				return nil
			}

			ctx := meta.WithNamespace(t.Context(), tc.ns)
			resp := &workflowservice.StartWorkflowExecutionRequest{}
			require.NoError(t, interceptor(ctx, "/svc/Start", startRequest(tc.payload), resp, nil, invoker))

			if tc.want == "" {
				require.Same(t, tc.payload, sent)
				return
			}

			require.Equal(t, codec.CompressionEncoding, string(sent.Metadata[codec.MetadataEncoding]))
			require.Equal(t, tc.want, string(sent.Metadata[codec.MetadataCompressionAlgorithm]))
		})
	}
}

// TestCodecInterceptorAlwaysDecompresses shows inbound payloads are restored
// whatever compression is configured, including none at all, so turning it off
// or switching algorithms strands nothing written earlier. Building the inbound
// chain without the shared decoder set fails every case.
func TestCodecInterceptorAlwaysDecompresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts proxy.CodecOptions
		with codec.Compression
	}{
		{name: "no compression block", opts: proxy.CodecOptions{DecodeLimiter: testLimiter(), ResponseLimit: testResponseLimit}, with: must(t, compress.NewLZ4)},
		{
			name: "compression disabled",
			opts: compressionOpts(t, config.Compression{}),
			with: must(t, compress.NewSnappy),
		},
		{
			name: "another algorithm",
			opts: compressionOpts(t, config.Compression{
				Enabled: true,
				Default: &config.CompressionPolicy{Algorithm: config.CompressionZstd},
			}),
			with: must(t, compress.NewLZ4),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			original := compressiblePayload()
			compressed, err := codec.NewCompressor(tc.with).Encode([]*common.Payload{original})
			require.NoError(t, err)
			require.Equal(t, codec.CompressionEncoding, string(compressed[0].Metadata[codec.MetadataEncoding]))

			interceptor, err := proxy.CodecInterceptor(tc.opts)
			require.NoError(t, err)

			resp := &workflowservice.StartWorkflowExecutionRequest{}
			require.NoError(t, interceptor(t.Context(), "/svc/Start", startRequest(), resp, nil, respondWith(compressed...)))

			require.Len(t, resp.Input.Payloads, 1)
			require.True(t, proto.Equal(original, resp.Input.Payloads[0]))
		})
	}
}

// TestCodecsDecodeUncompressedAllocations shows that, without a vault, payloads
// with nothing compressed among them pass through without building a chain,
// the cost every response pays now that inbound always decompresses. Building
// the chain before checking for a compressed payload fails it. AllocsPerRun
// counts every allocation in the process, so it can't run in parallel.
func TestCodecsDecodeUncompressedAllocations(t *testing.T) {
	tests := []struct {
		name string
		c    config.Compression
	}{
		{name: "compression disabled", c: config.Compression{}},
		{
			name: "compression enabled",
			c: config.Compression{
				Enabled: true,
				Default: &config.CompressionPolicy{Algorithm: config.CompressionZstd},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := proxy.NewCodecs(compressionOpts(t, tc.c))
			require.NoError(t, err)

			in := []*common.Payload{testPayload("json/plain", `"a"`), testPayload("json/plain", `"b"`)}
			allocs := testing.AllocsPerRun(100, func() {
				_, _ = c.Decode(t.Context(), "orders", "", in)
			})
			require.Zero(t, allocs)
		})
	}
}

// TestCodecInterceptorCompressesBeforeSealing shows a payload is compressed and
// then sealed on the way out, and opened and then restored on the way back,
// since ciphertext does not compress. Swapping the two in the chain fails the
// check on what the vault was handed.
func TestCodecInterceptorCompressesBeforeSealing(t *testing.T) {
	t.Parallel()

	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{
		DecodeLimiter: testLimiter(),
		ResponseLimit: testResponseLimit,
		Vault:         &fakeVault{},
		Encrypt:       true,
		Reporter:      newTestReporter(t),
		Compression: preparedCompression(t, config.Compression{
			Enabled: true,
			Default: &config.CompressionPolicy{Algorithm: config.CompressionSnappy},
		}),
		CompressionReporter: newCompressionReporter(prometheus.NewRegistry()),
	})
	require.NoError(t, err)

	original := compressiblePayload()
	invoker := func(_ context.Context, _ string, req, resp any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		sent := req.(*workflowservice.StartWorkflowExecutionRequest).Input.Payloads
		require.Equal(t, codec.EncryptionEncoding, string(sent[0].Metadata[codec.MetadataEncoding]))

		// fakeVault seals by prefixing, so what follows is the payload it was handed.
		var sealed common.Payload
		require.NoError(t, sealed.Unmarshal(bytes.TrimPrefix(sent[0].Data, []byte(sealPrefix))))
		require.Equal(t, codec.CompressionEncoding, string(sealed.Metadata[codec.MetadataEncoding]))

		resp.(*workflowservice.StartWorkflowExecutionRequest).Input = &common.Payloads{Payloads: sent}
		return nil
	}

	ctx := meta.WithNamespace(t.Context(), "orders")
	resp := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, interceptor(ctx, "/svc/Start", startRequest(proto.CloneOf(original)), resp, nil, invoker))

	require.Len(t, resp.Input.Payloads, 1)
	require.True(t, proto.Equal(original, resp.Input.Payloads[0]))
}

// TestCodecInterceptorReportsCompression shows one request's payloads are
// recorded under its local namespace: the op, both sizes of what compressed, and
// the reason for what didn't. Reporting the remote name, or recording sizes on a
// skipped payload, fails it.
func TestCodecInterceptorReportsCompression(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{
		DecodeLimiter: testLimiter(),
		ResponseLimit: testResponseLimit,
		Compression: preparedCompression(t, config.Compression{
			Enabled: true,
			Default: &config.CompressionPolicy{Algorithm: config.CompressionZstd, MinBytes: new(64)},
		}),
		CompressionReporter: newCompressionReporter(reg),
	}, proxy.WithRemoteNamespace(func(ns string) string { return ns + ".a1b2c" }))
	require.NoError(t, err)

	ctx := meta.WithNamespace(t.Context(), "orders")
	req := startRequest(compressiblePayload(), testPayload("json/plain", `"small"`))
	require.NoError(t, interceptor(ctx, "/svc/Start", req, &workflowservice.StartWorkflowExecutionRequest{}, nil, respondWith()))

	ops := gatherFamily(t, reg, "test_compression_ops_total")
	require.NotNil(t, ops)
	require.Len(t, ops.GetMetric(), 1)
	require.True(t, hasLabels(ops, map[string]string{
		"operation": "compress", "algorithm": "zstd", "result": "success", "namespace": "orders",
	}))

	bytes := gatherFamily(t, reg, "test_compression_payload_bytes")
	require.NotNil(t, bytes)
	require.Len(t, bytes.GetMetric(), 2)
	for _, m := range bytes.GetMetric() {
		require.Equal(t, uint64(1), m.GetHistogram().GetSampleCount())
	}

	skipped := gatherFamily(t, reg, "test_compression_payloads_skipped_total")
	require.NotNil(t, skipped)
	require.True(t, hasLabels(skipped, map[string]string{
		"reason": "below_min_bytes", "algorithm": "zstd", "namespace": "orders",
	}))
}

// TestCodecInterceptorReportsDecodeFailures shows a payload the proxy can't
// restore is recorded with a bounded algorithm label and a result naming why.
// The algorithm name comes from whoever wrote the payload, so reporting it as is
// fails the unknown algorithm case.
func TestCodecInterceptorReportsDecodeFailures(t *testing.T) {
	t.Parallel()

	// The cap is on decoding only, so a compressor allowed more writes a payload
	// the proxy's default decoder refuses.
	big, err := compress.NewZstd(3, compress.WithMaxDecodedSize(8<<20))
	require.NoError(t, err)

	tooLarge, err := codec.NewCompressor(big).Encode([]*common.Payload{
		testPayload("binary/plain", strings.Repeat("0", 5<<20)),
	})
	require.NoError(t, err)

	unknown := &common.Payload{
		Metadata: map[string][]byte{
			codec.MetadataEncoding:             []byte(codec.CompressionEncoding),
			codec.MetadataCompressionAlgorithm: []byte("brotli-1b2e"),
		},
		Data: []byte("compressed"),
	}

	tests := []struct {
		name    string
		payload *common.Payload
		want    map[string]string
	}{
		{
			name:    "past the decode cap",
			payload: tooLarge[0],
			want:    map[string]string{"algorithm": "zstd", "result": "too_large"},
		},
		{
			name:    "unknown algorithm",
			payload: unknown,
			want:    map[string]string{"algorithm": "unknown", "result": "unknown_algorithm"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reg := prometheus.NewRegistry()
			interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{
				DecodeLimiter:       testLimiter(),
				ResponseLimit:       testResponseLimit,
				Compression:         preparedCompression(t, config.Compression{}),
				CompressionReporter: newCompressionReporter(reg),
			})
			require.NoError(t, err)

			ctx := meta.WithNamespace(t.Context(), "orders")
			resp := &workflowservice.StartWorkflowExecutionRequest{}
			require.Error(t, interceptor(ctx, "/svc/Start", startRequest(), resp, nil, respondWith(tc.payload)))

			ops := gatherFamily(t, reg, "test_compression_ops_total")
			require.NotNil(t, ops)
			tc.want["operation"], tc.want["namespace"] = "decompress", "orders"
			require.True(t, hasLabels(ops, tc.want), "got %v", ops.GetMetric())
		})
	}
}

// TestCodecInterceptorDecodeLimits shows a response of many payloads, each far
// under the per-payload cap, is refused with ResourceExhausted once their
// decoded total passes the response limit, and that what a response decoded is
// returned to the pool once the interceptor is done. Charging per batch
// instead of per response is not caught here (one batch per response), which
// is why the limiter has its own tests; never releasing fails "repeated".
func TestCodecInterceptorDecodeLimits(t *testing.T) {
	t.Parallel()

	const (
		limit = 64 << 10 // 64KiB
		each  = 8 << 10  // 8KiB decoded
	)

	tests := []struct {
		name     string
		payloads int
		calls    int
		pool     int64
		wantCode codes.Code
	}{
		{name: "within the limit", payloads: 4, calls: 1, pool: limit, wantCode: codes.OK},
		{name: "past the response limit", payloads: 16, calls: 1, pool: 1 << 30, wantCode: codes.ResourceExhausted},
		{name: "past the pool", payloads: 4, calls: 1, pool: each, wantCode: codes.ResourceExhausted},
		{name: "repeated", payloads: 4, calls: 4, pool: 5 * each, wantCode: codes.OK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := compressionOpts(t, config.Compression{})
			opts.DecodeLimiter = proxy.NewDecodeLimiter(tc.pool)
			opts.ResponseLimit = limit

			interceptor, err := proxy.CodecInterceptor(opts)
			require.NoError(t, err)

			payloads := bombs(t, tc.payloads, each)
			for range tc.calls {
				resp := &workflowservice.StartWorkflowExecutionRequest{}
				err = interceptor(t.Context(), "/svc/Start", startRequest(), resp, nil, respondWith(payloads...))
				require.Equal(t, tc.wantCode, status.Code(err), "err: %v", err)
			}
		})
	}
}

// TestCodecsDecodeLimit shows the codec server's path refuses payloads whose
// decoded total passes the response limit, with ResourceExhausted, though each
// is far under the per-payload cap. Opening no budget fails it.
func TestCodecsDecodeLimit(t *testing.T) {
	t.Parallel()

	opts := compressionOpts(t, config.Compression{})
	opts.ResponseLimit = 64 << 10

	c, err := proxy.NewCodecs(opts)
	require.NoError(t, err)

	_, err = c.Decode(t.Context(), "orders", "", bombs(t, 4, 8<<10))
	require.NoError(t, err)

	_, err = c.Decode(t.Context(), "orders", "", bombs(t, 16, 8<<10))
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "err: %v", err)
}

// bombs returns n snappy payloads that each decode to about size bytes from
// a few dozen stored.
func bombs(t *testing.T, n, size int) []*common.Payload {
	t.Helper()

	out, err := codec.NewCompressor(must(t, compress.NewSnappy)).Encode([]*common.Payload{
		testPayload("binary/plain", strings.Repeat("0", size)),
	})
	require.NoError(t, err)
	require.Less(t, out[0].Size(), size/10)

	res := make([]*common.Payload, n)
	for i := range res {
		res[i] = out[0]
	}

	return res
}

// compressionOpts returns options that compress as c says, recording to a
// registry of their own.
func compressionOpts(t testing.TB, c config.Compression) proxy.CodecOptions {
	t.Helper()

	return proxy.CodecOptions{
		DecodeLimiter:       testLimiter(),
		ResponseLimit:       testResponseLimit,
		Compression:         preparedCompression(t, c),
		CompressionReporter: newCompressionReporter(prometheus.NewRegistry()),
	}
}

// newCompressionReporter builds a CompressionReporter over reg with namespace
// labels on, so tests can read the namespace back.
func newCompressionReporter(reg *prometheus.Registry) *proxy.CompressionReporter {
	return proxy.NewCompressionReporter(
		metrics.New("test", promauto.With(reg)).ForSubsystem("compression"),
		proxy.WithNamespaceLabels(true),
	)
}

// preparedCompression returns c as it is once its config is prepared, which is
// how [proxy.CodecOptions] needs it.
func preparedCompression(t testing.TB, c config.Compression) *config.Compression {
	t.Helper()

	cfg := &config.Config{
		Listen:      config.ListenConfig{HostPort: ":8080"},
		Upstreams:   []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}},
		Compression: c,
	}
	require.NoError(t, cfg.Prepare())

	return &cfg.Compression
}

// must builds a compressor with fn and its default options.
func must[T any](t *testing.T, fn func(...compress.Option) (T, error)) T {
	t.Helper()

	v, err := fn()
	require.NoError(t, err)

	return v
}

// compressiblePayload returns a payload well past any test's minBytes that
// every algorithm shrinks.
func compressiblePayload() *common.Payload {
	return testPayload("json/plain", `["`+strings.Repeat("compressible ", 100)+`"]`)
}
