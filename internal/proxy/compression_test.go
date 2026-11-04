package proxy_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/config"
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

	cmp := preparedCompression(t, config.Compression{
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

			interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{Compression: cmp})
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

	zstd := preparedCompression(t, config.Compression{
		Enabled: true,
		Default: &config.CompressionPolicy{Algorithm: config.CompressionZstd},
	})

	tests := []struct {
		name string
		opts proxy.CodecOptions
		with codec.Compression
	}{
		{name: "no compression block", opts: proxy.CodecOptions{}, with: must(t, compress.NewLZ4)},
		{name: "compression disabled", opts: proxy.CodecOptions{Compression: preparedCompression(t, config.Compression{})}, with: must(t, compress.NewSnappy)},
		{name: "another algorithm", opts: proxy.CodecOptions{Compression: zstd}, with: must(t, compress.NewLZ4)},
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

// TestCodecInterceptorCompressesBeforeSealing shows a payload is compressed and
// then sealed on the way out, and opened and then restored on the way back,
// since ciphertext does not compress. Swapping the two in the chain fails the
// check on what the vault was handed.
func TestCodecInterceptorCompressesBeforeSealing(t *testing.T) {
	t.Parallel()

	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{
		Vault:    &fakeVault{},
		Encrypt:  true,
		Reporter: newTestReporter(t),
		Compression: preparedCompression(t, config.Compression{
			Enabled: true,
			Default: &config.CompressionPolicy{Algorithm: config.CompressionSnappy},
		}),
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

// preparedCompression returns c as it is once its config is prepared, which is
// how [proxy.CodecOptions] needs it.
func preparedCompression(t *testing.T, c config.Compression) *config.Compression {
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
