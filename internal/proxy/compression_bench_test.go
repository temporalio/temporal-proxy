package proxy_test

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/pkg/codec"
	"go.temporal.io/proxy/pkg/compress"
)

// BenchmarkCodecInterceptorDecodeHistory measures what always decompressing
// costs a large history page with no vault, the case where compression decode
// is the only inbound work. "direct" is the invoker alone, so the difference to
// "uncompressed" is the walk every response now pays. The "disabled" and
// "enabled" cases decode the same compressed page one payload at a time and
// concurrently, which is what turning compression on changes.
func BenchmarkCodecInterceptorDecodeHistory(b *testing.B) {
	const events = 1000

	lz4, err := compress.NewLZ4()
	require.NoError(b, err)

	disabled, err := proxy.CodecInterceptor(proxy.CodecOptions{})
	require.NoError(b, err)

	cfg := &config.Config{
		Listen:    config.ListenConfig{HostPort: ":8080"},
		Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}},
		Compression: config.Compression{
			Enabled: true,
			Default: &config.CompressionPolicy{Algorithm: config.CompressionLZ4},
		},
	}
	require.NoError(b, cfg.Prepare())

	enabled, err := proxy.CodecInterceptor(proxy.CodecOptions{
		Compression:         &cfg.Compression,
		CompressionReporter: newCompressionReporter(prometheus.NewRegistry()),
	})
	require.NoError(b, err)

	for _, bc := range []struct {
		name        string
		compression codec.Compression
		interceptor grpc.UnaryClientInterceptor
	}{
		{name: "direct"},
		{name: "uncompressed", interceptor: disabled},
		{name: "compressed/disabled", compression: lz4, interceptor: disabled},
		{name: "compressed/enabled", compression: lz4, interceptor: enabled},
	} {
		b.Run(bc.name, func(b *testing.B) {
			page := historyPage(b, events, bc.compression)
			invoker := func(_ context.Context, _ string, _, reply any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				proto.Merge(reply.(*workflowservice.GetWorkflowExecutionHistoryResponse), page)
				return nil
			}

			req := &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: "orders"}

			b.ReportAllocs()
			for b.Loop() {
				resp := &workflowservice.GetWorkflowExecutionHistoryResponse{}
				if bc.interceptor == nil {
					require.NoError(b, invoker(b.Context(), "/m", req, resp, nil))
					continue
				}

				require.NoError(b, bc.interceptor(b.Context(), "/m", req, resp, nil, invoker))
			}
		})
	}
}

// historyPage returns a page of n activity completions, each with a payload
// compressed by c, or left as is when c is nil.
func historyPage(b *testing.B, n int, c codec.Compression) *workflowservice.GetWorkflowExecutionHistoryResponse {
	b.Helper()

	cmp := codec.NewCompressor(c, codec.WithMinBytes(0))
	page := &history.History{Events: make([]*history.HistoryEvent, n)}
	for i := range page.Events {
		payloads, err := cmp.Encode([]*common.Payload{compressiblePayload()})
		require.NoError(b, err)

		page.Events[i] = &history.HistoryEvent{
			EventId: int64(i + 1),
			Attributes: &history.HistoryEvent_ActivityTaskCompletedEventAttributes{
				ActivityTaskCompletedEventAttributes: &history.ActivityTaskCompletedEventAttributes{
					Result: &common.Payloads{Payloads: payloads},
				},
			},
		}
	}

	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: page}
}
