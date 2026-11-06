package proxy_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/proxy"
)

// The codecs themselves are covered where they live: encryption and compression
// through this interceptor in encryption_test.go and compression_test.go, and
// chain ordering in pkg/codec. What is left to pin here is what the interceptor
// does when no codec is configured, and that it opens Standalone Nexus
// Operation results.

func TestCodecInterceptorRejectsIncompleteOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts proxy.CodecOptions
		want string
	}{
		{
			name: "encrypting without a vault",
			opts: proxy.CodecOptions{DecodeLimiter: testLimiter(), ResponseLimit: testResponseLimit, Encrypt: true},
			want: "encryption requires a vault",
		},
		{
			name: "a vault without a reporter",
			opts: proxy.CodecOptions{DecodeLimiter: testLimiter(), ResponseLimit: testResponseLimit, Vault: &fakeVault{}},
			want: "a vault requires a reporter",
		},
		{
			name: "encoding failures without encryption",
			opts: proxy.CodecOptions{DecodeLimiter: testLimiter(), ResponseLimit: testResponseLimit, Vault: &fakeVault{}, EncodeFailures: true, Reporter: newTestReporter(t)},
			want: "failure encoding requires encryption",
		},
		{
			name: "compression without a reporter",
			opts: proxy.CodecOptions{DecodeLimiter: testLimiter(), ResponseLimit: testResponseLimit, Compression: &config.Compression{}},
			want: "compression requires a compression reporter",
		},
		{
			name: "no decode limiter",
			opts: proxy.CodecOptions{ResponseLimit: testResponseLimit},
			want: "a decode limiter is required",
		},
		{
			name: "no response limit",
			opts: proxy.CodecOptions{DecodeLimiter: testLimiter()},
			want: "response limit must be positive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := proxy.CodecInterceptor(tc.opts)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// A payload can arrive marked as encrypted even when this proxy has no vault,
// because something upstream of it may have sealed it. With no vault there is no
// decoder, so it has to travel through untouched rather than be handed to one.
func TestCodecInterceptorWithoutAVaultIgnoresSealedPayloads(t *testing.T) {
	t.Parallel()

	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{DecodeLimiter: testLimiter(), ResponseLimit: testResponseLimit})
	require.NoError(t, err)

	sealed := sealedPayload(t, testPayload("json/plain", `"secret"`))
	resp := &workflowservice.StartWorkflowExecutionRequest{}

	require.NoError(t, interceptor(t.Context(), "/svc/Start", startRequest(), resp, nil, respondWith(sealed)))

	require.Len(t, resp.Input.Payloads, 1)
	require.Same(t, sealed, resp.Input.Payloads[0])
}

func TestCodecInterceptorWithoutCodecs(t *testing.T) {
	t.Parallel()

	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{DecodeLimiter: testLimiter(), ResponseLimit: testResponseLimit})
	require.NoError(t, err)

	out := testPayload("json/plain", `"out"`)
	in := testPayload("json/plain", `"in"`)

	req := startRequest(out)
	resp := &workflowservice.StartWorkflowExecutionRequest{}

	var sent *common.Payload
	invoker := func(_ context.Context, _ string, gotReq, gotResp any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		sent = gotReq.(*workflowservice.StartWorkflowExecutionRequest).Input.Payloads[0]
		gotResp.(*workflowservice.StartWorkflowExecutionRequest).Input = &common.Payloads{
			Payloads: []*common.Payload{in},
		}

		return nil
	}

	require.NoError(t, interceptor(t.Context(), "/svc/Start", req, resp, nil, invoker))

	// Outbound has no codec and inbound only decompresses, which leaves anything
	// uncompressed alone, so both sides travel by reference.
	require.Same(t, out, sent)
	require.Same(t, in, resp.Input.Payloads[0])
}

// TestCodecInterceptorOpensNexusOperationResults pins that a Standalone Nexus
// Operation's result comes back opened on the concurrent path. Before
// go.temporal.io/api assigned a oneof payload only once its visit completed
// (temporalio/api-go#324), the concurrent visitor wrote these results back
// before the codec opened them, so the caller got the sealed result and -race
// reported the write.
func TestCodecInterceptorOpensNexusOperationResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		reply  func(result, input *common.Payload) proto.Message
		result func(proto.Message) *common.Payload
		input  func(proto.Message) *common.Payload
	}{
		{
			name: "describe",
			reply: func(result, input *common.Payload) proto.Message {
				return &workflowservice.DescribeNexusOperationExecutionResponse{
					Input:   input,
					Outcome: &workflowservice.DescribeNexusOperationExecutionResponse_Result{Result: result},
				}
			},
			result: func(m proto.Message) *common.Payload {
				return m.(*workflowservice.DescribeNexusOperationExecutionResponse).GetResult()
			},
			input: func(m proto.Message) *common.Payload {
				return m.(*workflowservice.DescribeNexusOperationExecutionResponse).GetInput()
			},
		},
		{
			name: "poll",
			reply: func(result, _ *common.Payload) proto.Message {
				return &workflowservice.PollNexusOperationExecutionResponse{
					Outcome: &workflowservice.PollNexusOperationExecutionResponse_Result{Result: result},
				}
			},
			result: func(m proto.Message) *common.Payload {
				return m.(*workflowservice.PollNexusOperationExecutionResponse).GetResult()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{
				DecodeLimiter: testLimiter(),
				ResponseLimit: testResponseLimit,
				Vault:         &fakeVault{},
				Reporter:      newTestReporter(t),
			})
			require.NoError(t, err)

			result := testPayload("json/plain", `"result"`)
			input := testPayload("json/plain", `"input"`)
			reply := tc.reply(nil, nil).ProtoReflect().Type().New().Interface()
			invoker := func(_ context.Context, _ string, _, got any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				proto.Merge(got.(proto.Message), tc.reply(sealedPayload(t, result), sealedPayload(t, input)))
				return nil
			}

			require.NoError(t, interceptor(t.Context(), "/svc/Nexus", startRequest(), reply, nil, invoker))
			require.True(t, proto.Equal(result, tc.result(reply)), "result still sealed: %v", tc.result(reply))

			if tc.input != nil {
				require.True(t, proto.Equal(input, tc.input(reply)), "input still sealed: %v", tc.input(reply))
			}
		})
	}
}
