package proxy_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/proxy"
)

// The codecs themselves are covered where they live: encryption through this
// interceptor in encryption_test.go, and chain ordering in pkg/codec. What is left
// to pin here is what the interceptor does when a direction has no codec at all.

func TestCodecInterceptorRejectsIncompleteOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts proxy.CodecOptions
		want string
	}{
		{
			name: "encrypting without a vault",
			opts: proxy.CodecOptions{Encrypt: true},
			want: "encryption requires a vault",
		},
		{
			name: "a vault without a reporter",
			opts: proxy.CodecOptions{Vault: &fakeVault{}},
			want: "a vault requires a reporter",
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

	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{})
	require.NoError(t, err)

	sealed := sealedPayload(t, testPayload("json/plain", `"secret"`))
	resp := &workflowservice.StartWorkflowExecutionRequest{}

	require.NoError(t, interceptor(t.Context(), "/svc/Start", startRequest(), resp, nil, respondWith(sealed)))

	require.Len(t, resp.Input.Payloads, 1)
	require.Same(t, sealed, resp.Input.Payloads[0])
}

func TestCodecInterceptorWithoutCodecs(t *testing.T) {
	t.Parallel()

	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{})
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

	// With no codec in either direction the payloads are not even rebuilt, so both
	// sides travel by reference.
	require.Same(t, out, sent)
	require.Same(t, in, resp.Input.Payloads[0])
}
