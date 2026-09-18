package proxy_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

// TestCodecsMatchInterceptor is the guarantee behind reusing the proxy's codec
// chain outside gRPC: both entry points must build the same chain and produce
// byte-identical output. A codec added to one path and not the other fails here.
func TestCodecsMatchInterceptor(t *testing.T) {
	t.Parallel()

	const ns = "local"

	original := []*common.Payload{
		testPayload("json/plain", `"first"`),
		testPayload("json/plain", `"second"`),
	}

	t.Run("encode", func(t *testing.T) {
		t.Parallel()

		// Path A: the interceptor seals on the way out, so read what the invoker saw.
		codecs, err := proxy.NewCodecs(codecOpts(t))
		require.NoError(t, err)

		interceptor, err := codecs.Interceptor()
		require.NoError(t, err)

		var viaInterceptor []*common.Payload
		invoker := func(_ context.Context, _ string, gotReq, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			viaInterceptor = gotReq.(*workflowservice.StartWorkflowExecutionRequest).Input.Payloads
			return nil
		}

		ctx := meta.WithNamespace(t.Context(), ns)
		require.NoError(t, interceptor(
			ctx, "/m", startRequest(clone(original)...),
			&workflowservice.StartWorkflowExecutionRequest{}, nil, invoker,
		))

		// Path B: the direct call.
		direct, err := proxy.NewCodecs(codecOpts(t))
		require.NoError(t, err)

		viaDirect, err := direct.Encode(ctx, ns, clone(original))
		require.NoError(t, err)

		requirePayloadsEqual(t, viaInterceptor, viaDirect)
	})

	t.Run("decode", func(t *testing.T) {
		t.Parallel()

		sealed := []*common.Payload{
			sealedPayload(t, original[0]),
			sealedPayload(t, original[1]),
		}

		// Path A: the interceptor opens on the way in, so read what the caller saw.
		codecs, err := proxy.NewCodecs(proxy.CodecOptions{Vault: &fakeVault{}, Reporter: newTestReporter(t)})
		require.NoError(t, err)

		interceptor, err := codecs.Interceptor()
		require.NoError(t, err)

		resp := &workflowservice.StartWorkflowExecutionRequest{}
		ctx := meta.WithNamespace(t.Context(), ns)
		require.NoError(t, interceptor(
			ctx, "/m", startRequest(), resp, nil, respondWith(clone(sealed)...),
		))
		viaInterceptor := resp.Input.Payloads

		// Path B: the direct call, with its own vault so the two paths cannot share
		// vault state and accidentally agree.
		direct, err := proxy.NewCodecs(proxy.CodecOptions{Vault: &fakeVault{}, Reporter: newTestReporter(t)})
		require.NoError(t, err)

		viaDirect, err := direct.Decode(ctx, ns, clone(sealed))
		require.NoError(t, err)

		requirePayloadsEqual(t, viaInterceptor, viaDirect)
	})

	t.Run("encrypt disabled", func(t *testing.T) {
		t.Parallel()

		// encryption.enabled: false with keys still configured is the deliberate
		// sealing-off / rollback posture: new outbound traffic must not be
		// sealed, but whatever was sealed earlier must still open. The "encode"
		// subtest above runs with Encrypt: true, where inbound and outbound are
		// the same slice, so it cannot catch Encode reading the wrong one; this
		// is the only case that can.
		codecs, err := proxy.NewCodecs(proxy.CodecOptions{Vault: &fakeVault{}, Encrypt: false, Reporter: newTestReporter(t)})
		require.NoError(t, err)

		ctx := meta.WithNamespace(t.Context(), ns)

		encoded, err := codecs.Encode(ctx, ns, clone(original))
		require.NoError(t, err)
		requirePayloadsEqual(t, original, encoded)

		sealed := []*common.Payload{
			sealedPayload(t, original[0]),
			sealedPayload(t, original[1]),
		}

		opened, err := codecs.Decode(ctx, ns, clone(sealed))
		require.NoError(t, err)
		requirePayloadsEqual(t, original, opened)
	})
}

// codecOpts returns options with a fresh vault, so the two paths cannot share
// vault state and accidentally agree.
func codecOpts(t *testing.T) proxy.CodecOptions {
	t.Helper()
	return proxy.CodecOptions{Vault: &fakeVault{}, Encrypt: true, Reporter: newTestReporter(t)}
}

func clone(ps []*common.Payload) []*common.Payload {
	out := make([]*common.Payload, len(ps))
	for i, p := range ps {
		out[i] = proto.Clone(p).(*common.Payload)
	}

	return out
}

func requirePayloadsEqual(t *testing.T, want, got []*common.Payload) {
	t.Helper()

	require.Len(t, got, len(want))
	for i := range want {
		require.True(t, proto.Equal(want[i], got[i]), "payload %d differs between the interceptor and the direct call", i)
	}
}
