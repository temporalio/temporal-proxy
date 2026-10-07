package proxy_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/codec"
)

const failureNS = "orders"

func TestCodecInterceptorEncodesFailures(t *testing.T) {
	t.Parallel()

	codecs, sent := sendFailure(t, failureOpts(t), &failurepb.Failure{Message: "boom", StackTrace: "at x"})

	require.Equal(t, "Encoded failure", sent.GetMessage())
	require.Empty(t, sent.GetStackTrace())

	// The moved attributes travel sealed, so the upstream never sees the text.
	sealed := sent.GetEncodedAttributes()
	require.Equal(t, codec.EncryptionEncoding, string(sealed.GetMetadata()[codec.MetadataEncoding]))

	// Golden values produced by the Temporal SDK's EncodeCommonFailureAttributes
	// (v1.49.0, default data converter). An SDK worker decodes these with
	// DecodeCommonFailureAttributes, so any drift here leaves it showing
	// "Encoded failure" instead of the real message.
	opened := openAttributes(t, codecs, sent)
	require.Equal(t, map[string][]byte{"encoding": []byte("json/plain")}, opened.GetMetadata())
	require.JSONEq(t, `{"message":"boom","stack_trace":"at x"}`, string(opened.GetData()))
}

func TestCodecInterceptorEncodesFailureCauses(t *testing.T) {
	t.Parallel()

	codecs, sent := sendFailure(t, failureOpts(t), &failurepb.Failure{
		Message: "outer",
		Cause:   &failurepb.Failure{Message: "inner", StackTrace: "at y"},
	})

	cause := sent.GetCause()
	require.Equal(t, "Encoded failure", cause.GetMessage())
	require.Empty(t, cause.GetStackTrace())
	require.JSONEq(t, `{"message":"inner","stack_trace":"at y"}`, string(openAttributes(t, codecs, cause).GetData()))
}

// A worker running the SDK with EncodeCommonAttributes has already moved the
// text, so the proxy only seals what the SDK wrote rather than wrapping it again.
func TestCodecInterceptorKeepsSDKEncodedFailures(t *testing.T) {
	t.Parallel()

	attrs := testPayload("json/plain", `{"message":"boom","stack_trace":"at x"}`)
	codecs, sent := sendFailure(t, failureOpts(t), &failurepb.Failure{
		Message:           "Encoded failure",
		EncodedAttributes: testPayload("json/plain", `{"message":"boom","stack_trace":"at x"}`),
	})

	require.Equal(t, "Encoded failure", sent.GetMessage())
	require.Equal(t, codec.EncryptionEncoding, string(sent.GetEncodedAttributes().GetMetadata()[codec.MetadataEncoding]))
	require.True(t, proto.Equal(attrs, openAttributes(t, codecs, sent)))
}

func TestCodecInterceptorLeavesFailuresWhenDisabled(t *testing.T) {
	t.Parallel()

	opts := failureOpts(t)
	opts.EncodeFailures = false

	_, sent := sendFailure(t, opts, &failurepb.Failure{Message: "boom", StackTrace: "at x"})

	require.Equal(t, "boom", sent.GetMessage())
	require.Equal(t, "at x", sent.GetStackTrace())
	require.Nil(t, sent.GetEncodedAttributes())
}

// A client reading through the proxy, such as the Web UI or CLI, sees the text
// where it expects it rather than having to decode the attributes itself. The
// attributes stay too, so an SDK client decoding them gets the same values.
func TestCodecInterceptorRestoresFailures(t *testing.T) {
	t.Parallel()

	attrs := testPayload("json/plain", `{"message":"boom","stack_trace":"at x"}`)
	got := receiveFailure(t, failureOpts(t), &failurepb.Failure{
		Message:           "Encoded failure",
		EncodedAttributes: sealedPayload(t, attrs),
	})

	require.Equal(t, "boom", got.GetMessage())
	require.Equal(t, "at x", got.GetStackTrace())
	require.True(t, proto.Equal(attrs, got.GetEncodedAttributes()))
}

// Failures encoded while encoding was on, or by an SDK Worker, stay readable
// with it off, as payloads sealed earlier stay readable with sealing off.
func TestCodecInterceptorRestoresFailuresWhenEncodingDisabled(t *testing.T) {
	t.Parallel()

	opts := failureOpts(t)
	opts.EncodeFailures = false

	got := receiveFailure(t, opts, &failurepb.Failure{
		Message:           "Encoded failure",
		EncodedAttributes: sealedPayload(t, testPayload("json/plain", `{"message":"boom","stack_trace":"at x"}`)),
	})

	require.Equal(t, "boom", got.GetMessage())
	require.Equal(t, "at x", got.GetStackTrace())
}

func TestCodecInterceptorKeepsUnreadableFailureAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		attrs *common.Payload
	}{
		{
			// Readable JSON, so only the encoding keeps it from being restored.
			name:  "sealed by a client codec",
			attrs: testPayload("binary/custom", `{"message":"boom","stack_trace":"at x"}`),
		},
		{
			name:  "not the SDK's JSON",
			attrs: testPayload("json/plain", "not json"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := receiveFailure(t, failureOpts(t), &failurepb.Failure{
				Message:           "Encoded failure",
				EncodedAttributes: tc.attrs,
			})

			require.Equal(t, "Encoded failure", got.GetMessage())
			require.True(t, proto.Equal(tc.attrs, got.GetEncodedAttributes()))
		})
	}
}

// failureOpts enables failure encoding over a fake vault.
func failureOpts(t *testing.T) proxy.CodecOptions {
	t.Helper()

	return proxy.CodecOptions{Vault: &fakeVault{}, Encrypt: true, EncodeFailures: true, Reporter: newTestReporter(t)}
}

// sendFailure sends f through the interceptor opts build and returns the codecs
// along with the failure as the upstream received it.
func sendFailure(t *testing.T, opts proxy.CodecOptions, f *failurepb.Failure) (*proxy.Codecs, *failurepb.Failure) {
	t.Helper()

	codecs, err := proxy.NewCodecs(opts)
	require.NoError(t, err)

	interceptor, err := codecs.Interceptor()
	require.NoError(t, err)

	var sent *failurepb.Failure
	invoker := func(_ context.Context, _ string, gotReq, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		sent = gotReq.(*workflowservice.RespondActivityTaskFailedRequest).GetFailure()
		return nil
	}

	req := &workflowservice.RespondActivityTaskFailedRequest{Failure: f}
	resp := &workflowservice.RespondActivityTaskFailedResponse{}
	ctx := meta.WithNamespace(t.Context(), failureNS)
	require.NoError(t, interceptor(ctx, "/svc/RespondActivityTaskFailed", req, resp, nil, invoker))

	return codecs, sent
}

// receiveFailure returns f as a client receives it, inside a history response
// from the interceptor opts build.
func receiveFailure(t *testing.T, opts proxy.CodecOptions, f *failurepb.Failure) *failurepb.Failure {
	t.Helper()

	interceptor, err := proxy.CodecInterceptor(opts)
	require.NoError(t, err)

	invoker := func(_ context.Context, _ string, _, gotResp any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotResp.(*workflowservice.GetWorkflowExecutionHistoryResponse).History = &historypb.History{
			Events: []*historypb.HistoryEvent{{
				Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
					ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{Failure: f},
				},
			}},
		}

		return nil
	}

	req := &workflowservice.GetWorkflowExecutionHistoryRequest{}
	resp := &workflowservice.GetWorkflowExecutionHistoryResponse{}
	ctx := meta.WithNamespace(t.Context(), failureNS)
	require.NoError(t, interceptor(ctx, "/svc/GetWorkflowExecutionHistory", req, resp, nil, invoker))

	return resp.GetHistory().GetEvents()[0].GetActivityTaskFailedEventAttributes().GetFailure()
}

// openAttributes opens the sealed encoded attributes of f.
func openAttributes(t *testing.T, codecs *proxy.Codecs, f *failurepb.Failure) *common.Payload {
	t.Helper()

	opened, err := codecs.Decode(t.Context(), failureNS, []*common.Payload{f.GetEncodedAttributes()})
	require.NoError(t, err)
	require.Len(t, opened, 1)

	return opened[0]
}
