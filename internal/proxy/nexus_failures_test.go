package proxy_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/internal/transport/meta"
	"go.temporal.io/proxy/pkg/codec"
)

const temporalFailureType = "temporal.api.failure.v1.Failure"

// A handler answering a server without the TemporalFailureResponses capability
// sends its failure in the deprecated Nexus shape, a Temporal failure serialized
// into details. It travels sealed just like the current shape.
func TestCodecInterceptorEncodesLegacyNexusFailures(t *testing.T) {
	t.Parallel()

	for name, wrap := range legacyNexusRequests() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			details := testPayload("json/plain", `"secret"`)
			nf := legacyNexusFailure(t, &failurepb.Failure{
				Message:    "boom",
				StackTrace: "at x",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Details: &common.Payloads{Payloads: []*common.Payload{proto.Clone(details).(*common.Payload)}},
				}},
			})

			v := &fakeVault{}
			opts := failureOpts(t)
			opts.Vault = v

			codecs, sent := sendNexusFailure(t, opts, wrap(nf))

			require.Equal(t, "Encoded failure", sent.GetMessage())
			require.Empty(t, sent.GetStackTrace())
			require.Equal(t, map[string]string{"type": temporalFailureType}, sent.GetMetadata())

			// The server rebuilds the failure from details, taking the message and
			// stack trace from the outer fields, so details carries neither.
			var tf failurepb.Failure
			require.NoError(t, protojson.Unmarshal(sent.GetDetails(), &tf))
			require.Empty(t, tf.GetMessage())
			require.Empty(t, tf.GetStackTrace())

			attrs := tf.GetEncodedAttributes()
			require.Equal(t, codec.EncryptionEncoding, string(attrs.GetMetadata()[codec.MetadataEncoding]))
			require.JSONEq(t, `{"message":"boom","stack_trace":"at x"}`, string(openAttributes(t, codecs, &tf).GetData()))

			sealed := tf.GetApplicationFailureInfo().GetDetails().GetPayloads()
			require.Len(t, sealed, 1)
			require.Equal(t, codec.EncryptionEncoding, string(sealed[0].GetMetadata()[codec.MetadataEncoding]))
			opened, err := codecs.Decode(t.Context(), failureNS, failureNS, sealed)
			require.NoError(t, err)
			require.True(t, proto.Equal(details, opened[0]))

			// Sealed for the request's namespace, like every other payload in it.
			require.Equal(t, []string{failureNS, failureNS}, v.namespaces)
		})
	}
}

// With failure encoding off, a deprecated Nexus failure is treated as the
// current shape would be: its payloads are sealed and its text is left alone.
func TestCodecInterceptorSealsLegacyNexusFailurePayloadsWhenEncodingDisabled(t *testing.T) {
	t.Parallel()

	details := testPayload("json/plain", `"secret"`)
	nf := legacyNexusFailure(t, &failurepb.Failure{
		Message:    "boom",
		StackTrace: "at x",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Details: &common.Payloads{Payloads: []*common.Payload{proto.Clone(details).(*common.Payload)}},
		}},
	})

	opts := failureOpts(t)
	opts.EncodeFailures = false

	codecs, sent := sendNexusFailure(t, opts, legacyNexusRequests()["handler error"](nf))

	require.Equal(t, "boom", sent.GetMessage())
	require.Equal(t, "at x", sent.GetStackTrace())

	var tf failurepb.Failure
	require.NoError(t, protojson.Unmarshal(sent.GetDetails(), &tf))
	require.Nil(t, tf.GetEncodedAttributes())

	sealed := tf.GetApplicationFailureInfo().GetDetails().GetPayloads()
	require.Equal(t, codec.EncryptionEncoding, string(sealed[0].GetMetadata()[codec.MetadataEncoding]))
	opened, err := codecs.Decode(t.Context(), failureNS, failureNS, sealed)
	require.NoError(t, err)
	require.True(t, proto.Equal(details, opened[0]))
}

// A deprecated Nexus failure that is not a serialized Temporal failure has no
// attributes to keep its text sealed in. One holding only a message loses it to
// the placeholder when failure encoding is on, as Python's shutdown drain sends.
// One holding anything else is refused while encrypting rather than sent in
// plaintext, and a Temporal failure that will not decode is refused outright.
func TestCodecInterceptorHandlesUntypedLegacyNexusFailures(t *testing.T) {
	t.Parallel()

	unknownField := &nexuspb.Failure{
		Message:  "boom",
		Metadata: map[string]string{"type": temporalFailureType},
		Details:  []byte(`{"notAField":"secret"}`),
	}

	tests := []struct {
		name    string
		failure *nexuspb.Failure
		encrypt bool
		encode  bool
		want    *nexuspb.Failure
		code    codes.Code
	}{
		{
			name:    "message only, encoding on",
			failure: &nexuspb.Failure{Message: "Worker shutting down", StackTrace: "at x"},
			encrypt: true,
			encode:  true,
			want:    &nexuspb.Failure{Message: "Encoded failure"},
		},
		{
			name:    "message only, encoding off",
			failure: &nexuspb.Failure{Message: "Worker shutting down", StackTrace: "at x"},
			encrypt: true,
			want:    &nexuspb.Failure{Message: "Worker shutting down", StackTrace: "at x"},
		},
		{
			name:    "details, encrypting",
			failure: &nexuspb.Failure{Message: "boom", Details: []byte(`{"secret":true}`)},
			encrypt: true,
			code:    codes.FailedPrecondition,
		},
		{
			name:    "unknown type, encrypting",
			failure: &nexuspb.Failure{Message: "boom", Metadata: map[string]string{"type": "nexus.HandlerError"}},
			encrypt: true,
			code:    codes.FailedPrecondition,
		},
		{
			name:    "cause, encrypting",
			failure: &nexuspb.Failure{Message: "boom", Cause: &nexuspb.Failure{Message: "inner"}},
			encrypt: true,
			code:    codes.FailedPrecondition,
		},
		{
			name:    "details, decrypt only",
			failure: &nexuspb.Failure{Message: "boom", Details: []byte(`{"secret":true}`)},
			want:    &nexuspb.Failure{Message: "boom", Details: []byte(`{"secret":true}`)},
		},
		{
			name:    "unknown field in a Temporal failure",
			failure: unknownField,
			encrypt: true,
			encode:  true,
			code:    codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := failureOpts(t)
			opts.Encrypt = tc.encrypt
			opts.EncodeFailures = tc.encode

			sent, err := interceptNexusFailure(t, opts, legacyNexusRequests()["handler error"](tc.failure))
			if tc.code != codes.OK {
				require.Equal(t, tc.code, status.Code(err), "err: %v", err)
				require.Nil(t, sent, "a refused request must not reach the upstream")

				return
			}

			require.NoError(t, err)
			require.True(t, proto.Equal(tc.want, sent), "sent: %v", sent)
		})
	}
}

// legacyNexusRequests returns each request that can carry a deprecated Nexus
// failure, keyed by name, as a func wrapping a failure in it.
func legacyNexusRequests() map[string]func(*nexuspb.Failure) proto.Message {
	return map[string]func(*nexuspb.Failure) proto.Message{
		"handler error": func(f *nexuspb.Failure) proto.Message {
			return &workflowservice.RespondNexusTaskFailedRequest{
				Namespace: failureNS,
				Error:     &nexuspb.HandlerError{ErrorType: "INTERNAL", Failure: f}, //nolint:staticcheck // the deprecated field is the subject
			}
		},
		"operation error": func(f *nexuspb.Failure) proto.Message {
			return &workflowservice.RespondNexusTaskCompletedRequest{
				Namespace: failureNS,
				Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_OperationError{OperationError: &nexuspb.UnsuccessfulOperationError{ //nolint:staticcheck // as above
						OperationState: "failed",
						Failure:        f,
					}},
				}}},
			}
		},
	}
}

// legacyNexusFailure wraps f the way the Java SDK and sdk-core do for a server
// without the TemporalFailureResponses capability: the message and stack trace
// go in the outer fields and the rest of f is serialized into details.
func legacyNexusFailure(t *testing.T, f *failurepb.Failure) *nexuspb.Failure {
	t.Helper()

	inner := proto.Clone(f).(*failurepb.Failure)
	inner.Message = ""
	inner.StackTrace = ""

	details, err := protojson.Marshal(inner)
	require.NoError(t, err)

	return &nexuspb.Failure{
		Message:    f.GetMessage(),
		StackTrace: f.GetStackTrace(),
		Metadata:   map[string]string{"type": temporalFailureType},
		Details:    details,
	}
}

// sendNexusFailure sends req through the interceptor opts build and returns the
// codecs along with the deprecated Nexus failure as the upstream received it.
func sendNexusFailure(t *testing.T, opts proxy.CodecOptions, req proto.Message) (*proxy.Codecs, *nexuspb.Failure) {
	t.Helper()

	codecs, err := proxy.NewCodecs(opts)
	require.NoError(t, err)

	sent, err := interceptWith(t, codecs, req)
	require.NoError(t, err)
	require.NotNil(t, sent)

	return codecs, sent
}

// interceptNexusFailure sends req through the interceptor opts build and returns
// the deprecated Nexus failure as the upstream received it, nil if the request
// never reached it, along with the interceptor's error.
func interceptNexusFailure(t *testing.T, opts proxy.CodecOptions, req proto.Message) (*nexuspb.Failure, error) {
	t.Helper()

	codecs, err := proxy.NewCodecs(opts)
	require.NoError(t, err)

	return interceptWith(t, codecs, req)
}

// interceptWith sends req through the interceptor codecs build. See
// [interceptNexusFailure].
func interceptWith(t *testing.T, codecs *proxy.Codecs, req proto.Message) (*nexuspb.Failure, error) {
	t.Helper()

	interceptor, err := codecs.Interceptor()
	require.NoError(t, err)

	var sent *nexuspb.Failure
	invoker := func(_ context.Context, _ string, gotReq, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		sent = legacyFailureOf(gotReq)
		return nil
	}

	ctx := meta.WithNamespace(t.Context(), failureNS)
	err = interceptor(ctx, "/svc/RespondNexusTask", req, &workflowservice.RespondNexusTaskFailedResponse{}, nil, invoker)

	return sent, err
}

// legacyFailureOf returns the deprecated Nexus failure req carries.
func legacyFailureOf(req any) *nexuspb.Failure {
	switch r := req.(type) {
	case *workflowservice.RespondNexusTaskFailedRequest:
		return r.GetError().GetFailure() //nolint:staticcheck // the deprecated field is the subject
	case *workflowservice.RespondNexusTaskCompletedRequest:
		return r.GetResponse().GetStartOperation().GetOperationError().GetFailure() //nolint:staticcheck // as above
	default:
		return nil
	}
}
