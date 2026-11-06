package proxy_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/command/v1"
	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	apiproxy "go.temporal.io/api/proxy"
	"go.temporal.io/api/sdk/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/internal/transport/meta"
	"go.temporal.io/proxy/pkg/codec"
)

// systemEnvelope wraps inner as the input of a system Nexus operation, with the
// system payload marker when marked.
func systemEnvelope(t *testing.T, inner proto.Message, marked bool) *common.Payload {
	t.Helper()

	data, err := proto.Marshal(inner)
	require.NoError(t, err)

	env := &common.Payload{
		Metadata: map[string][]byte{
			codec.MetadataEncoding: []byte("binary/protobuf"),
			"messageType":          []byte(proto.MessageName(inner)),
		},
		Data: data,
	}
	if marked {
		env.Metadata[apiproxy.SystemPayloadMetadataKey] = []byte("true")
	}

	return env
}

// scheduleOn is a Workflow task completion scheduling one Nexus operation on
// endpoint with input.
func scheduleOn(endpoint string, input *common.Payload) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: "orders",
		Commands: []*command.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
			Attributes: &command.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &command.ScheduleNexusOperationCommandAttributes{
					Endpoint:  endpoint,
					Service:   "svc",
					Operation: "op",
					Input:     input,
				},
			},
		}},
	}
}

func marked(p *common.Payload) *common.Payload {
	p.Metadata[apiproxy.SystemPayloadMetadataKey] = []byte("true")
	return p
}

// sendThrough runs req through the codec interceptor, sealing when encrypt is
// set, and returns what reached the upstream, or nil when it was not invoked.
func sendThrough(t *testing.T, encrypt bool, req proto.Message) (proto.Message, error) {
	t.Helper()

	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{
		DecodeLimiter: testLimiter(),
		ResponseLimit: testResponseLimit,
		Vault:         &fakeVault{},
		Encrypt:       encrypt,
		Reporter:      newTestReporter(t),
	})
	require.NoError(t, err)

	var sent proto.Message
	invoker := func(_ context.Context, _ string, got, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		sent = got.(proto.Message)
		return nil
	}

	ctx := meta.WithNamespace(t.Context(), "orders")
	err = interceptor(ctx, "/svc/Method", req, &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil, invoker)

	return sent, err
}

// TestSystemEnvelopeIsOpened pins that the input of a system Nexus operation is
// opened rather than sealed whole, whether or not it carries the system payload
// marker: go.temporal.io/api opens only a marked one, and the Go SDK does not
// mark it. The envelope keeps its own metadata and the request inside has its
// payloads sealed.
func TestSystemEnvelopeIsOpened(t *testing.T) {
	t.Parallel()

	for _, mark := range []bool{false, true} {
		t.Run(map[bool]string{false: "unmarked", true: "marked"}[mark], func(t *testing.T) {
			t.Parallel()

			inner := &workflowservice.SignalWithStartWorkflowExecutionRequest{
				Namespace: "orders",
				Input:     &common.Payloads{Payloads: []*common.Payload{testPayload("json/plain", `"in"`)}},
			}
			envelope := systemEnvelope(t, inner, mark)

			sent, err := sendThrough(t, true, scheduleOn("__temporal_system", proto.Clone(envelope).(*common.Payload)))
			require.NoError(t, err)

			got := sent.(*workflowservice.RespondWorkflowTaskCompletedRequest).GetCommands()[0].
				GetScheduleNexusOperationCommandAttributes().GetInput()
			require.Equal(t, envelope.GetMetadata(), got.GetMetadata(), "the envelope's own metadata is kept as it was")

			var opened workflowservice.SignalWithStartWorkflowExecutionRequest
			require.NoError(t, proto.Unmarshal(got.GetData(), &opened))

			p := opened.GetInput().GetPayloads()[0]
			require.Equal(t, codec.EncryptionEncoding, string(p.GetMetadata()[codec.MetadataEncoding]))
			require.Equal(t, testKEKID, string(p.GetMetadata()[codec.MetadataEncryptionKeyID]))
		})
	}
}

// TestSystemMarkerIsRefusedElsewhere pins that with sealing on, a single payload
// carrying the system payload marker anywhere but the input of a system Nexus
// operation fails InvalidArgument before the upstream is invoked. The payload
// visitor in go.temporal.io/api would hand such a payload to no codec, so it
// would otherwise reach the upstream unsealed. With sealing off nothing is
// refused.
func TestSystemMarkerIsRefusedElsewhere(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  func(t *testing.T) proto.Message
	}{
		{
			name: "Nexus start input",
			req: func(*testing.T) proto.Message {
				return &workflowservice.StartNexusOperationExecutionRequest{
					Namespace: "orders",
					Endpoint:  "ep",
					Input:     marked(testPayload("binary/protobuf", "x")),
				}
			},
		},
		{
			name: "Nexus command input on another Nexus Endpoint",
			req: func(*testing.T) proto.Message {
				return scheduleOn("ep", marked(testPayload("binary/protobuf", "x")))
			},
		},
		{
			name: "user metadata",
			req: func(*testing.T) proto.Message {
				req := startRequest(testPayload("json/plain", `"in"`))
				req.UserMetadata = &sdk.UserMetadata{Summary: marked(testPayload("json/plain", `"summary"`))}

				return req
			},
		},
		{
			name: "inside a system envelope",
			req: func(t *testing.T) proto.Message {
				inner := &workflowservice.SignalWithStartWorkflowExecutionRequest{
					Namespace:    "orders",
					UserMetadata: &sdk.UserMetadata{Summary: marked(testPayload("json/plain", `"summary"`))},
				}

				return scheduleOn("__temporal_system", systemEnvelope(t, inner, false))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sent, err := sendThrough(t, true, tc.req(t))
			require.Nil(t, sent, "the upstream must not be invoked")
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.ErrorContains(t, err, apiproxy.SystemPayloadMetadataKey)

			req := tc.req(t)
			sent, err = sendThrough(t, false, req)
			require.NoError(t, err)
			require.True(t, proto.Equal(req, sent), "with sealing off the request is forwarded untouched")
		})
	}
}

// TestSystemMarkerInPayloadListIsSealed pins that the marker on a payload in a
// list is not refused: the visitor seals list payloads marked or not.
func TestSystemMarkerInPayloadListIsSealed(t *testing.T) {
	t.Parallel()

	sent, err := sendThrough(t, true, startRequest(marked(testPayload("json/plain", `"in"`))))
	require.NoError(t, err)

	p := sent.(*workflowservice.StartWorkflowExecutionRequest).GetInput().GetPayloads()[0]
	require.Equal(t, codec.EncryptionEncoding, string(p.GetMetadata()[codec.MetadataEncoding]))
}
