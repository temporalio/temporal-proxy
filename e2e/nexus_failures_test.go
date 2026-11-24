package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
)

const (
	// cardNumber is the secret each test's failure carries in its message and
	// details.
	cardNumber = "4111-1111-1111-1111"

	// temporalFailureType is the metadata type the Temporal SDKs give a deprecated
	// Nexus failure whose details hold a serialized Temporal failure.
	temporalFailureType = "temporal.api.failure.v1.Failure"
)

// TestEndToEndLegacyNexusFailureIsSealed sends a handler's failure in the
// deprecated Nexus shape through an encrypting proxy, as a handler does when its
// task lacks temporal_failure_responses. Nothing reaches the upstream in
// plaintext. Rebuilt the way the server rebuilds it, the failure's sealed
// payloads open with the proxy's keys, so it is still readable downstream.
func TestEndToEndLegacyNexusFailureIsSealed(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, nexusConfig(t, up, true))

	details := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
		Data:     []byte(`{"card":"` + cardNumber + `"}`),
	}

	_, err := respondNexus(t, f, legacyOperationError(t, details))
	require.NoError(t, err)

	reply := upstreamReply(t, up)
	wire, err := proto.Marshal(reply)
	require.NoError(t, err)
	require.NotContains(t, string(wire), cardNumber, "the card number never reaches the upstream")

	sent := reply.GetResponse().GetStartOperation().GetOperationError().GetFailure() //nolint:staticcheck // the deprecated shape is the subject
	require.Equal(t, "Encoded failure", sent.GetMessage())
	require.Empty(t, sent.GetStackTrace())
	require.Equal(t, map[string]string{"type": temporalFailureType}, sent.GetMetadata())

	// The server's rebuild: details hold the Temporal failure, and the message
	// and stack trace come from the outer fields.
	var rebuilt failurepb.Failure
	require.NoError(t, protojson.Unmarshal(sent.GetDetails(), &rebuilt))
	rebuilt.Message, rebuilt.StackTrace = sent.GetMessage(), sent.GetStackTrace()

	attrs := rebuilt.GetEncodedAttributes()
	sealedDetails := rebuilt.GetApplicationFailureInfo().GetDetails().GetPayloads()
	require.Len(t, sealedDetails, 1)
	require.Equal(t, wireEncryptedMarker, string(attrs.GetMetadata()[wireEncoding]))
	require.Equal(t, wireEncryptedMarker, string(sealedDetails[0].GetMetadata()[wireEncoding]))

	opened := decodeThroughCodecServer(t, f, []*common.Payload{attrs, sealedDetails[0]}, "ns1")
	require.Len(t, opened, 2)
	require.JSONEq(t, `{"message":"card declined for `+cardNumber+`","stack_trace":"at charge()"}`,
		string(opened[0].GetData()))
	require.True(t, proto.Equal(details, opened[1]), "the details open to what the handler sent")
}

// TestEndToEndRefusesUnsealableLegacyNexusFailure sends a deprecated Nexus
// failure whose details are not a Temporal failure, as Java SDK 1.26 and 1.27
// send for a handler's own OperationHandlerException. The proxy cannot seal it,
// so it refuses the reply rather than send it in plaintext.
func TestEndToEndRefusesUnsealableLegacyNexusFailure(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, nexusConfig(t, up, true))

	_, err := respondNexus(t, f, operationError(&nexuspb.Failure{
		Message:  "card declined",
		Metadata: map[string]string{"type": "com.acme.Declined"},
		Details:  []byte(`{"card":"` + cardNumber + `"}`),
	}))
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "err: %v", err)

	for _, r := range up.Requests() {
		_, ok := r.(*workflowservice.RespondNexusTaskCompletedRequest)
		require.False(t, ok, "a refused reply never reaches the upstream")
	}
}

// TestEndToEndLegacyNexusFailureWithoutEncryption sends a deprecated Nexus
// failure through a proxy that seals nothing. It reaches the upstream exactly as
// the handler sent it.
func TestEndToEndLegacyNexusFailureWithoutEncryption(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, nexusConfig(t, up, false))

	details := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
		Data:     []byte(`{"card":"` + cardNumber + `"}`),
	}
	resp := legacyOperationError(t, details)

	_, err := respondNexus(t, f, resp)
	require.NoError(t, err)
	require.True(t, proto.Equal(resp, upstreamReply(t, up).GetResponse().GetStartOperation()))
}

// nexusConfig is a config for up. When encrypt is set, encryption and failure
// encoding are on under a local key, and the codec server is enabled.
func nexusConfig(t *testing.T, up *dataplanetest.Upstream, encrypt bool) *config.Config {
	t.Helper()

	cfg := dataplanetest.Config(up)
	if encrypt {
		cfg.Encryption = config.Encryption{
			Enabled:          true,
			Failures:         true,
			NamespaceBinding: config.NamespaceBindingStrict,
			Default:          &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
		}
		cfg.HTTP = config.HTTP{
			Listen:      config.ListenConfig{HostPort: "127.0.0.1:0", Insecure: true},
			CodecServer: config.CodecServer{Enabled: true},
		}
	}

	return cfg
}

// legacyOperationError is a start operation response failed with a Temporal
// failure carrying details, in the deprecated shape the SDKs send: the message
// and stack trace in the outer fields and the rest of the failure serialized
// into details.
func legacyOperationError(t *testing.T, details *common.Payload) *nexuspb.StartOperationResponse {
	t.Helper()

	inner, err := protojson.Marshal(&failurepb.Failure{
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:    "Declined",
			Details: &common.Payloads{Payloads: []*common.Payload{proto.Clone(details).(*common.Payload)}},
		}},
	})
	require.NoError(t, err)

	return operationError(&nexuspb.Failure{
		Message:    "card declined for " + cardNumber,
		StackTrace: "at charge()",
		Metadata:   map[string]string{"type": temporalFailureType},
		Details:    inner,
	})
}

// operationError is a start operation response failed with f in the deprecated
// shape.
func operationError(f *nexuspb.Failure) *nexuspb.StartOperationResponse {
	return &nexuspb.StartOperationResponse{
		//nolint:staticcheck // the deprecated shape is the subject
		Variant: &nexuspb.StartOperationResponse_OperationError{OperationError: &nexuspb.UnsuccessfulOperationError{
			OperationState: "failed",
			Failure:        f,
		}},
	}
}

// respondNexus sends resp as a handler Worker's reply to a Nexus task in ns1
// through f's gateway.
func respondNexus(
	t *testing.T, f *dataplanetest.Fixture, resp *nexuspb.StartOperationResponse,
) (*workflowservice.RespondNexusTaskCompletedResponse, error) {
	t.Helper()

	return f.Client().RespondNexusTaskCompleted(f.Context(), &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: "ns1",
		TaskToken: []byte("task-1"),
		Response:  &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: resp}},
	}, grpc.WaitForReady(true))
}

// upstreamReply is the Nexus reply as up received it.
func upstreamReply(t *testing.T, up *dataplanetest.Upstream) *workflowservice.RespondNexusTaskCompletedRequest {
	t.Helper()

	for _, r := range up.Requests() {
		if reply, ok := r.(*workflowservice.RespondNexusTaskCompletedRequest); ok {
			return reply
		}
	}

	t.Fatal("the upstream never received the reply")

	return nil
}
