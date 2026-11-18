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
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
)

// nexusNoFailureSupportFamily is the counter a proxy started by [dataplanetest]
// reports Nexus tasks polled without temporal_failure_responses in.
const nexusNoFailureSupportFamily = "test_encryption_nexus_tasks_without_failure_support_total"

// legacyFailureDetails is what the Go SDK writes into a legacy nexus.Failure's
// details: the Temporal failure, protojson-encoded, sensitive details and all.
const legacyFailureDetails = `{"message":"card declined for 4111-1111-1111-1111",` +
	`"applicationFailureInfo":{"details":{"payloads":[{"metadata":{"encoding":"anNvbi9wbGFpbg=="},` +
	`"data":"eyJjYXJkIjoiNDExMS0xMTExLTExMTEtMTExMSJ9"}]}}}`

// nexusConfig is a config for up with namespace-labeled metrics, and
// encryption on under a local key when encrypt is set.
func nexusConfig(t *testing.T, up *dataplanetest.Upstream, encrypt bool) *config.Config {
	t.Helper()

	cfg := dataplanetest.Config(up)
	cfg.Metrics.Labels.Namespace = true

	if encrypt {
		cfg.Encryption = config.Encryption{
			Enabled:          true,
			NamespaceBinding: config.NamespaceBindingStrict,
			Default:          &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
		}
	}

	return cfg
}

// nexusTask is a Nexus task in ns1 whose request carries the
// temporal_failure_responses capability as given.
func nexusTask(failureSupport bool) *workflowservice.PollNexusTaskQueueResponse {
	return &workflowservice.PollNexusTaskQueueResponse{
		TaskToken: []byte("task-1"),
		Request: &nexuspb.Request{
			Capabilities: &nexuspb.Request_Capabilities{TemporalFailureResponses: failureSupport},
			Variant: &nexuspb.Request_StartOperation{StartOperation: &nexuspb.StartOperationRequest{
				Service:   "payments",
				Operation: "charge",
			}},
		},
	}
}

// pollNexus polls ns1's Nexus task queue through f's gateway, as a handler
// Worker would.
func pollNexus(t *testing.T, f *dataplanetest.Fixture) *workflowservice.PollNexusTaskQueueResponse {
	t.Helper()

	polled, err := f.Client().PollNexusTaskQueue(f.Context(), &workflowservice.PollNexusTaskQueueRequest{
		Namespace: "ns1",
	}, grpc.WaitForReady(true))
	require.NoError(t, err)

	return polled
}

// respondNexus sends resp as the reply to the task with token through f's
// gateway, and returns the reply as the upstream received it.
func respondNexus(
	t *testing.T, f *dataplanetest.Fixture, up *dataplanetest.Upstream, token []byte, resp *nexuspb.StartOperationResponse,
) *workflowservice.RespondNexusTaskCompletedRequest {
	t.Helper()

	_, err := f.Client().RespondNexusTaskCompleted(f.Context(), &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: "ns1",
		TaskToken: token,
		Response:  &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: resp}},
	}, grpc.WaitForReady(true))
	require.NoError(t, err)

	for _, r := range up.Requests() {
		if reply, ok := r.(*workflowservice.RespondNexusTaskCompletedRequest); ok {
			return reply
		}
	}

	t.Fatal("the upstream never received the reply")

	return nil
}

// nexusNoFailureSupportCount is the counter's value for ns1, or 0 when it has
// no series for ns1.
func nexusNoFailureSupportCount(t *testing.T, f *dataplanetest.Fixture) float64 {
	t.Helper()

	mfs, err := f.Gatherer().Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != nexusNoFailureSupportFamily {
			continue
		}

		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "namespace" && lp.GetValue() == "ns1" {
					return m.GetCounter().GetValue()
				}
			}
		}
	}

	return 0
}

// TestEndToEndNexusTaskWithoutFailureSupport polls a Nexus task without
// temporal_failure_responses through an encrypting proxy. The handler Worker
// receives the task unchanged, and the proxy counts it, once per task. The
// Worker's reply in the legacy nexus.Failure shape then reaches the upstream
// byte for byte, plaintext details included: the exposure the counter makes
// visible, which no codec can reach.
func TestEndToEndNexusTaskWithoutFailureSupport(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	up.SetNexusTask(nexusTask(false))
	f := dataplanetest.Start(t, nexusConfig(t, up, true))

	polled := pollNexus(t, f)
	require.True(t, proto.Equal(nexusTask(false), polled), "the Worker gets the task as the upstream sent it")
	require.InDelta(t, 1, nexusNoFailureSupportCount(t, f), 0)

	pollNexus(t, f)
	require.InDelta(t, 2, nexusNoFailureSupportCount(t, f), 0, "every task is counted")

	legacy := &nexuspb.StartOperationResponse{
		//nolint:staticcheck // the deprecated legacy shape is what this test sends
		Variant: &nexuspb.StartOperationResponse_OperationError{OperationError: &nexuspb.UnsuccessfulOperationError{
			OperationState: "failed",
			Failure: &nexuspb.Failure{
				Message:  "card declined for 4111-1111-1111-1111",
				Metadata: map[string]string{"type": "temporal.api.failure.v1.Failure"},
				Details:  []byte(legacyFailureDetails),
			},
		}},
	}
	reply := respondNexus(t, f, up, polled.GetTaskToken(), legacy)
	require.True(t, proto.Equal(legacy, reply.GetResponse().GetStartOperation()),
		"a legacy failure reaches the upstream unchanged, since nothing in it is a payload")
}

// TestEndToEndNexusTaskWithFailureSupport polls a Nexus task carrying
// temporal_failure_responses through an encrypting proxy. It is not counted,
// and the Worker's reply in the Temporal failure shape reaches the upstream with
// its details sealed.
func TestEndToEndNexusTaskWithFailureSupport(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	up.SetNexusTask(nexusTask(true))
	f := dataplanetest.Start(t, nexusConfig(t, up, true))

	polled := pollNexus(t, f)
	require.Zero(t, nexusNoFailureSupportCount(t, f))

	details := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
		Data:     []byte(`{"card":"4111-1111-1111-1111"}`),
	}
	modern := &nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_Failure{Failure: &failurepb.Failure{
			Message: "charge failed",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:    "Declined",
				Details: &common.Payloads{Payloads: []*common.Payload{details}},
			}},
		}},
	}
	reply := respondNexus(t, f, up, polled.GetTaskToken(), modern)

	sent := reply.GetResponse().GetStartOperation().GetFailure().GetApplicationFailureInfo().GetDetails().GetPayloads()
	require.Len(t, sent, 1)
	require.Equal(t, wireEncryptedMarker, string(sent[0].GetMetadata()[wireEncoding]), "the details are sealed upstream")
	require.NotContains(t, string(sent[0].GetData()), "4111", "the card number never reaches the upstream")
}

// TestEndToEndNexusTaskWithoutEncryption polls a Nexus task without
// temporal_failure_responses through a proxy that seals nothing. With nothing
// for a failure to bypass, it is not counted.
func TestEndToEndNexusTaskWithoutEncryption(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	up.SetNexusTask(nexusTask(false))
	f := dataplanetest.Start(t, nexusConfig(t, up, false))

	polled := pollNexus(t, f)
	require.True(t, proto.Equal(nexusTask(false), polled))
	require.Zero(t, nexusNoFailureSupportCount(t, f))
}
