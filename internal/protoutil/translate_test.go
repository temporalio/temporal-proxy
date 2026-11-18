package protoutil_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/reflect/protoregistry"

	"go.temporal.io/proxy/internal/protoutil"
)

func TestTranslatorTranslate(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "local"}
	tr.Translate(req, remote)
	require.Equal(t, "remote-local", req.Namespace)
}

func TestTranslatorNilMessageIsNoop(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	var req *workflowservice.StartWorkflowExecutionRequest
	require.NotPanics(t, func() { tr.Translate(req, remote) })
}

func TestTranslatorWarmServicePopulatesCache(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	require.NoError(t, tr.WarmService("temporal.api.workflowservice.v1.WorkflowService"))

	// After warming, translating a request message must not need to build a plan;
	// IsWarm reports whether the type's plan is already cached.
	req := &workflowservice.StartWorkflowExecutionRequest{}
	require.True(t, tr.IsWarm(req.ProtoReflect().Descriptor().FullName()))
}

func TestTranslatorWarmServiceUnknownName(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	require.Error(t, tr.WarmService("does.not.Exist"))
}

func TestTranslateLeavesEmptyNamespace(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	req := &workflowservice.StartWorkflowExecutionRequest{}
	tr.Translate(req, remote)
	require.Equal(t, "", req.Namespace, "an unset namespace must not be wrapped")
}

func TestTranslateNestedOverrideAndRepeated(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	resp := &workflowservice.ListNamespacesResponse{
		Namespaces: []*workflowservice.DescribeNamespaceResponse{
			{NamespaceInfo: &namespacepb.NamespaceInfo{Name: "one", Id: "id-1"}},
			{NamespaceInfo: &namespacepb.NamespaceInfo{Name: "two"}},
		},
	}
	tr.Translate(resp, remote)
	require.Equal(t, "remote-one", resp.Namespaces[0].NamespaceInfo.Name)
	require.Equal(t, "id-1", resp.Namespaces[0].NamespaceInfo.Id, "namespace id must not change")
	require.Equal(t, "remote-two", resp.Namespaces[1].NamespaceInfo.Name)
}

func TestTranslateDeepHistoryEventLeavesNamespaceId(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	evt := &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
			StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
				Namespace:   "child-local",
				NamespaceId: "uuid-keep",
			},
		},
	}
	tr.Translate(evt, remote)
	attrs := evt.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	require.Equal(t, "remote-child-local", attrs.Namespace)
	require.Equal(t, "uuid-keep", attrs.NamespaceId)
}

func TestTranslateRecursiveFailureCause(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	f := &failurepb.Failure{
		Message: "outer",
		Cause: &failurepb.Failure{
			FailureInfo: &failurepb.Failure_ChildWorkflowExecutionFailureInfo{
				ChildWorkflowExecutionFailureInfo: &failurepb.ChildWorkflowExecutionFailureInfo{
					Namespace: "child-local",
				},
			},
		},
	}
	tr.Translate(f, remote)
	require.Equal(t, "remote-child-local", f.Cause.GetChildWorkflowExecutionFailureInfo().Namespace)
}

// TestTranslateNexusLinkURL pins that the namespace segment of a Nexus link
// URL is translated like a namespace field, and that everything else in the
// URL, or a URL that is not a Temporal link, is left exactly as it was.
func TestTranslateNexusLinkURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		fn   func(string) string
		want string
	}{
		{
			name: "workflow event link keeps its path and query",
			url:  "temporal:///namespaces/orders/workflows/wf-1/run-1/history?eventID=10&eventType=NexusOperationScheduled&referenceType=EventReference",
			fn:   remote,
			want: "temporal:///namespaces/remote-orders/workflows/wf-1/run-1/history?eventID=10&eventType=NexusOperationScheduled&referenceType=EventReference",
		},
		{
			name: "remote to local strips the suffix",
			url:  "temporal:///namespaces/orders.acct/workflows/wf-1/run-1",
			fn:   func(s string) string { return strings.TrimSuffix(s, ".acct") },
			want: "temporal:///namespaces/orders/workflows/wf-1/run-1",
		},
		{
			name: "an escaped segment is unescaped for fn and escaped again",
			url:  "temporal:///namespaces/a%20b/nexus-operations/op/run/details",
			fn:   func(s string) string { return s + "+c" },
			want: "temporal:///namespaces/a%20b+c/nexus-operations/op/run/details",
		},
		{
			name: "an unchanged name leaves the URL byte for byte",
			url:  "temporal:///namespaces/%6Frders/workflows/wf-1/run-1",
			fn:   func(s string) string { return s },
			want: "temporal:///namespaces/%6Frders/workflows/wf-1/run-1",
		},
		{
			name: "a namespace with nothing after it",
			url:  "temporal:///namespaces/orders",
			fn:   remote,
			want: "temporal:///namespaces/remote-orders",
		},
		{
			name: "another scheme is left alone",
			url:  "https://example.com/namespaces/orders/workflows/wf-1",
			fn:   remote,
			want: "https://example.com/namespaces/orders/workflows/wf-1",
		},
		{
			name: "a Temporal URL naming no namespace is left alone",
			url:  "temporal://system",
			fn:   remote,
			want: "temporal://system",
		},
		{
			name: "an empty namespace segment is left alone",
			url:  "temporal:///namespaces//workflows/wf-1",
			fn:   remote,
			want: "temporal:///namespaces//workflows/wf-1",
		},
		{
			name: "a malformed escape is left alone",
			url:  "temporal:///namespaces/a%zz/workflows/wf-1",
			fn:   remote,
			want: "temporal:///namespaces/a%zz/workflows/wf-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
			link := &nexuspb.Link{Url: tc.url, Type: "temporal.api.common.v1.Link.WorkflowEvent"}
			tr.Translate(link, tc.fn)
			require.Equal(t, tc.want, link.Url)
			require.Equal(t, "temporal.api.common.v1.Link.WorkflowEvent", link.Type, "the link type is not a namespace")
		})
	}
}

// TestTranslateNexusLinksInTaskMessages pins the two hops a spike observed
// breaking under translation: the caller's link a handler Worker receives on
// its poll, and the handler's link it sends back on an async start.
func TestTranslateNexusLinksInTaskMessages(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	local := func(s string) string { return strings.TrimPrefix(s, "remote-") }

	poll := &workflowservice.PollNexusTaskQueueResponse{
		Request: &nexuspb.Request{
			Variant: &nexuspb.Request_StartOperation{StartOperation: &nexuspb.StartOperationRequest{
				Links: []*nexuspb.Link{{Url: "temporal:///namespaces/remote-orders/workflows/wf-1/run-1/history"}},
			}},
		},
	}
	tr.Translate(poll, local)
	require.Equal(t, "temporal:///namespaces/orders/workflows/wf-1/run-1/history",
		poll.GetRequest().GetStartOperation().GetLinks()[0].GetUrl(), "the handler sees the caller's local name")

	reply := &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: "payments",
		Response: &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_AsyncSuccess{AsyncSuccess: &nexuspb.StartOperationResponse_Async{
					OperationToken: `{"ns":"payments"}`,
					Links:          []*nexuspb.Link{{Url: "temporal:///namespaces/payments/workflows/charge-1/run-1/history"}},
				}},
			}},
		},
	}
	tr.Translate(reply, remote)
	async := reply.GetResponse().GetStartOperation().GetAsyncSuccess()
	require.Equal(t, "remote-payments", reply.GetNamespace())
	require.Equal(t, "temporal:///namespaces/remote-payments/workflows/charge-1/run-1/history",
		async.GetLinks()[0].GetUrl(), "the upstream sees the handler's remote name")
	require.JSONEq(t, `{"ns":"payments"}`, async.GetOperationToken(), "the opaque operation token is not translated")
}

func remote(s string) string { return "remote-" + s }
