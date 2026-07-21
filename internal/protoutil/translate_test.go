package protoutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
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

func remote(s string) string { return "remote-" + s }
