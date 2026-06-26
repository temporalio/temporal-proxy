package namespace_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	operatorservicev1 "go.temporal.io/api/operatorservice/v1"
	workflowservicev1 "go.temporal.io/api/workflowservice/v1"

	"github.com/temporalio/temporal-proxy/pkg/namespace"
)

// testMapper produces distinct outputs for Local vs Remote so tests can
// assert which direction the translator dispatched to.
type testMapper struct{}

func TestTranslator_ToRemote(t *testing.T) {
	t.Parallel()

	tr := namespace.New(testMapper{})

	t.Run("top-level namespace field", func(t *testing.T) {
		t.Parallel()

		msg := &workflowservicev1.StartWorkflowExecutionRequest{Namespace: "payments"}
		tr.ToRemote(msg)

		require.Equal(t, "R(payments)", msg.Namespace)
	})

	t.Run("workflow_namespace on poll response", func(t *testing.T) {
		t.Parallel()

		msg := &workflowservicev1.PollActivityTaskQueueResponse{WorkflowNamespace: "payments"}
		tr.ToRemote(msg)

		require.Equal(t, "R(payments)", msg.WorkflowNamespace)
	})

	t.Run("deleted_namespace on operator response", func(t *testing.T) {
		t.Parallel()

		msg := &operatorservicev1.DeleteNamespaceResponse{DeletedNamespace: "payments"}
		tr.ToRemote(msg)

		require.Equal(t, "R(payments)", msg.DeletedNamespace)
	})

	t.Run("nested parent_workflow_namespace inside attributes", func(t *testing.T) {
		t.Parallel()

		msg := &historypb.WorkflowExecutionStartedEventAttributes{
			ParentWorkflowNamespace:   "payments",
			ParentWorkflowNamespaceId: "uuid-stays",
		}
		tr.ToRemote(msg)

		require.Equal(t, "R(payments)", msg.ParentWorkflowNamespace)
		require.Equal(t, "uuid-stays", msg.ParentWorkflowNamespaceId, "namespace id must be untouched")
	})

	t.Run("deeply nested via HistoryEvent oneof", func(t *testing.T) {
		t.Parallel()

		msg := &workflowservicev1.GetWorkflowExecutionHistoryResponse{
			History: &historypb.History{
				Events: []*historypb.HistoryEvent{
					{Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
						WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
							ParentWorkflowNamespace: "payments",
						},
					}},
					{Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
						SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
							Namespace: "billing",
						},
					}},
				},
			},
		}
		tr.ToRemote(msg)

		require.Equal(t, "R(payments)",
			msg.History.Events[0].GetWorkflowExecutionStartedEventAttributes().ParentWorkflowNamespace)
		require.Equal(t, "R(billing)",
			msg.History.Events[1].GetSignalExternalWorkflowExecutionInitiatedEventAttributes().Namespace)
	})

	t.Run("scoped NamespaceInfo.Name allowlist", func(t *testing.T) {
		t.Parallel()

		msg := &workflowservicev1.DescribeNamespaceResponse{
			NamespaceInfo: &namespacepb.NamespaceInfo{
				Name: "payments",
				Id:   "uuid-stays",
			},
		}
		tr.ToRemote(msg)

		require.Equal(t, "R(payments)", msg.NamespaceInfo.Name)
		require.Equal(t, "uuid-stays", msg.NamespaceInfo.Id, "scoped allowlist must not touch sibling fields")
	})

	t.Run("repeated message field walks each element", func(t *testing.T) {
		t.Parallel()

		msg := &workflowservicev1.ListNamespacesResponse{
			Namespaces: []*workflowservicev1.DescribeNamespaceResponse{
				{NamespaceInfo: &namespacepb.NamespaceInfo{Name: "payments"}},
				{NamespaceInfo: &namespacepb.NamespaceInfo{Name: "billing"}},
			},
		}
		tr.ToRemote(msg)

		require.Equal(t, "R(payments)", msg.Namespaces[0].NamespaceInfo.Name)
		require.Equal(t, "R(billing)", msg.Namespaces[1].NamespaceInfo.Name)
	})

	t.Run("empty namespace stays empty", func(t *testing.T) {
		t.Parallel()

		msg := &workflowservicev1.StartWorkflowExecutionRequest{Namespace: ""}
		tr.ToRemote(msg)

		require.Empty(t, msg.Namespace, "proto3 treats empty string as unset; translator must not touch it")
	})

	t.Run("nil msg is a no-op", func(t *testing.T) {
		t.Parallel()

		require.NotPanics(t, func() { tr.ToRemote(nil) })
	})

	t.Run("nil mapper falls back to identity", func(t *testing.T) {
		t.Parallel()

		idTr := namespace.New(nil)
		msg := &workflowservicev1.StartWorkflowExecutionRequest{Namespace: "payments"}

		require.NotPanics(t, func() { idTr.ToRemote(msg) })
		require.Equal(t, "payments", msg.Namespace, "identity mapper must leave the value unchanged")
	})
}

func TestTranslator_ToLocal(t *testing.T) {
	t.Parallel()

	tr := namespace.New(testMapper{})

	t.Run("dispatches to Mapper.Local, not Remote", func(t *testing.T) {
		t.Parallel()

		msg := &workflowservicev1.StartWorkflowExecutionRequest{Namespace: "payments"}
		tr.ToLocal(msg)

		require.Equal(t, "L(payments)", msg.Namespace)
	})

	t.Run("round-trip Local then Remote uses both directions", func(t *testing.T) {
		t.Parallel()

		msg := &workflowservicev1.StartWorkflowExecutionRequest{Namespace: "payments"}

		tr.ToLocal(msg)
		require.Equal(t, "L(payments)", msg.Namespace)

		tr.ToRemote(msg)
		require.Equal(t, "R(L(payments))", msg.Namespace)
	})
}

func TestIsKnownProtoField(t *testing.T) {
	t.Parallel()

	const (
		nsInfo = "temporal.api.namespace.v1.NamespaceInfo"
		swer   = "temporal.api.workflowservice.v1.StartWorkflowExecutionRequest"
	)

	tests := []struct {
		name       string
		field      string
		parent     string
		want       bool
		wantReason string
	}{
		{name: "global namespace field", field: "namespace", parent: swer, want: true},
		{name: "global workflow_namespace", field: "workflow_namespace", parent: swer, want: true},
		{name: "global parent_workflow_namespace", field: "parent_workflow_namespace", parent: swer, want: true},
		{name: "global deleted_namespace", field: "deleted_namespace", parent: swer, want: true},
		{name: "scoped NamespaceInfo.name", field: "name", parent: nsInfo, want: true},
		{name: "name on unrelated message is not scoped", field: "name", parent: swer, want: false},
		{
			name: "ignored namespace_id", field: "namespace_id", parent: swer, want: true,
			wantReason: "namespace_id is in ignoredNamespaceFields, which IsKnownProtoField reports as known so the coverage test skips it",
		},
		{name: "ignored namespace_ids", field: "namespace_ids", parent: swer, want: true},
		{name: "ignored parent_workflow_namespace_id", field: "parent_workflow_namespace_id", parent: swer, want: true},
		{name: "unrelated field", field: "task_queue", parent: swer, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, namespace.IsKnownProtoField(tt.field, tt.parent), tt.wantReason)
		})
	}
}

func (testMapper) Local(s string) string  { return "L(" + s + ")" }
func (testMapper) Remote(s string) string { return "R(" + s + ")" }
