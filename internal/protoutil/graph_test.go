package protoutil_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
)

// Synthetic message-like fixtures for the graph helpers. Keep in sync with the
// equivalent copy in cmd/codegen/visitors_test.go, which exercises the visitor
// render model built on top of these same shapes.
type (
	vtLeaf struct{}

	vtChild struct{}

	vtDead struct{}

	vtRoot struct{}

	vtNode struct{} // self-referential
)

func (*vtLeaf) GetData() string { return "" } // no message edges

func (*vtChild) GetLeaf() *vtLeaf { return nil }

func (*vtDead) GetCount() int32 { return 0 } // cannot reach a target

func (*vtRoot) GetChild() *vtChild            { return nil } // singular message
func (*vtRoot) GetLeaves() []*vtLeaf          { return nil } // repeated message
func (*vtRoot) GetByName() map[string]*vtLeaf { return nil } // map message
func (*vtRoot) GetDead() *vtDead              { return nil } // dead-end message
func (*vtRoot) GetName() string               { return "" }  // scalar, ignored

func (*vtNode) GetNext() *vtNode { return nil }
func (*vtNode) GetLeaf() *vtLeaf { return nil }

func TestMessageEdges(t *testing.T) {
	t.Parallel()

	edges := protoutil.MessageEdges(reflect.TypeFor[vtRoot]())

	got := map[string]protoutil.Edge{}
	for _, e := range edges {
		got[e.Getter] = e
	}

	require.Contains(t, got, "GetChild")
	require.False(t, got["GetChild"].Iter)
	require.Equal(t, reflect.TypeFor[vtChild](), got["GetChild"].Elem)

	require.Contains(t, got, "GetLeaves")
	require.True(t, got["GetLeaves"].Iter)
	require.Equal(t, reflect.TypeFor[vtLeaf](), got["GetLeaves"].Elem)

	require.Contains(t, got, "GetByName")
	require.True(t, got["GetByName"].Iter)
	require.Equal(t, reflect.TypeFor[vtLeaf](), got["GetByName"].Elem)

	require.Contains(t, got, "GetDead")
	require.NotContains(t, got, "GetName", "scalar getters must be ignored")

	// Deterministic order (sorted by getter name).
	for i := 1; i < len(edges); i++ {
		require.Less(t, edges[i-1].Getter, edges[i].Getter)
	}
}

func TestBuildGraphDiscoversReachableTypes(t *testing.T) {
	t.Parallel()

	graph := protoutil.BuildGraph([]reflect.Type{reflect.TypeFor[vtRoot]()})

	require.Contains(t, graph, reflect.TypeFor[vtRoot]())
	require.Contains(t, graph, reflect.TypeFor[vtChild]())
	require.Contains(t, graph, reflect.TypeFor[vtLeaf]())
	require.Contains(t, graph, reflect.TypeFor[vtDead]())
}

func TestReachesTargetPrunesDeadEnds(t *testing.T) {
	t.Parallel()

	graph := protoutil.BuildGraph([]reflect.Type{reflect.TypeFor[vtRoot]()})
	reach := protoutil.ReachesTarget(graph, reflect.TypeFor[vtLeaf]())

	require.True(t, reach[reflect.TypeFor[vtLeaf]()])
	require.True(t, reach[reflect.TypeFor[vtChild]()])
	require.True(t, reach[reflect.TypeFor[vtRoot]()])
	require.False(t, reach[reflect.TypeFor[vtDead]()], "vtDead cannot reach the target and must be pruned")
}

func TestReachesTargetHandlesCycles(t *testing.T) {
	t.Parallel()

	graph := protoutil.BuildGraph([]reflect.Type{reflect.TypeFor[vtNode]()})
	reach := protoutil.ReachesTarget(graph, reflect.TypeFor[vtLeaf]())

	require.True(t, reach[reflect.TypeFor[vtNode]()])
	require.True(t, reach[reflect.TypeFor[vtLeaf]()])
}

func TestService_MessageTypes(t *testing.T) {
	t.Parallel()

	t.Run("dedups request and response types", func(t *testing.T) {
		t.Parallel()

		svc := &protoutil.Service{
			Name: "fake",
			RPCs: []protoutil.RPC{
				{Name: "A", Unary: true, Req: reflect.TypeFor[vtRoot](), Resp: reflect.TypeFor[vtChild]()},
				{Name: "B", Unary: true, Req: reflect.TypeFor[vtRoot](), Resp: reflect.TypeFor[vtLeaf]()}, // vtRoot repeats
			},
		}

		got, err := svc.MessageTypes()
		require.NoError(t, err)
		require.ElementsMatch(t, []reflect.Type{
			reflect.TypeFor[vtRoot](),
			reflect.TypeFor[vtChild](),
			reflect.TypeFor[vtLeaf](),
		}, got)
	})

	t.Run("errors when the service has no RPCs", func(t *testing.T) {
		t.Parallel()

		svc := &protoutil.Service{Name: "empty"}
		_, err := svc.MessageTypes()
		require.Error(t, err)
	})

	t.Run("real WorkflowService", func(t *testing.T) {
		t.Parallel()

		svc, err := protoutil.ParseService(reflect.TypeFor[workflowservice.WorkflowServiceServer]())
		require.NoError(t, err)

		roots, err := svc.MessageTypes()
		require.NoError(t, err)
		require.NotEmpty(t, roots)
	})
}
