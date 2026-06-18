package main

import (
	"bytes"
	"go/format"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// Synthetic message-like fixtures for the visitor render-model tests. Keep in
// sync with the equivalent copy in internal/protoutil/graph_test.go, which
// exercises the message-graph helpers built on these same shapes.
type (
	vtLeaf struct{}

	vtChild struct{}

	vtDead struct{}

	vtRoot struct{}

	vtNode struct{} // self-referential
)

func TestImportManagerAliases(t *testing.T) {
	t.Parallel()

	im := newImportManager()

	require.Equal(t, "common", im.alias("go.temporal.io/api/common/v1"))
	require.Equal(t, "failure", im.alias("go.temporal.io/api/failure/v1"))
	require.Equal(t, "common", im.alias("go.temporal.io/api/common/v1"), "stable for repeat calls")

	// Non-versioned path uses the last segment.
	require.Equal(t, "proto", im.alias("google.golang.org/protobuf/proto"))

	// Distinct packages whose derived base alias collides must get unique
	// numeric suffixes so the generated file's imports never collide.
	require.Equal(t, "common2", im.alias("example.com/a/common/v1"))
	require.Equal(t, "common3", im.alias("example.com/b/common/v1"))
}

func TestHelperName(t *testing.T) {
	t.Parallel()

	im := newImportManager()
	name := helperName("Payloads", im, reflect.TypeFor[vtLeaf]())
	require.Contains(t, name, "visitPayloads_")
	require.Contains(t, name, "vtLeaf")
}

func TestBuildVisitorFileModel(t *testing.T) {
	t.Parallel()

	targets := []targetSpec{{Name: "Leaves", Type: reflect.TypeFor[vtLeaf]()}}
	roots := []reflect.Type{reflect.TypeFor[vtRoot]()}

	vf := buildVisitorFile("proxy", roots, targets)

	require.Equal(t, "proxy", vf.Package)
	require.Len(t, vf.Targets, 1)

	tv := vf.Targets[0]
	require.Equal(t, "Leaves", tv.Func)
	require.Contains(t, tv.TargetExpr, "vtLeaf")

	// vtRoot reaches the target, so it is a dispatcher case.
	require.Len(t, tv.Roots, 1)
	require.Contains(t, tv.Roots[0].BareExpr, "vtRoot")

	helpers := map[string]helperFunc{}
	for _, h := range tv.Helpers {
		helpers[h.Name] = h
	}

	// Helper exists for vtRoot and vtChild (both reach the target) but not for
	// vtLeaf (the target itself) nor vtDead (pruned).
	rootHelper := helperName("Leaves", newImportManager(), reflect.TypeFor[vtRoot]())
	childHelper := helperName("Leaves", newImportManager(), reflect.TypeFor[vtChild]())
	leafHelper := helperName("Leaves", newImportManager(), reflect.TypeFor[vtLeaf]())
	require.Contains(t, helpers, rootHelper)
	require.Contains(t, helpers, childHelper)
	require.NotContains(t, helpers, leafHelper, "target type must not get a helper")

	// vtRoot's helper: GetLeaves/GetByName are target ops; GetChild recurses;
	// GetDead is pruned.
	ops := map[string]visitOp{}
	for _, op := range helpers[rootHelper].Ops {
		ops[op.Getter] = op
	}
	require.True(t, ops["GetLeaves"].Iter)
	require.True(t, ops["GetLeaves"].IsTarget)
	require.True(t, ops["GetByName"].Iter)
	require.True(t, ops["GetByName"].IsTarget)
	require.False(t, ops["GetChild"].IsTarget)
	require.NotEmpty(t, ops["GetChild"].Helper)
	require.NotContains(t, ops, "GetDead", "dead-end edge must be pruned")
}

func TestBuildVisitorFileCycleTerminates(t *testing.T) {
	t.Parallel()

	targets := []targetSpec{{Name: "Leaves", Type: reflect.TypeFor[vtLeaf]()}}
	roots := []reflect.Type{reflect.TypeFor[vtNode]()}

	vf := buildVisitorFile("proxy", roots, targets)
	require.Len(t, vf.Targets, 1)

	nodeHelper := helperName("Leaves", newImportManager(), reflect.TypeFor[vtNode]())
	var found helperFunc
	for _, h := range vf.Targets[0].Helpers {
		if h.Name == nodeHelper {
			found = h
		}
	}
	require.Equal(t, nodeHelper, found.Name)

	// The node helper recurses into itself (GetNext) and cb's GetLeaf.
	ops := map[string]visitOp{}
	for _, op := range found.Ops {
		ops[op.Getter] = op
	}
	require.Equal(t, found.Name, ops["GetNext"].Helper, "self-recursion")
	require.True(t, ops["GetLeaf"].IsTarget)
}

func TestRenderVisitorsCompiles(t *testing.T) {
	t.Parallel()

	roots, err := serviceRoots()
	require.NoError(t, err)
	require.NotEmpty(t, roots)

	for typ, target := range visitorTargets {
		t.Run(typ, func(t *testing.T) {
			t.Parallel()

			vf := buildVisitorFile("visit", roots, []targetSpec{target})

			for _, tmpl := range []string{"visitors", "visitors_test"} {
				var buf bytes.Buffer
				require.NoError(t, render(&buf, tmpl, vf), "render %s", tmpl)

				_, ferr := format.Source(buf.Bytes())
				require.NoError(t, ferr, "generated %s is not valid Go:\n%s", tmpl, buf.String())
			}

			var buf bytes.Buffer
			require.NoError(t, render(&buf, "visitors", vf))
			src := buf.String()
			require.Contains(t, src, "func "+target.Name+"(")
			require.Contains(t, src, "proto.Message")

			var testBuf bytes.Buffer
			require.NoError(t, render(&testBuf, "visitors_test", vf))
			testSrc := testBuf.String()
			require.NotContains(t, testSrc, "google.golang.org/protobuf/proto", "generated test must not import unused proto")
			require.Contains(t, testSrc, "go.temporal.io/api/common/v1", "generated test needs the target package")
			require.Contains(t, testSrc, "go.temporal.io/api/workflowservice/v1", "generated test needs the root package")
		})
	}
}

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
