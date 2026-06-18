package main

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/urfave/cli/v3"
	"go.temporal.io/api/common/v1"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
)

const protoPkgPath = "google.golang.org/protobuf/proto"

// visitorTargets maps the --type flag value to the message type we generate a
// visitor for. The key is the bare proto type name; Name stays plural so the
// generated dispatcher reads <Name> (e.g. Payloads).
var visitorTargets = map[string]targetSpec{
	"Payload":  {Name: "Payloads", Type: reflect.TypeFor[common.Payload]()},
	"DataBlob": {Name: "DataBlobs", Type: reflect.TypeFor[common.DataBlob]()},
	"Memo":     {Name: "Memos", Type: reflect.TypeFor[common.Memo]()},
}

type (
	importSpec struct {
		Alias string
		Path  string
	}

	importManager struct {
		byPath  map[string]string
		aliases map[string]bool
		order   []string
	}

	targetSpec struct {
		Name string       // noun used in Visit<Name> / visit<Name>_; e.g. "Payloads"
		Type reflect.Type // the matched message type; e.g. common.Payload
	}

	visitorFile struct {
		Package     string
		ProtoAlias  string
		Imports     []importSpec // for the generated visitors file
		TestImports []importSpec // subset used by the generated test file (target + root packages)
		Targets     []targetVisitor
	}

	targetVisitor struct {
		Func       string // exported dispatcher, e.g. "VisitPayloads"
		TargetExpr string // e.g. "common.Payload"
		Roots      []rootCase
		Helpers    []helperFunc
	}

	rootCase struct {
		BareExpr string // e.g. "workflowservice.StartWorkflowExecutionRequest" (no leading *)
		Helper   string // helper to call for this root
		TestName string // unique test function suffix, e.g. "StartWorkflowExecutionRequest"
	}

	helperFunc struct {
		Name       string // e.g. "visitPayloads_failure_Failure"
		RecvExpr   string // e.g. "*failure.Failure"
		TargetExpr string // e.g. "common.Payload"
		Ops        []visitOp
	}

	visitOp struct {
		Getter   string // getter method name
		Iter     bool   // range (repeated/map) vs singular
		IsTarget bool   // cb(v) when true; recurse via Helper when false
		Helper   string // helper to call when !IsTarget
	}
)

// alias returns a stable, unique import alias for pkgPath. When the derived base
// alias is already taken by a different package, a numeric suffix is appended so
// the generated file's imports never collide.
func (im *importManager) alias(pkgPath string) string {
	if a, ok := im.byPath[pkgPath]; ok {
		return a
	}

	base := aliasBase(pkgPath)
	a := base
	for n := 2; im.aliases[a]; n++ {
		a = fmt.Sprintf("%s%d", base, n)
	}

	im.byPath[pkgPath] = a
	im.aliases[a] = true
	im.order = append(im.order, pkgPath)
	return a
}

// specs returns the registered imports in insertion order.
func (im *importManager) specs() []importSpec {
	out := make([]importSpec, 0, len(im.order))
	for _, p := range im.order {
		out = append(out, importSpec{Alias: im.byPath[p], Path: p})
	}
	return out
}

func visitorsCommand() *cli.Command {
	return &cli.Command{
		Name:  "visitors",
		Usage: "Generates field visitors for a target message type across all services",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "out", Aliases: []string{"o"}, Usage: "Where to write the output file", Required: true},
			&cli.StringFlag{Name: "type", Aliases: []string{"t"}, Usage: "The message type to visit (Payload, DataBlob, Memo)", Required: true},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			target, ok := visitorTargets[cmd.String("type")]
			if !ok {
				return fmt.Errorf("unknown type: %s", cmd.String("type"))
			}

			roots, err := serviceRoots()
			if err != nil {
				return err
			}

			vf := buildVisitorFile("visit", roots, []targetSpec{target})
			for tmpl, path := range map[string]string{
				"visitors":      cmd.String("out"),
				"visitors_test": strings.TrimSuffix(cmd.String("out"), ".go") + "_test.go",
			} {
				if err := writeFile(tmpl, path, vf); err != nil {
					return err
				}
			}

			return nil
		},
	}
}

// serviceRoots returns the de-duplicated request/response message types across
// every registered service. A single dispatcher must switch over all of them,
// so visitors are generated against the union rather than one service.
func serviceRoots() ([]reflect.Type, error) {
	seen := map[reflect.Type]bool{}
	var roots []reflect.Type
	for _, name := range slices.Sorted(maps.Keys(serviceMap)) {
		s, err := protoutil.ParseService(serviceMap[name].Type)
		if err != nil {
			return nil, err
		}

		rs, err := s.MessageTypes()
		if err != nil {
			return nil, err
		}

		for _, r := range rs {
			if !seen[r] {
				seen[r] = true
				roots = append(roots, r)
			}
		}
	}

	return roots, nil
}

// buildVisitorFile builds the full render model for a visitors file: a
// dispatcher and helper set per target over the message graph rooted at roots.
func buildVisitorFile(pkg string, roots []reflect.Type, targets []targetSpec) *visitorFile {
	graph := protoutil.BuildGraph(roots)
	im := newImportManager()

	vf := &visitorFile{Package: pkg}
	vf.ProtoAlias = im.alias(protoPkgPath)

	testPaths := map[string]bool{}

	for _, ts := range targets {
		reach := protoutil.ReachesTarget(graph, ts.Type)

		tv := targetVisitor{
			Func:       ts.Name,
			TargetExpr: typeExpr(im, ts.Type),
		}

		testPaths[ts.Type.PkgPath()] = true

		for _, r := range sortedReaching(roots, reach) {
			tv.Roots = append(tv.Roots, rootCase{
				BareExpr: typeExpr(im, r),
				Helper:   helperName(ts.Name, im, r),
				TestName: r.Name(),
			})
			testPaths[r.PkgPath()] = true
		}

		for _, ht := range sortedHelperTypes(graph, reach, ts.Type) {
			h := helperFunc{
				Name:       helperName(ts.Name, im, ht),
				RecvExpr:   "*" + typeExpr(im, ht),
				TargetExpr: tv.TargetExpr,
			}
			for _, e := range graph[ht] {
				if !reach[e.Elem] {
					continue // prune edges that cannot reach the target
				}
				op := visitOp{Getter: e.Getter, Iter: e.Iter}
				if e.Elem == ts.Type {
					op.IsTarget = true
				} else {
					op.Helper = helperName(ts.Name, im, e.Elem)
				}
				h.Ops = append(h.Ops, op)
			}
			tv.Helpers = append(tv.Helpers, h)
		}

		vf.Targets = append(vf.Targets, tv)
	}

	vf.Imports = im.specs()
	for _, s := range vf.Imports {
		if testPaths[s.Path] {
			vf.TestImports = append(vf.TestImports, s)
		}
	}
	return vf
}

// sortedReaching returns the roots that can reach target, in stable order.
func sortedReaching(roots []reflect.Type, reach map[reflect.Type]bool) []reflect.Type {
	var out []reflect.Type
	for _, r := range roots {
		if reach[r] {
			out = append(out, r)
		}
	}
	sortTypes(out)
	return out
}

// sortedHelperTypes returns every type that gets a helper (reaches target, but
// is not the target), in stable order.
func sortedHelperTypes(graph map[reflect.Type][]protoutil.Edge, reach map[reflect.Type]bool, target reflect.Type) []reflect.Type {
	var out []reflect.Type
	for t := range graph {
		if t != target && reach[t] {
			out = append(out, t)
		}
	}
	sortTypes(out)
	return out
}

// typeExpr returns "alias.TypeName" for t, registering the import as a side effect.
func typeExpr(im *importManager, t reflect.Type) string {
	return im.alias(t.PkgPath()) + "." + t.Name()
}

// helperName returns the unexported helper identifier for (target, t),
// qualified by the type's package alias to stay unique across packages.
func helperName(target string, im *importManager, t reflect.Type) string {
	return fmt.Sprintf("visit%s_%s_%s", target, im.alias(t.PkgPath()), t.Name())
}

func newImportManager() *importManager {
	return &importManager{byPath: map[string]string{}, aliases: map[string]bool{}}
}

func sortTypes(ts []reflect.Type) {
	slices.SortFunc(ts, func(a, b reflect.Type) int {
		if c := strings.Compare(a.PkgPath(), b.PkgPath()); c != 0 {
			return c
		}
		return strings.Compare(a.Name(), b.Name())
	})
}

func aliasBase(pkgPath string) string {
	parts := strings.Split(pkgPath, "/")
	last := parts[len(parts)-1]
	if len(parts) >= 2 && isVersionSegment(last) {
		return sanitizeAlias(parts[len(parts)-2])
	}
	return sanitizeAlias(last)
}

func isVersionSegment(s string) bool {
	if len(s) < 2 || s[0] != 'v' {
		return false
	}
	for _, r := range s[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func sanitizeAlias(s string) string {
	return strings.NewReplacer("-", "", ".", "").Replace(s)
}
