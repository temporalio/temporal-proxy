package protoutil

import (
	"reflect"
	"slices"
	"strings"
)

// Edge is a message-typed Get* getter on a proto message struct: an outgoing
// edge from one message type to another in the message graph.
type Edge struct {
	// Getter is the getter method name, e.g. "GetInput".
	Getter string
	// Elem is the child message struct type (not the pointer).
	Elem reflect.Type
	// Iter reports whether the getter is repeated or map-valued (visited with a
	// range) rather than a singular message.
	Iter bool
}

// MessageEdges returns the message-typed Get* getters of struct type t, sorted
// by getter name for deterministic output. A getter is a message edge when it
// returns *T, []*T, or map[K]*T where T is a struct.
func MessageEdges(t reflect.Type) []Edge {
	pt := reflect.PointerTo(t)

	var edges []Edge
	for m := range pt.Methods() {
		if !strings.HasPrefix(m.Name, "Get") {
			continue
		}

		ft := m.Type // method value type carries the receiver as In(0)
		if ft.NumIn() != 1 || ft.NumOut() != 1 {
			continue
		}

		elem, iter, ok := classifyReturn(ft.Out(0))
		if !ok {
			continue
		}

		edges = append(edges, Edge{Getter: m.Name, Elem: elem, Iter: iter})
	}

	slices.SortFunc(edges, func(a, b Edge) int { return strings.Compare(a.Getter, b.Getter) })
	return edges
}

// BuildGraph discovers every message type reachable from roots by following
// message edges, returning each type's outgoing edges keyed by type.
func BuildGraph(roots []reflect.Type) map[reflect.Type][]Edge {
	graph := map[reflect.Type][]Edge{}

	var visit func(t reflect.Type)
	visit = func(t reflect.Type) {
		if _, seen := graph[t]; seen {
			return
		}

		edges := MessageEdges(t)
		graph[t] = edges
		for _, e := range edges {
			visit(e.Elem)
		}
	}

	for _, r := range roots {
		visit(r)
	}

	return graph
}

// ReachesTarget returns the set of types that are target or can reach it through
// the graph. target is included in the set but is only ever a leaf for callers
// (it never has outgoing edges that matter for reachability).
func ReachesTarget(graph map[reflect.Type][]Edge, target reflect.Type) map[reflect.Type]bool {
	parents := map[reflect.Type][]reflect.Type{}
	for t, edges := range graph {
		for _, e := range edges {
			parents[e.Elem] = append(parents[e.Elem], t)
		}
	}

	reach := map[reflect.Type]bool{target: true}
	queue := []reflect.Type{target}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, p := range parents[cur] {
			if !reach[p] {
				reach[p] = true
				queue = append(queue, p)
			}
		}
	}

	return reach
}

// classifyReturn reports the element struct type of a message-shaped getter
// return (*T, []*T, map[K]*T) and whether it requires iteration. ok is false
// for any other return type.
func classifyReturn(out reflect.Type) (elem reflect.Type, iter bool, ok bool) {
	switch out.Kind() {
	case reflect.Pointer:
		if out.Elem().Kind() == reflect.Struct {
			return out.Elem(), false, true
		}
	case reflect.Slice:
		if e := out.Elem(); e.Kind() == reflect.Pointer && e.Elem().Kind() == reflect.Struct {
			return e.Elem(), true, true
		}
	case reflect.Map:
		if e := out.Elem(); e.Kind() == reflect.Pointer && e.Elem().Kind() == reflect.Struct {
			return e.Elem(), true, true
		}
	}

	return nil, false, false
}
