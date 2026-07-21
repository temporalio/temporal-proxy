package protoutil

import (
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// namespaceNameOverrides lists namespace-name string fields whose names do not
// end in "namespace" and so are missed by the suffix rule. Keyed by the fully
// qualified message type, then by field name. NamespaceInfo.name carries the
// namespace name returned by DescribeNamespace and ListNamespaces, both of which
// are WorkflowService methods that flow through the proxy.
var namespaceNameOverrides = map[protoreflect.FullName]map[protoreflect.Name]bool{
	"temporal.api.namespace.v1.NamespaceInfo": {"name": true},
}

type (
	// nsPlan is the pre-computed set of namespace-name locations in a single
	// message type: direct string fields to translate, and message-typed fields
	// whose own plan is non-empty. A plan with neither is empty, marking a type
	// that carries no namespace anywhere and can be skipped entirely.
	nsPlan struct {
		names    []protoreflect.FieldDescriptor
		children []nsChild
	}

	// nsChild is a message-, repeated-message-, or map-valued field whose value
	// type carries a namespace somewhere, paired with the plan for that value type.
	nsChild struct {
		field protoreflect.FieldDescriptor
		plan  *nsPlan
	}
)

// empty reports whether the plan has no namespace locations at all.
func (p *nsPlan) empty() bool {
	return len(p.names) == 0 && len(p.children) == 0
}

// apply walks m following the plan and rewrites every namespace-name value with
// fn. Empty string values are left untouched so an unset namespace is not
// wrapped. Fields absent from m are skipped.
func (p *nsPlan) apply(m protoreflect.Message, fn func(string) string) {
	for _, fd := range p.names {
		if fd.IsList() {
			list := m.Get(fd).List()
			for i := 0; i < list.Len(); i++ {
				if s := list.Get(i).String(); s != "" {
					list.Set(i, protoreflect.ValueOfString(fn(s)))
				}
			}
			continue
		}

		if !m.Has(fd) {
			continue
		}
		if s := m.Get(fd).String(); s != "" {
			m.Set(fd, protoreflect.ValueOfString(fn(s)))
		}
	}

	for _, c := range p.children {
		if !m.Has(c.field) {
			continue
		}

		v := m.Get(c.field)
		switch {
		case c.field.IsMap():
			v.Map().Range(func(_ protoreflect.MapKey, val protoreflect.Value) bool {
				c.plan.apply(val.Message(), fn)
				return true
			})
		case c.field.IsList():
			list := v.List()
			for i := 0; i < list.Len(); i++ {
				c.plan.apply(list.Get(i).Message(), fn)
			}
		default:
			c.plan.apply(v.Message(), fn)
		}
	}
}

// buildPlan computes the plan for md. memo breaks recursive type graphs and is
// shared across the whole build; callers pass a fresh memo per top-level build.
// It links every message child first, then prunes edges that carry no namespace.
func buildPlan(md protoreflect.MessageDescriptor, memo map[protoreflect.FullName]*nsPlan) *nsPlan {
	root := buildNode(md, memo)
	pruneDead(memo)
	return root
}

// buildNode builds the raw plan graph, recording md's plan in memo before its
// fields are walked so a type that transitively contains itself references the
// same plan instead of recursing forever. Every message-typed field is linked
// unconditionally here; pruneDead removes the ones that carry no namespace.
func buildNode(md protoreflect.MessageDescriptor, memo map[protoreflect.FullName]*nsPlan) *nsPlan {
	if p, ok := memo[md.FullName()]; ok {
		return p
	}

	p := &nsPlan{}
	memo[md.FullName()] = p

	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)

		if fd.Kind() == protoreflect.StringKind && isNamespaceName(md, fd) {
			p.names = append(p.names, fd)
			continue
		}

		if sub := childMessage(fd); sub != nil {
			p.children = append(p.children, nsChild{field: fd, plan: buildNode(sub, memo)})
		}
	}

	return p
}

// pruneDead removes child edges that lead to no namespace, computed as a
// fixpoint so cyclic type graphs converge: a plan is live when it has a name
// field or a live child. Dead nodes are left empty, so apply skips them and the
// zero-cost fast path is preserved.
func pruneDead(memo map[protoreflect.FullName]*nsPlan) {
	live := make(map[*nsPlan]bool, len(memo))
	for _, p := range memo {
		if len(p.names) > 0 {
			live[p] = true
		}
	}

	for changed := true; changed; {
		changed = false
		for _, p := range memo {
			if live[p] {
				continue
			}
			for _, c := range p.children {
				if live[c.plan] {
					live[p] = true
					changed = true
					break
				}
			}
		}
	}

	for _, p := range memo {
		kept := p.children[:0]
		for _, c := range p.children {
			if live[c.plan] {
				kept = append(kept, c)
			}
		}
		p.children = kept
	}
}

// childMessage returns the message descriptor a field recurses into: the element
// type for a repeated message, the value type for a map with a message value, or
// the message type for a singular message field. It returns nil for scalar
// fields and for maps whose values are not messages.
func childMessage(fd protoreflect.FieldDescriptor) protoreflect.MessageDescriptor {
	if fd.IsMap() {
		if v := fd.MapValue(); v.Kind() == protoreflect.MessageKind {
			return v.Message()
		}
		return nil
	}

	if fd.Kind() == protoreflect.MessageKind {
		return fd.Message()
	}

	return nil
}

// isNamespaceName reports whether fd (declared on md) is a string field that
// carries a namespace name. A field carries a namespace name when its proto name
// ends in "namespace" (which excludes "*namespace_id", since those end in "_id"),
// or when an explicit override lists it. Only string fields qualify.
func isNamespaceName(md protoreflect.MessageDescriptor, fd protoreflect.FieldDescriptor) bool {
	if fd.Kind() != protoreflect.StringKind {
		return false
	}

	if strings.HasSuffix(string(fd.Name()), "namespace") {
		return true
	}

	return namespaceNameOverrides[md.FullName()][fd.Name()]
}
