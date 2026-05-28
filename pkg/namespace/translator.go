package namespace

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	// namespaceNameFields lists proto field names that, wherever they appear,
	// carry a Temporal namespace name. Adding an entry expands the set of fields
	// the translator rewrites.
	namespaceNameFields = map[string]struct{}{
		"namespace":                 {},
		"workflow_namespace":        {},
		"parent_workflow_namespace": {},
		"deleted_namespace":         {},
	}

	// ignoredNamespaceFields contains field with the word namespace in them, but
	// which are not to be translated.
	//
	// These fields quack like namespace fields, but they aren't ducks.
	ignoredNamespaceFields = map[string]struct{}{
		"namespace_id":                 {},
		"namespace_ids":                {},
		"parent_namespace_id":          {},
		"parent_workflow_namespace_id": {},
	}

	// messageScopedNamespaceFields handles the cases where a namespace name lives
	// in a generically named field (e.g. NamespaceInfo.name) that would be unsafe
	// to add to the global allowlist.
	messageScopedNamespaceFields = map[protoreflect.FullName]map[string]struct{}{
		"temporal.api.namespace.v1.NamespaceInfo": {"name": {}},
	}
)

type (
	// Translator rewrites namespace-name string fields inside any
	// [proto.Message] by delegating each value to a [Mapper]. It is safe to
	// use concurrently as long as the underlying Mapper is.
	Translator struct {
		mapper Mapper
	}

	// Mapper converts namespace names between the proxy's local view and its
	// remote (upstream) view. Implementations must be deterministic and should
	// be cheap to call, since a single message may invoke them many times.
	Mapper interface {
		// Local returns the local namespace name for a remote one.
		Local(string) string
		// Remote returns the remote namespace name for a local one.
		Remote(string) string
	}

	nilMapper struct{}
)

// New returns a [Translator] backed by m. A nil m is replaced with an
// identity mapper, so the returned Translator never panics on nil and leaves
// every namespace value unchanged.
func New(m Mapper) *Translator {
	t := &Translator{mapper: new(nilMapper)}
	if m != nil {
		t.mapper = m
	}

	return t
}

// IsKnownProtoField reports whether the translator recognizes the field named
// fieldName on the message type identified by parentName. A field is
// "recognized" if it is either translated (an entry in namespaceNameFields or
// the scoped allowlist) or explicitly ignored (an entry in
// ignoredNamespaceFields). parentName is the fully-qualified proto message
// name (e.g. "temporal.api.namespace.v1.NamespaceInfo").
func IsKnownProtoField(fieldName, parentName string) bool {
	if _, ok := ignoredNamespaceFields[fieldName]; ok {
		return true
	}
	return isNamespaceField(fieldName, messageScopedNamespaceFields[protoreflect.FullName(parentName)])
}

// ToLocal rewrites every namespace-name field in msg by passing the current
// value through [Mapper.Local]. A nil msg is a no-op.
func (t *Translator) ToLocal(msg proto.Message) {
	t.translate(msg, t.mapper.Local)
}

// ToRemote rewrites every namespace-name field in msg by passing the current
// value through [Mapper.Remote]. A nil msg is a no-op.
func (t *Translator) ToRemote(msg proto.Message) {
	t.translate(msg, t.mapper.Remote)
}

func (t *Translator) translate(msg proto.Message, fn func(string) string) {
	if t == nil || t.mapper == nil || msg == nil {
		return
	}

	t.walkMessage(msg.ProtoReflect(), fn)
}

func (t *Translator) walkMessage(m protoreflect.Message, fn func(string) string) {
	if !m.IsValid() {
		return
	}

	md := m.Descriptor()
	scoped := messageScopedNamespaceFields[md.FullName()]
	fields := md.Fields()

	for i := range fields.Len() {
		fd := fields.Get(i)
		if !m.Has(fd) {
			continue
		}

		switch {
		case fd.IsMap():
			t.walkMap(fd, m.Mutable(fd).Map(), fn)
		case fd.IsList():
			t.walkList(fd, m.Mutable(fd).List(), isNamespaceField(string(fd.Name()), scoped), fn)
		case fd.Kind() == protoreflect.MessageKind, fd.Kind() == protoreflect.GroupKind:
			t.walkMessage(m.Mutable(fd).Message(), fn)
		case fd.Kind() == protoreflect.StringKind:
			if isNamespaceField(string(fd.Name()), scoped) {
				m.Set(fd, protoreflect.ValueOfString(fn(m.Get(fd).String())))
			}
		}
	}
}

func (t *Translator) walkList(fd protoreflect.FieldDescriptor, list protoreflect.List, isNamespaceField bool, fn func(string) string) {
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		for i := range list.Len() {
			t.walkMessage(list.Get(i).Message(), fn)
		}
	case protoreflect.StringKind:
		if !isNamespaceField {
			return
		}
		for i := range list.Len() {
			list.Set(i, protoreflect.ValueOfString(fn(list.Get(i).String())))
		}
	}
}

func (t *Translator) walkMap(fd protoreflect.FieldDescriptor, m protoreflect.Map, fn func(string) string) {
	vk := fd.MapValue().Kind()
	if vk != protoreflect.MessageKind && vk != protoreflect.GroupKind {
		return
	}

	m.Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool {
		t.walkMessage(v.Message(), fn)
		return true
	})
}

func (m *nilMapper) Local(ns string) string  { return ns }
func (m *nilMapper) Remote(ns string) string { return ns }

// isNamespaceField reports whether a field's string value should be rewritten
// by the translator. Fields in ignoredNamespaceFields return false even though
// their names contain "namespace": they are identifiers, not names.
func isNamespaceField(name string, scoped map[string]struct{}) bool {
	if _, ok := namespaceNameFields[name]; ok {
		return true
	}

	if scoped != nil {
		if _, ok := scoped[name]; ok {
			return true
		}
	}

	return false
}
