package protoutil

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Translator rewrites namespace names inside decoded protobuf messages. It
// caches a translation plan per message type, so repeat translations of the
// same type skip descriptor traversal. Plans may be pre-populated with
// WarmService or built lazily on first use. A Translator is safe for concurrent
// use.
type Translator struct {
	files Files
	plans sync.Map // protoreflect.FullName -> *nsPlan
}

// NewTranslator returns a Translator that resolves service descriptors for
// warming through files. Production callers pass protoregistry.GlobalFiles.
func NewTranslator(files Files) *Translator {
	return &Translator{files: files}
}

// WarmService pre-computes and caches the plan for every method input and output
// type of the named gRPC service, so no request pays first-touch plan
// construction. It fails when the name does not resolve to a service.
func (t *Translator) WarmService(name protoreflect.FullName) error {
	d, err := t.files.FindDescriptorByName(name)
	if err != nil {
		return fmt.Errorf("protoutil: resolve service %q: %w", name, err)
	}

	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return fmt.Errorf("protoutil: %q is not a service", name)
	}

	methods := sd.Methods()
	for i := 0; i < methods.Len(); i++ {
		m := methods.Get(i)
		t.planFor(m.Input())
		t.planFor(m.Output())
	}

	return nil
}

// Translate rewrites every namespace name in m using fn. It is a no-op when m is
// nil, invalid, or carries no namespace field. fn maps a namespace name to its
// translated form (local to remote, or remote to local).
func (t *Translator) Translate(m proto.Message, fn func(string) string) {
	if m == nil || fn == nil {
		return
	}

	msg := m.ProtoReflect()
	if !msg.IsValid() {
		return
	}

	if p := t.planFor(msg.Descriptor()); !p.empty() {
		p.apply(msg, fn)
	}
}

// IsWarm reports whether a plan for the named message type is already cached.
func (t *Translator) IsWarm(name protoreflect.FullName) bool {
	_, ok := t.plans.Load(name)
	return ok
}

// planFor returns the cached plan for md, building and caching it on a miss.
func (t *Translator) planFor(md protoreflect.MessageDescriptor) *nsPlan {
	if v, ok := t.plans.Load(md.FullName()); ok {
		return v.(*nsPlan)
	}

	p := buildPlan(md, map[protoreflect.FullName]*nsPlan{})
	actual, _ := t.plans.LoadOrStore(md.FullName(), p)
	return actual.(*nsPlan)
}
