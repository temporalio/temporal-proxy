package protoutil

import (
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type (
	// Extractor reads the namespace from an encoded gRPC request. It resolves
	// the concrete request message type for a full method name from the
	// descriptor and type registries it was given, decodes the payload into it,
	// and reads the namespace via the generated accessor. Resolved types are
	// cached per method, so repeat calls for the same method skip the lookup.
	// Construct one with NewExtractor; an Extractor is safe for concurrent use.
	Extractor struct {
		msgs  sync.Map // fullMethod -> protoreflect.MessageType (nil when unresolvable)
		files Files
		types Types
	}

	// Files resolves a descriptor by its fully qualified name. *protoregistry.Files,
	// and in particular protoregistry.GlobalFiles, implements it.
	Files interface {
		FindDescriptorByName(protoreflect.FullName) (protoreflect.Descriptor, error)
	}

	// Types resolves a message type by its fully qualified name. *protoregistry.Types,
	// and in particular protoregistry.GlobalTypes, implements it.
	Types interface {
		FindMessageByName(protoreflect.FullName) (protoreflect.MessageType, error)
	}

	// namespacer is implemented by the generated request types that carry a
	// namespace. Requests without one (e.g. GetSystemInfo) do not implement it,
	// so they yield an empty namespace.
	namespacer interface {
		GetNamespace() string
	}
)

// NewExtractor returns an Extractor that resolves request types using files and
// types. Production callers pass protoregistry.GlobalFiles and
// protoregistry.GlobalTypes; tests may pass fakes. Resolution only succeeds for
// message types registered in the given registries, so the relevant service
// packages (e.g. go.temporal.io/api/workflowservice/v1) must be imported into
// the binary.
func NewExtractor(files Files, types Types) *Extractor {
	return &Extractor{
		files: files,
		types: types,
	}
}

// Namespace returns the namespace carried by an encoded request for fullMethod,
// or the empty string when the method is unknown, the request type carries no
// namespace, or the payload cannot be decoded.
func (e *Extractor) Namespace(fullMethod string, payload []byte) string {
	mt := e.messageType(fullMethod)
	if mt == nil {
		return ""
	}

	msg := mt.New().Interface()
	if err := proto.Unmarshal(payload, msg); err != nil {
		return ""
	}

	ns, ok := msg.(namespacer)
	if !ok {
		return ""
	}

	return ns.GetNamespace()
}

// messageType returns the request message type for fullMethod, or nil when it
// cannot be resolved. The result, including a nil miss, is cached so each method
// is resolved at most once.
func (e *Extractor) messageType(fullMethod string) protoreflect.MessageType {
	if v, ok := e.msgs.Load(fullMethod); ok {
		if v == nil {
			return nil
		}

		return v.(protoreflect.MessageType)
	}

	mt := e.resolveInputType(fullMethod)
	e.msgs.Store(fullMethod, mt)
	return mt
}

// resolveInputType looks up the request message type for a "/pkg.Service/Method"
// name via the extractor's Files and Types, returning nil when the name is
// malformed or the service, method, or message type is not resolvable.
func (e *Extractor) resolveInputType(fullMethod string) protoreflect.MessageType {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	slash := strings.LastIndex(trimmed, "/")
	if slash < 0 {
		return nil
	}

	service := trimmed[:slash]
	method := trimmed[slash+1:]

	d, err := e.files.FindDescriptorByName(protoreflect.FullName(service))
	if err != nil {
		return nil
	}

	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil
	}

	md := sd.Methods().ByName(protoreflect.Name(method))
	if md == nil {
		return nil
	}

	mt, err := e.types.FindMessageByName(md.Input().FullName())
	if err != nil {
		return nil
	}

	return mt
}
