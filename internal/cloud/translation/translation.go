package translation

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/temporalio/temporal-proxy/internal/rpc"
)

type (
	// Translation stands one unary method in for another: it converts the
	// caller's request into the upstream's request type, allocates the reply the
	// upstream will fill, and folds that reply back into the message type the
	// caller is waiting on. Build one with [Adapt] rather than by hand, so the
	// conversions are written against concrete message types. A Translation holds
	// no per-call state and is safe for concurrent use.
	Translation struct {
		from, to string
		request  func(req proto.Message) (proto.Message, error)
		response func(req, upstream, reply proto.Message) error
		reply    func() proto.Message
		headers  map[string]string
	}

	// Registry is the set of translations an interceptor consults, keyed by the
	// inbound full method. It is fixed once built and safe for concurrent use; a
	// nil Registry translates nothing, so a caller with none to install can pass
	// one straight through.
	Registry struct {
		byMethod map[string]*Translation
	}
)

// Adapt builds a [Translation] from from onto to out of two typed conversions.
// request converts the caller's request into the upstream's; response folds the
// upstream's reply into the caller's, and is given the original request too,
// since a field the upstream has no equivalent for (a filter, say) can only be
// honoured on the way back. Both message types are inferred from the
// conversions, so a mapping never asserts on proto.Message itself.
func Adapt[Req, UpReq, UpResp, Resp proto.Message](
	from, to string,
	request func(Req) (UpReq, error),
	response func(Req, UpResp, Resp) error,
) *Translation {
	return &Translation{
		from: from,
		to:   to,
		reply: func() proto.Message {
			return newMessage[UpResp]()
		},
		request: func(m proto.Message) (proto.Message, error) {
			req, ok := m.(Req)
			if !ok {
				return nil, fmt.Errorf("translation: %s wanted a %s request, got %T", from, nameOf[Req](), m)
			}

			return request(req)
		},
		response: func(m, up, out proto.Message) error {
			req, ok := m.(Req)
			if !ok {
				return fmt.Errorf("translation: %s wanted a %s request, got %T", from, nameOf[Req](), m)
			}

			upstream, ok := up.(UpResp)
			if !ok {
				return fmt.Errorf("translation: %s wanted a %s reply, got %T", to, nameOf[UpResp](), up)
			}

			reply, ok := out.(Resp)
			if !ok {
				return fmt.Errorf("translation: %s wanted a %s reply, got %T", from, nameOf[Resp](), out)
			}

			return response(req, upstream, reply)
		},
	}
}

// WithHeader stamps key: value on the substituted call and returns t, so a
// mapping can declare the dialect the upstream method needs alongside the
// conversions themselves. It replaces any value the caller sent rather than
// adding to it: the caller did not ask for this upstream method and cannot know
// what its API expects, so its own header is not intent worth preserving. A
// caller invoking that API directly is forwarded untranslated and keeps its
// header.
//
// Headers travel only on a call this translation substituted; a method the
// registry does not translate is untouched.
func (t *Translation) WithHeader(key, value string) *Translation {
	if t.headers == nil {
		t.headers = make(map[string]string, 1)
	}

	t.headers[key] = value

	return t
}

// NewRegistry indexes ts by the method each translates from. It rejects a nil
// entry, a method name that is not a gRPC full method, a translation onto
// itself, and two translations of the same inbound method, so a mapping mistake
// surfaces at construction rather than on the first request that hits it.
func NewRegistry(ts ...*Translation) (*Registry, error) {
	byMethod := make(map[string]*Translation, len(ts))
	for i, t := range ts {
		if t == nil {
			return nil, fmt.Errorf("translation: nil translation at index %d", i)
		}

		from, err := canonical(t.from)
		if err != nil {
			return nil, err
		}

		to, err := canonical(t.to)
		if err != nil {
			return nil, err
		}

		if from == to {
			return nil, fmt.Errorf("translation: %s translates onto itself", from)
		}

		if _, dup := byMethod[from]; dup {
			return nil, fmt.Errorf("translation: %s is translated twice", from)
		}

		t.from, t.to = from, to
		byMethod[from] = t
	}

	return &Registry{byMethod: byMethod}, nil
}

// Lookup returns the translation registered for fullMethod, reporting false when
// there is none and the call should be forwarded unchanged. A nil Registry, or
// one built from no translations, always reports false.
func (r *Registry) Lookup(fullMethod string) (*Translation, bool) {
	if r == nil {
		return nil, false
	}

	t, ok := r.byMethod[fullMethod]
	return t, ok
}

// Methods returns the inbound methods the registry translates, in canonical
// "/pkg.Service/Method" form. Order is not significant.
func (r *Registry) Methods() []string {
	if r == nil {
		return nil
	}

	out := make([]string, 0, len(r.byMethod))
	for method := range r.byMethod {
		out = append(out, method)
	}

	return out
}

// stamp returns ctx with this translation's headers set on the outgoing
// metadata, or ctx unchanged when it has none. Set rather than append, so a
// value that arrived inbound and was forwarded cannot leave two on the wire.
func (t *Translation) stamp(ctx context.Context) context.Context {
	if len(t.headers) == 0 {
		return ctx
	}

	return rpc.WithOutgoing(ctx, func(md metadata.MD) {
		for key, value := range t.headers {
			md.Set(key, value)
		}
	})
}

// From is the inbound method this translation replaces.
func (t *Translation) From() string { return t.from }

// To is the upstream method that stands in for it.
func (t *Translation) To() string { return t.to }

// canonical returns fullMethod in the leading-slash "/pkg.Service/Method" form
// gRPC hands an interceptor, so a mapping written either way still matches the
// method the interceptor is asked about.
func canonical(fullMethod string) (string, error) {
	service, method, ok := rpc.ServiceMethod(fullMethod)
	if !ok || service == "" || method == "" {
		return "", fmt.Errorf("translation: %q is not a gRPC full method", fullMethod)
	}

	return "/" + service + "/" + method, nil
}

// newMessage allocates an empty T. The zero value of a generated message type is
// a nil pointer, which still carries its descriptor, so this works without the
// type registry the forwarder uses.
func newMessage[T proto.Message]() proto.Message {
	var zero T
	return zero.ProtoReflect().New().Interface()
}

// nameOf returns the proto full name of T, so a type mismatch names the message
// a conversion expected rather than its Go type.
func nameOf[T proto.Message]() protoreflect.FullName {
	var zero T
	return zero.ProtoReflect().Descriptor().FullName()
}
