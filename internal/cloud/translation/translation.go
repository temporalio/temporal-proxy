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
	// conversions are written against concrete message types, or with [Answer]
	// for a method the proxy replies to itself. A Translation holds no per-call
	// state and is safe for concurrent use once passed to [NewRegistry], which,
	// like [Translation.WithHeader], modifies it.
	Translation struct {
		from, to string
		call     func(ctx context.Context, req, reply proto.Message, send sendFunc) error
		headers  map[string]string
	}

	// Registry is the set of translations an interceptor consults, keyed by the
	// inbound full method. It is fixed once built and safe for concurrent use; a
	// nil Registry translates nothing, so a caller with none to install can pass
	// one straight through.
	Registry struct {
		byMethod map[string]*Translation
	}

	// sendFunc invokes the upstream method a [Translation] stands in for, filling
	// reply from req.
	sendFunc func(ctx context.Context, req, reply proto.Message) error
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
		call: func(ctx context.Context, m, out proto.Message, send sendFunc) error {
			req, ok := m.(Req)
			if !ok {
				err := fmt.Errorf("translation: %s wanted a %s request, got %T", from, nameOf[Req](), m)
				return rpc.StatusError("translation: adapting the request failed", err)
			}

			upReq, err := request(req)
			if err != nil {
				return rpc.StatusError("translation: adapting the request failed", err)
			}

			upstream := newMessage[UpResp]()
			if err := send(ctx, upReq, upstream); err != nil {
				return err
			}

			reply, ok := out.(Resp)
			if !ok {
				err := fmt.Errorf("translation: %s wanted a %s reply, got %T", from, nameOf[Resp](), out)
				return rpc.StatusError("translation: adapting the reply failed", err)
			}

			if err := response(req, upstream, reply); err != nil {
				return rpc.StatusError("translation: adapting the reply failed", err)
			}

			return nil
		},
	}
}

// Answer builds a [Translation] that replies to from without calling any
// upstream: answer fills the caller's reply from its request alone. It is for a
// method the upstream cannot serve and nothing else stands in for, where a reply
// the proxy builds is closer to right than the upstream's refusal.
func Answer[Req, Resp proto.Message](from string, answer func(Req, Resp) error) *Translation {
	return &Translation{
		from: from,
		call: func(_ context.Context, m, out proto.Message, _ sendFunc) error {
			req, ok := m.(Req)
			if !ok {
				err := fmt.Errorf("translation: %s wanted a %s request, got %T", from, nameOf[Req](), m)
				return rpc.StatusError("translation: answering the request failed", err)
			}

			reply, ok := out.(Resp)
			if !ok {
				err := fmt.Errorf("translation: %s wanted a %s reply, got %T", from, nameOf[Resp](), out)
				return rpc.StatusError("translation: answering the request failed", err)
			}

			if err := answer(req, reply); err != nil {
				return rpc.StatusError("translation: answering the request failed", err)
			}

			return nil
		},
	}
}

// WithHeader stamps key: value on the substituted call and returns t. It
// replaces any value the caller sent rather than adding to it. Headers travel
// only on a call this translation substituted: a method the registry does not
// translate is untouched, and a caller invoking the upstream API directly is
// forwarded untranslated and keeps its header.
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
// surfaces at construction rather than on the first request that hits it. An
// [Answer] has no upstream method, so only its inbound one is checked.
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

		if t.to != "" {
			to, err := canonical(t.to)
			if err != nil {
				return nil, err
			}

			if from == to {
				return nil, fmt.Errorf("translation: %s translates onto itself", from)
			}

			t.to = to
		}

		if _, dup := byMethod[from]; dup {
			return nil, fmt.Errorf("translation: %s is translated twice", from)
		}

		t.from = from
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

// To is the upstream method that stands in for it, or empty for an [Answer],
// which calls none.
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
func newMessage[T proto.Message]() T {
	var zero T
	return zero.ProtoReflect().New().Interface().(T)
}

// nameOf returns the proto full name of T, so a type mismatch names the message
// a conversion expected rather than its Go type.
func nameOf[T proto.Message]() protoreflect.FullName {
	var zero T
	return zero.ProtoReflect().Descriptor().FullName()
}
