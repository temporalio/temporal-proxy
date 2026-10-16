package proxy

import (
	"context"
	"errors"
	"runtime"
	"slices"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/failure/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/proxy"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/codec"
)

type (
	// CodecOptions selects the codecs a [Codecs] applies.
	CodecOptions struct {
		// Vault seals and opens payloads. With no Vault there is no encryption
		// codec at all. Leave it nil rather than passing a nil concrete vault,
		// which would read as present here and panic on first use.
		Vault Vault

		// Encrypt seals outbound payloads. Inbound payloads are opened whenever a
		// Vault is present regardless, so payloads sealed earlier stay readable
		// after sealing is turned off for new traffic.
		Encrypt bool

		// EncodeFailures moves the message and stack trace of outbound failures into
		// a payload, as the Temporal SDK does with EncodeCommonAttributes, so they
		// are sealed with every other payload. It requires Encrypt.
		EncodeFailures bool

		// SkipEncodings lists payload encodings already encrypted before they
		// reach the proxy. Outbound payloads under one are forwarded unsealed, and
		// listing codec.EncryptionEncoding also returns inbound payloads sealed
		// under a KEK the vault doesn't hold, rather than failing the call.
		SkipEncodings []string

		// Reporter records the duration and result of each vault operation. It is
		// required whenever a Vault is set.
		Reporter *Reporter
	}

	// Codecs is the chain payloads travel through in both directions. It is the
	// only place a chain is assembled, so every caller applies the same one.
	Codecs struct {
		inbound         []codecOpt
		outbound        []codecOpt
		encodeFailures  bool
		restoreFailures bool
	}

	// InterceptorOption configures one [Codecs.Interceptor].
	InterceptorOption func(*interceptorOptions)

	interceptorOptions struct {
		remote func(string) string
	}

	// scope is what a codec knows about the request its payloads belong to: the
	// local namespace that selects the key policy, the namespace sealed payloads
	// are bound to, and whether opened payloads must be bound to that namespace.
	scope struct {
		ns    string
		label string
		check bool
	}

	// codecOpt builds the [codec.Option] for one codec, given the request the
	// payloads belong to. A codec with no per-request state ignores both
	// arguments; the encryption codec uses them to bind its cipher.
	codecOpt func(ctx context.Context, s scope) codec.Option

	// crossKey marks a context with the [crossing] of the Nexus message it sits
	// under.
	crossKey struct{}

	// crossing is which payloads beneath a Nexus message cross namespaces: all of
	// them, under a field that crosses, or else only those under parts, the
	// fields of the nearest Nexus message that do.
	crossing struct {
		all   bool
		parts []proto.Message
	}
)

// NewCodecs returns the [Codecs] opts select. Encoding is gated per codec;
// decoding is not, so a decoder recognizes its own output and passes anything
// else through. It errors when Encrypt is set without a Vault, or a Vault is
// set without a Reporter.
func NewCodecs(opts CodecOptions) (*Codecs, error) {
	if opts.Encrypt && opts.Vault == nil {
		return nil, errors.New("proxy: encryption requires a vault")
	}

	// Without sealing, moving the text into a payload hides nothing; it would only
	// cost anyone reading the failure without an SDK or codec server its message.
	if opts.EncodeFailures && !opts.Encrypt {
		return nil, errors.New("proxy: failure encoding requires encryption")
	}

	// Every vault call is timed and counted, so a vault without somewhere to
	// record is a wiring mistake. Catch it here rather than on the first payload.
	if opts.Vault != nil && opts.Reporter == nil {
		return nil, errors.New("proxy: a vault requires a reporter")
	}

	c := &Codecs{encodeFailures: opts.EncodeFailures, restoreFailures: opts.Vault != nil}
	if opts.Vault != nil {
		enc := func(ctx context.Context, s scope) codec.Option {
			return codec.WithCipher(&cipher{ctx: ctx, s: s, v: opts.Vault, r: opts.Reporter})
		}

		c.inbound = append(c.inbound, enc)
		if opts.Encrypt {
			c.outbound = append(c.outbound, enc)
		}

		// The set is built once here; only the observer is bound per request.
		if len(opts.SkipEncodings) > 0 {
			encodings := codec.WithSkipEncodings(opts.SkipEncodings...)
			skip := func(ctx context.Context, s scope) codec.Option {
				return codec.WithEncryptorOptions(encodings, codec.WithSkipObserver(func(op, encoding string) {
					opts.Reporter.PayloadSkipped(ctx, op, encoding, s.ns)
				}))
			}

			c.inbound = append(c.inbound, skip)
			if opts.Encrypt {
				c.outbound = append(c.outbound, skip)
			}
		}
	}

	return c, nil
}

// CodecInterceptor returns the unary client interceptor for the codecs opts
// select. It is [NewCodecs] followed by [Codecs.Interceptor].
func CodecInterceptor(opts CodecOptions, iopts ...InterceptorOption) (grpc.UnaryClientInterceptor, error) {
	c, err := NewCodecs(opts)
	if err != nil {
		return nil, err
	}

	return c.Interceptor(iopts...)
}

// WithRemoteNamespace sets how the interceptor maps a request's local namespace
// to the name payloads are bound to, usually the one its upstream knows. Sealed
// payloads are bound to that name and opened payloads must be bound to it. It
// only sets the label; the request's namespace is translated elsewhere. Without
// it the two names are the same.
func WithRemoteNamespace(remote func(string) string) InterceptorOption {
	return func(o *interceptorOptions) { o.remote = remote }
}

// Interceptor returns a unary client interceptor that encodes outbound requests
// and decodes inbound responses. A direction with no codecs is skipped entirely
// rather than walked for nothing.
//
// Search attributes are never encoded, so they stay queryable upstream. The
// namespace a codec is given is the one the request carries, read via
// [meta.NamespaceFrom]; payloads are bound to its remote name (see
// [WithRemoteNamespace]), and opened payloads must be bound to it unless they
// sit under a Nexus field that crosses namespaces by design (see [crossingOf]).
//
// Failures are handled outside the payload codecs. With failure encoding on, they
// are encoded first, so the payload holding their attributes is sealed like any
// other. With a vault, their text is restored last, from attributes already
// opened, whether or not encoding is on, so failures encoded earlier stay
// readable after it is turned off.
func (c *Codecs) Interceptor(opts ...InterceptorOption) (grpc.UnaryClientInterceptor, error) {
	var io interceptorOptions
	for _, opt := range opts {
		opt(&io)
	}

	payloads, err := proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{
		Inbound:  visitPayloads(c.inbound, codec.Chain.Decode, io.remote),
		Outbound: visitPayloads(c.outbound, codec.Chain.Encode, io.remote),
	})
	if err != nil || (!c.encodeFailures && !c.restoreFailures) {
		return payloads, err
	}

	failures, err := proxy.NewFailureVisitorInterceptor(proxy.FailureVisitorInterceptorOptions{
		Inbound:  visitFailures(c.restoreFailures, codec.RestoreFailure),
		Outbound: visitFailures(c.encodeFailures, codec.EncodeFailure),
	})
	if err != nil {
		return nil, err
	}

	return chainUnary(failures, payloads), nil
}

// Decode runs payloads through the inbound chain, the same chain
// [Codecs.Interceptor] applies to a response. local selects the key policy.
// When remote is set, opened payloads are compared with it under the vault's
// binding mode: strict refuses a mismatch, while permissive and off open it and
// count it. When it is empty nothing is compared.
func (c *Codecs) Decode(
	ctx context.Context,
	local, remote string,
	payloads []*common.Payload,
) ([]*common.Payload, error) {
	return apply(ctx, scope{ns: local, label: remote, check: remote != ""}, c.inbound, codec.Chain.Decode, payloads)
}

// Encode runs payloads through the outbound chain, the same chain
// [Codecs.Interceptor] applies to a request. local selects the key policy and
// sealed payloads are bound to remote.
func (c *Codecs) Encode(
	ctx context.Context,
	local, remote string,
	payloads []*common.Payload,
) ([]*common.Payload, error) {
	return apply(ctx, scope{ns: local, label: remote}, c.outbound, codec.Chain.Encode, payloads)
}

// apply builds the chain opts imply for ctx and s and runs payloads through it
// with fn. Every caller reaches the chain through here, so no two of them can
// apply different chains.
func apply(
	ctx context.Context,
	s scope,
	opts []codecOpt,
	fn func(codec.Chain, []*common.Payload) ([]*common.Payload, error),
	payloads []*common.Payload,
) ([]*common.Payload, error) {
	chain := make([]codec.Option, len(opts))
	for i, opt := range opts {
		chain[i] = opt(ctx, s)
	}

	return fn(codec.NewChain(chain...), payloads)
}

// chainUnary returns an interceptor that runs outer around inner, so outer sees
// the request first and the response last.
func chainUnary(outer, inner grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		next := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			return inner(ctx, method, req, reply, cc, invoker, opts...)
		}

		return outer(ctx, method, req, reply, cc, next, opts...)
	}
}

// visitPayloads returns the options that apply opts with fn per request, or nil
// when opts is empty so that direction is left alone. remote maps the request's
// local namespace to its upstream's, and may be nil.
func visitPayloads(
	opts []codecOpt,
	fn func(codec.Chain, []*common.Payload) ([]*common.Payload, error),
	remote func(string) string,
) *proxy.VisitPayloadsOptions {
	if len(opts) == 0 {
		return nil
	}

	return &proxy.VisitPayloadsOptions{
		ConcurrencyLimit:     runtime.NumCPU(),
		SkipSearchAttributes: true,
		ContextHook:          markCrossing,
		Visitor: func(ctx *proxy.VisitPayloadsContext, payloads []*common.Payload) ([]*common.Payload, error) {
			ns := meta.NamespaceFrom(ctx)
			s := scope{ns: ns, label: remoteName(remote, ns), check: !crosses(ctx, payloads)}

			return apply(ctx, s, opts, fn, payloads)
		},
	}
}

// remoteName maps ns through remote. An absent namespace stays absent rather
// than becoming whatever prefix or suffix a translation rule adds.
func remoteName(remote func(string) string, ns string) string {
	if remote == nil || ns == "" {
		return ns
	}

	return remote(ns)
}

// markCrossing records in ctx which payloads beneath m cross namespaces. A
// field [crossingOf] names marks everything beneath it; a Nexus message records
// its own crossing fields for its children to match.
func markCrossing(ctx context.Context, m proto.Message) (context.Context, error) {
	c, _ := ctx.Value(crossKey{}).(crossing)
	if c.all {
		return ctx, nil
	}

	if slices.Contains(c.parts, m) {
		return context.WithValue(ctx, crossKey{}, crossing{all: true}), nil
	}

	if parts := crossingOf(m); parts != nil {
		return context.WithValue(ctx, crossKey{}, crossing{parts: parts}), nil
	}

	return ctx, nil
}

// crosses reports whether payloads cross namespaces: they sit under a crossing
// field, or are one themselves.
func crosses(ctx context.Context, payloads []*common.Payload) bool {
	c, _ := ctx.Value(crossKey{}).(crossing)

	return c.all || (len(payloads) == 1 && slices.Contains(c.parts, proto.Message(payloads[0])))
}

// crossingOf returns the fields of m that carry payloads sealed in another
// namespace: a Nexus operation's input, which the handler reads, and its
// outcome, which the caller reads. Every other field is checked, including the
// caller's own copy of the input, so a field added upstream is compared until
// it is listed here.
func crossingOf(m proto.Message) []proto.Message {
	switch m := m.(type) {
	case *nexus.StartOperationRequest:
		return []proto.Message{m.GetPayload()}
	case *history.NexusOperationCompletedEventAttributes:
		return []proto.Message{m.GetResult()}
	case *history.NexusOperationFailedEventAttributes:
		return []proto.Message{m.GetFailure()}
	case *history.NexusOperationCanceledEventAttributes:
		return []proto.Message{m.GetFailure()}
	case *history.NexusOperationCancelRequestFailedEventAttributes:
		return []proto.Message{m.GetFailure()}
	case *history.NexusOperationTimedOutEventAttributes:
		return []proto.Message{m.GetFailure()}
	case *workflow.PendingNexusOperationInfo:
		return []proto.Message{m.GetCancellationInfo(), m.GetLastAttemptFailure()}
	case *workflow.NexusOperationCancellationInfo:
		return []proto.Message{m.GetLastAttemptFailure()}
	case *nexus.NexusOperationExecutionInfo:
		return []proto.Message{m.GetCancellationInfo(), m.GetLastAttemptFailure()}
	case *nexus.NexusOperationExecutionCancellationInfo:
		return []proto.Message{m.GetLastAttemptFailure()}
	case *workflowservice.DescribeNexusOperationExecutionResponse:
		return []proto.Message{m.GetResult(), m.GetFailure()}
	case *workflowservice.PollNexusOperationExecutionResponse:
		return []proto.Message{m.GetResult(), m.GetFailure()}
	default:
		return nil
	}
}

// visitFailures returns the options that apply fn to every failure, or nil
// when on is false so that direction is left alone.
func visitFailures(on bool, fn func(*failure.Failure) error) *proxy.VisitFailuresOptions {
	if !on {
		return nil
	}

	return &proxy.VisitFailuresOptions{
		Visitor: func(_ *proxy.VisitFailuresContext, f *failure.Failure) error { return fn(f) },
	}
}
