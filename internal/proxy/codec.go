package proxy

import (
	"context"
	"errors"
	"runtime"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/failure/v1"
	"go.temporal.io/api/proxy"
	"google.golang.org/grpc"

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

	// codecOpt builds the [codec.Option] for one codec, given the request the
	// payloads belong to. A codec with no per-request state ignores both
	// arguments; the encryption codec uses them to bind its cipher.
	codecOpt func(ctx context.Context, ns string) codec.Option
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
		enc := func(ctx context.Context, ns string) codec.Option {
			return codec.WithCipher(&cipher{ctx: ctx, ns: ns, v: opts.Vault, r: opts.Reporter})
		}

		c.inbound = append(c.inbound, enc)
		if opts.Encrypt {
			c.outbound = append(c.outbound, enc)
		}

		// The set is built once here; only the observer is bound per request.
		if len(opts.SkipEncodings) > 0 {
			encodings := codec.WithSkipEncodings(opts.SkipEncodings...)
			skip := func(ctx context.Context, ns string) codec.Option {
				return codec.WithEncryptorOptions(encodings, codec.WithSkipObserver(func(op, encoding string) {
					opts.Reporter.PayloadSkipped(ctx, op, encoding, ns)
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
func CodecInterceptor(opts CodecOptions) (grpc.UnaryClientInterceptor, error) {
	c, err := NewCodecs(opts)
	if err != nil {
		return nil, err
	}

	return c.Interceptor()
}

// Interceptor returns a unary client interceptor that encodes outbound requests
// and decodes inbound responses. A direction with no codecs is skipped entirely
// rather than walked for nothing.
//
// Search attributes are never encoded, so they stay queryable upstream. The
// namespace a codec is given is the one the request carries, read via
// [meta.NamespaceFrom].
//
// Failures are handled outside the payload codecs. With failure encoding on, they
// are encoded first, so the payload holding their attributes is sealed like any
// other. With a vault, their text is restored last, from attributes already
// opened, whether or not encoding is on, so failures encoded earlier stay
// readable after it is turned off.
func (c *Codecs) Interceptor() (grpc.UnaryClientInterceptor, error) {
	payloads, err := proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{
		Inbound:  visitPayloads(c.inbound, codec.Chain.Decode),
		Outbound: visitPayloads(c.outbound, codec.Chain.Encode),
	})
	if err != nil || (!c.encodeFailures && !c.restoreFailures) {
		return payloads, err
	}

	failures, err := proxy.NewFailureVisitorInterceptor(proxy.FailureVisitorInterceptorOptions{
		Inbound:  visitFailures(c.restoreFailures, restoreFailure),
		Outbound: visitFailures(c.encodeFailures, encodeFailure),
	})
	if err != nil {
		return nil, err
	}

	return chainUnary(failures, payloads), nil
}

// Decode runs payloads through the inbound chain for ns, the same chain
// [Codecs.Interceptor] applies to a response.
func (c *Codecs) Decode(
	ctx context.Context,
	ns string,
	payloads []*common.Payload,
) ([]*common.Payload, error) {
	return apply(ctx, ns, c.inbound, codec.Chain.Decode, payloads)
}

// Encode runs payloads through the outbound chain for ns, the same chain
// [Codecs.Interceptor] applies to a request.
func (c *Codecs) Encode(
	ctx context.Context,
	ns string,
	payloads []*common.Payload,
) ([]*common.Payload, error) {
	return apply(ctx, ns, c.outbound, codec.Chain.Encode, payloads)
}

// apply builds the chain opts imply for ctx and ns and runs payloads through it
// with fn. Every caller reaches the chain through here, so no two of them can
// apply different chains.
func apply(
	ctx context.Context,
	ns string,
	opts []codecOpt,
	fn func(codec.Chain, []*common.Payload) ([]*common.Payload, error),
	payloads []*common.Payload,
) ([]*common.Payload, error) {
	chain := make([]codec.Option, len(opts))
	for i, opt := range opts {
		chain[i] = opt(ctx, ns)
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
// when opts is empty so that direction is left alone.
func visitPayloads(
	opts []codecOpt,
	fn func(codec.Chain, []*common.Payload) ([]*common.Payload, error),
) *proxy.VisitPayloadsOptions {
	if len(opts) == 0 {
		return nil
	}

	return &proxy.VisitPayloadsOptions{
		ConcurrencyLimit:     runtime.NumCPU(),
		SkipSearchAttributes: true,
		Visitor: func(ctx *proxy.VisitPayloadsContext, payloads []*common.Payload) ([]*common.Payload, error) {
			return apply(ctx, meta.NamespaceFrom(ctx), opts, fn, payloads)
		},
	}
}

// visitFailures returns the options that apply visitor to every failure, or nil
// when on is false so that direction is left alone.
func visitFailures(
	on bool,
	visitor func(*proxy.VisitFailuresContext, *failure.Failure) error,
) *proxy.VisitFailuresOptions {
	if !on {
		return nil
	}

	return &proxy.VisitFailuresOptions{Visitor: visitor}
}
