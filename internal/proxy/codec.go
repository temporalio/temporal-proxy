package proxy

import (
	"context"
	"errors"
	"runtime"

	"go.temporal.io/api/common/v1"
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

		// Reporter records the duration and result of each vault operation. It is
		// required whenever a Vault is set.
		Reporter *Reporter
	}

	// Codecs is the chain payloads travel through in both directions. It is the
	// only place a chain is assembled, so every caller applies the same one.
	Codecs struct {
		inbound  []codecOpt
		outbound []codecOpt
	}

	// codecOpt builds the [codec.Option] for one codec, given the request the
	// payloads belong to. A codec with no per-request state ignores both
	// arguments; the encryption codec uses them to bind its cipher.
	codecOpt func(ctx context.Context, ns string) codec.Option
)

// NewCodecs returns the [Codecs] opts select. Encoding is gated per codec;
// decoding is not, so a decoder recognizes its own output and passes anything
// else through.
func NewCodecs(opts CodecOptions) (*Codecs, error) {
	if opts.Encrypt && opts.Vault == nil {
		return nil, errors.New("proxy: encryption requires a vault")
	}

	// Every vault call is timed and counted, so a vault without somewhere to
	// record is a wiring mistake. Catch it here rather than on the first payload.
	if opts.Vault != nil && opts.Reporter == nil {
		return nil, errors.New("proxy: a vault requires a reporter")
	}

	c := &Codecs{}
	if opts.Vault != nil {
		enc := func(ctx context.Context, ns string) codec.Option {
			return codec.WithCipher(&cipher{ctx: ctx, ns: ns, v: opts.Vault, r: opts.Reporter})
		}

		c.inbound = append(c.inbound, enc)
		if opts.Encrypt {
			c.outbound = append(c.outbound, enc)
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
func (c *Codecs) Interceptor() (grpc.UnaryClientInterceptor, error) {
	return proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{
		Inbound:  visitPayloads(c.inbound, codec.Chain.Decode),
		Outbound: visitPayloads(c.outbound, codec.Chain.Encode),
	})
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
