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
	// CodecOptions selects the codecs a [CodecInterceptor] applies.
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

	// codecOpt builds the [codec.Option] for one codec, given the request the
	// payloads belong to. A codec with no per-request state ignores both
	// arguments; the encryption codec uses them to bind its cipher.
	codecOpt func(ctx context.Context, ns string) codec.Option
)

// CodecInterceptor returns a unary client interceptor that runs payloads through
// the codecs opts select: outbound requests are encoded and inbound responses are
// decoded, each through a [codec.Chain] built for that request. A direction with
// no codecs is skipped entirely rather than walked for nothing.
//
// Search attributes are never encoded, so they stay queryable upstream. The
// namespace a codec is given is the one the request carries, read via
// [meta.NamespaceFrom].
func CodecInterceptor(opts CodecOptions) (grpc.UnaryClientInterceptor, error) {
	if opts.Encrypt && opts.Vault == nil {
		return nil, errors.New("proxy: encryption requires a vault")
	}

	// Every vault call is timed and counted, so a vault without somewhere to
	// record is a wiring mistake. Catch it here rather than on the first payload.
	if opts.Vault != nil && opts.Reporter == nil {
		return nil, errors.New("proxy: a vault requires a reporter")
	}

	// Every codec is listed once, with the gate that decides whether it encodes.
	// Decoding is ungated: a decoder recognizes its own output and passes anything
	// else through, so including it costs nothing and keeps older data readable.
	var outbound, inbound []codecOpt
	if opts.Vault != nil {
		enc := func(ctx context.Context, ns string) codec.Option {
			return codec.WithCipher(&cipher{ctx: ctx, ns: ns, v: opts.Vault, r: opts.Reporter})
		}

		inbound = append(inbound, enc)
		if opts.Encrypt {
			outbound = append(outbound, enc)
		}
	}

	return proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{
		Inbound:  visitPayloads(inbound, codec.Chain.Decode),
		Outbound: visitPayloads(outbound, codec.Chain.Encode),
	})
}

// visitPayloads returns the options that build a chain from opts per request and
// apply it with fn ([codec.Chain.Encode] or [codec.Chain.Decode]), or nil when
// opts is empty so that direction is left alone.
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
			ns := meta.NamespaceFrom(ctx)

			chain := make([]codec.Option, len(opts))
			for i, opt := range opts {
				chain[i] = opt(ctx, ns)
			}

			return fn(codec.NewChain(chain...), payloads)
		},
	}
}
