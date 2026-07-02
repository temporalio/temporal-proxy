package interceptor

import (
	"context"
	"fmt"
	"runtime"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	"google.golang.org/grpc"
)

type (
	// PayloadCodec transforms a single Temporal payload. Implementations
	// typically rewrite the payload's bytes (for example compressing or
	// encrypting them) in Encode and reverse that transformation in Decode.
	// Encode and Decode must be inverses: Decode(Encode(p)) should yield the
	// original payload.
	//
	// The interceptor visits payloads concurrently, so implementations must be
	// safe for use by multiple goroutines.
	PayloadCodec interface {
		Encode(context.Context, *common.Payload) (*common.Payload, error)
		Decode(context.Context, *common.Payload) (*common.Payload, error)
	}

	// payloadCodecChain applies an ordered series of codecs to payloads.
	payloadCodecChain []PayloadCodec
)

// Payloads returns a gRPC unary client interceptor that runs the given codecs
// over every payload in outbound requests and inbound responses.
//
// Outbound payloads are encoded by applying the codecs in the order given.
// Inbound payloads are decoded by applying the codecs in reverse order, so the
// same chain used on the way out undoes its own transformations on the way back.
//
// Search attributes are intentionally skipped so they remain readable/indexable.
//
// These codecs transform the Temporal payloads carried inside WorkflowService
// messages, not the gRPC frames themselves. Unlike gRPC transport compression
// (grpc.UseCompressor), which a peer decodes on arrival, these transformations
// persist: the upstream server stores the encoded bytes. That is what enables
// at-rest compression and encryption, and lets an ordered chain express
// transforms (e.g. compress-then-encrypt) that fixed transport options cannot.
func Payloads(codecs ...PayloadCodec) (grpc.UnaryClientInterceptor, error) {
	if len(codecs) == 0 {
		return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		}, nil
	}

	cores := runtime.NumCPU()
	chain := payloadCodecChain(codecs)

	pv, err := proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{
		Outbound: &proxy.VisitPayloadsOptions{
			ConcurrencyLimit:     cores,
			SkipSearchAttributes: true,
			Visitor: func(ctx *proxy.VisitPayloadsContext, p []*common.Payload) ([]*common.Payload, error) {
				return chain.encode(ctx.Context, p)
			},
		},
		Inbound: &proxy.VisitPayloadsOptions{
			ConcurrencyLimit:     cores,
			SkipSearchAttributes: true,
			Visitor: func(ctx *proxy.VisitPayloadsContext, p []*common.Payload) ([]*common.Payload, error) {
				return chain.decode(ctx.Context, p)
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to construct PayloadVisitorInterceptor: %w", err)
	}

	return pv, nil
}

// encode applies each codec's Encode to every payload in forward order.
func (codecs payloadCodecChain) encode(ctx context.Context, p []*common.Payload) ([]*common.Payload, error) {
	var err error

	res := make([]*common.Payload, len(p))
	for i := range p {
		res[i] = p[i]

		for j := range codecs {
			res[i], err = codecs[j].Encode(ctx, res[i])
			if err != nil {
				return nil, fmt.Errorf("failed to encode payload: %w", err)
			}
		}
	}

	return res, nil
}

// decode applies each codec's Decode to every payload in reverse order,
// undoing the transformations applied by encode.
func (codecs payloadCodecChain) decode(ctx context.Context, p []*common.Payload) ([]*common.Payload, error) {
	var err error

	res := make([]*common.Payload, len(p))
	for i := range p {
		res[i] = p[i]

		for j := len(codecs) - 1; j >= 0; j-- {
			res[i], err = codecs[j].Decode(ctx, res[i])
			if err != nil {
				return nil, fmt.Errorf("failed to decode payload: %w", err)
			}
		}
	}

	return res, nil
}
