package proxy

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

// These keys form the on-the-wire contract for an encrypted payload: the
// encoding marker lets decryptPayloads recognize its own output, and the key-ID
// and wrapped-DEK entries carry the material needed to open it. They live in the
// payload metadata so the ciphertext travels with everything required to
// decrypt it.
const (
	metadataEncoding        = "encoding" // Copied to avoid importing SDK just for this
	metadataEncryptionKeyID = "encryption-key-id"
	metadataEncryptionDEK   = "encryption-dek"
	encryptionEncoding      = "binary/encrypted"
)

type (
	// Vault seals and opens payloads using envelope encryption scoped by
	// namespace. It is the subset of [crypto.Vault] the interceptor depends on.
	Vault interface {
		Seal(context.Context, string, []byte) (*crypto.Message, error)
		Open(context.Context, *crypto.Message) ([]byte, error)
	}

	// encryptingClientStream seals each frame on the way out and opens each one
	// on the way back, so a streamed payload gets the treatment a unary one
	// already gets. ctx is the interceptor's own context, captured once at
	// construction: [grpc.ClientStream.Context] must not be called from here,
	// since it commits the RPC attempt and would disable gRPC's transparent
	// retries for every proxied stream.
	encryptingClientStream struct {
		grpc.ClientStream

		ctx      context.Context
		outbound *proxy.VisitPayloadsOptions
		inbound  *proxy.VisitPayloadsOptions
	}
)

// EncryptionInterceptor returns a unary client interceptor that opens inbound
// response payloads using v and, when enabled is true, seals outbound request
// payloads as well. Sealing is gated so encryption can be turned off for new
// traffic while still opening data sealed earlier: inbound decryption always
// runs. Each payload is sealed under the DEK for the request's namespace, read
// from the outgoing gRPC metadata via [meta.NamespaceFrom], so the upstream
// never sees plaintext while local workers still exchange cleartext. On the way
// back only payloads this interceptor sealed (identified by the
// encryptionEncoding marker) are opened; anything else passes through
// untouched. Search attributes are skipped so they stay queryable upstream. r
// records the duration and result of every seal/open through VaultOp. It
// returns an error only if the underlying visitor interceptor cannot be
// constructed.
func EncryptionInterceptor(enabled bool, v Vault, r *Reporter) (grpc.UnaryClientInterceptor, error) {
	var outbound *proxy.VisitPayloadsOptions
	if enabled {
		outbound = &proxy.VisitPayloadsOptions{
			ConcurrencyLimit:     runtime.NumCPU(),
			SkipSearchAttributes: true,
			Visitor:              encryptPayloads(v, r),
		}
	}

	return proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{
		Inbound: &proxy.VisitPayloadsOptions{
			ConcurrencyLimit:     runtime.NumCPU(),
			SkipSearchAttributes: true,
			Visitor:              decryptPayloads(v, r),
		},
		Outbound: outbound,
	})
}

// EncryptionStreamInterceptor returns a stream client interceptor that opens
// inbound frames using v and, when enabled is true, seals outbound frames as
// well. It is the streaming counterpart of EncryptionInterceptor and gates
// sealing the same way, so frames sealed earlier stay openable after
// encryption is turned off for new traffic.
//
// It applies the same visitor to each frame that the unary interceptor
// applies to a whole call, which means it inherits the same coverage: the
// visitor only sees message types it was generated for, and silently skips
// any other message type rather than erroring. Startup refuses a
// configuration that enables encryption while exposing a service whose
// payloads fall outside that set, but that check runs only when encryption
// is enabled; it does not bound what RecvMsg's unconditional opening may see
// when encryption is disabled.
func EncryptionStreamInterceptor(enabled bool, v Vault, r *Reporter) grpc.StreamClientInterceptor {
	inbound := &proxy.VisitPayloadsOptions{
		ConcurrencyLimit:     runtime.NumCPU(),
		SkipSearchAttributes: true,
		Visitor:              decryptPayloads(v, r),
	}

	var outbound *proxy.VisitPayloadsOptions
	if enabled {
		outbound = &proxy.VisitPayloadsOptions{
			ConcurrencyLimit:     runtime.NumCPU(),
			SkipSearchAttributes: true,
			Visitor:              encryptPayloads(v, r),
		}
	}

	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		return &encryptingClientStream{ClientStream: cs, ctx: ctx, outbound: outbound, inbound: inbound}, nil
	}
}

// SendMsg seals the frame's payloads before forwarding it, when sealing is
// enabled. It visits using s.ctx, the interceptor's own context captured at
// construction, rather than [grpc.ClientStream.Context], which must not be
// called here (see encryptingClientStream).
func (s *encryptingClientStream) SendMsg(m any) error {
	if s.outbound != nil {
		if pm, ok := m.(proto.Message); ok {
			if err := proxy.VisitPayloads(s.ctx, pm, *s.outbound); err != nil {
				return err
			}
		}
	}

	return s.ClientStream.SendMsg(m)
}

// RecvMsg opens the frame's payloads after receiving it. Opening always runs,
// so data sealed while encryption was enabled stays readable after it is
// turned off. Like SendMsg, it visits using s.ctx rather than
// [grpc.ClientStream.Context].
func (s *encryptingClientStream) RecvMsg(m any) error {
	if err := s.ClientStream.RecvMsg(m); err != nil {
		return err
	}

	if pm, ok := m.(proto.Message); ok {
		return proxy.VisitPayloads(s.ctx, pm, *s.inbound)
	}

	return nil
}

// encryptPayloads returns a payload visitor that marshals each payload, seals
// the bytes under v for the context's namespace, and replaces it with a payload
// whose data is the ciphertext and whose metadata carries the wrapped DEK
// needed to open it. The entire original payload (metadata included) is sealed,
// so decryptPayloads can restore it exactly. Each Seal call is timed end to
// end and recorded via r.VaultOp, regardless of outcome.
func encryptPayloads(v Vault, r *Reporter) func(*proxy.VisitPayloadsContext, []*common.Payload) ([]*common.Payload, error) {
	return func(ctx *proxy.VisitPayloadsContext, payloads []*common.Payload) ([]*common.Payload, error) {
		ns := meta.NamespaceFrom(ctx)

		res := make([]*common.Payload, len(payloads))
		for i, p := range payloads {
			data, err := p.Marshal()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal payload: %w", err)
			}

			start := time.Now()
			msg, err := v.Seal(ctx, ns, data)
			r.VaultOp("encrypt", resultLabel(err), ns, time.Since(start).Seconds())
			if err != nil {
				return nil, fmt.Errorf("failed to encrypt payload: %w", err)
			}

			res[i] = &common.Payload{
				Metadata: map[string][]byte{
					metadataEncoding:        []byte(encryptionEncoding),
					metadataEncryptionKeyID: []byte(msg.KeyMaterial.KEKID),
					metadataEncryptionDEK:   []byte(msg.KeyMaterial.EncryptedDEK),
				},
				Data: msg.Ciphertext,
			}
		}

		return res, nil
	}
}

// decryptPayloads returns a payload visitor that reverses encryptPayloads:
// payloads carrying the encryptionEncoding marker are opened and unmarshaled
// back into their original form, while any others pass through unchanged so
// payloads produced elsewhere survive the round trip. Only payloads actually
// opened are timed end to end and recorded via r.VaultOp; pass-through
// payloads are not.
func decryptPayloads(v Vault, r *Reporter) func(*proxy.VisitPayloadsContext, []*common.Payload) ([]*common.Payload, error) {
	return func(ctx *proxy.VisitPayloadsContext, payloads []*common.Payload) ([]*common.Payload, error) {
		ns := meta.NamespaceFrom(ctx)

		res := make([]*common.Payload, len(payloads))
		for i, p := range payloads {
			// Only decrypt what we've encrypted
			if enc := string(p.Metadata[metadataEncoding]); enc != encryptionEncoding {
				res[i] = p
				continue
			}

			start := time.Now()
			pt, err := v.Open(ctx, &crypto.Message{
				Ciphertext: p.Data,
				KeyMaterial: &crypto.DEKMaterial{
					KEKID:        string(p.Metadata[metadataEncryptionKeyID]),
					EncryptedDEK: string(p.Metadata[metadataEncryptionDEK]),
				},
			})
			r.VaultOp("decrypt", resultLabel(err), ns, time.Since(start).Seconds())
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt payload: %w", err)
			}

			og := new(common.Payload)
			if err := og.Unmarshal(pt); err != nil {
				return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
			}

			res[i] = og
		}

		return res, nil
	}
}

// resultLabel maps an error to the "result" metric label value.
func resultLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}
