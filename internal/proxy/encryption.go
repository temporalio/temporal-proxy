package proxy

import (
	"context"
	"fmt"
	"runtime"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	"google.golang.org/grpc"

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

// Vault seals and opens payloads using envelope encryption scoped by namespace.
// It is the subset of [crypto.Vault] the interceptor depends on.
type Vault interface {
	Seal(context.Context, string, []byte) (*crypto.Message, error)
	Open(context.Context, *crypto.Message) ([]byte, error)
}

// EncryptionInterceptor returns a unary client interceptor that opens inbound
// response payloads using v and, when enabled is true, seals outbound request
// payloads as well. Sealing is gated so encryption can be turned off for new
// traffic while still opening data sealed earlier: inbound decryption always
// runs. Each payload is sealed under the DEK for the request's namespace, read
// from the outgoing gRPC metadata via [meta.NamespaceFrom], so the upstream
// never sees plaintext while local workers still exchange cleartext. On the way
// back only payloads this interceptor sealed (identified by the
// encryptionEncoding marker) are opened; anything else passes through
// untouched. Search attributes are skipped so they stay queryable upstream. It
// returns an error only if the underlying visitor interceptor cannot be
// constructed.
func EncryptionInterceptor(enabled bool, v Vault) (grpc.UnaryClientInterceptor, error) {
	var outbound *proxy.VisitPayloadsOptions
	if enabled {
		outbound = &proxy.VisitPayloadsOptions{
			ConcurrencyLimit:     runtime.NumCPU(),
			SkipSearchAttributes: true,
			Visitor:              encryptPayloads(v),
		}
	}

	return proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{
		Inbound: &proxy.VisitPayloadsOptions{
			ConcurrencyLimit:     runtime.NumCPU(),
			SkipSearchAttributes: true,
			Visitor:              decryptPayloads(v),
		},
		Outbound: outbound,
	})
}

// encryptPayloads returns a payload visitor that marshals each payload, seals
// the bytes under v for the context's namespace, and replaces it with a payload
// whose data is the ciphertext and whose metadata carries the wrapped DEK
// needed to open it. The entire original payload (metadata included) is sealed,
// so decryptPayloads can restore it exactly.
func encryptPayloads(v Vault) func(*proxy.VisitPayloadsContext, []*common.Payload) ([]*common.Payload, error) {
	return func(ctx *proxy.VisitPayloadsContext, payloads []*common.Payload) ([]*common.Payload, error) {
		ns := meta.NamespaceFrom(ctx)

		res := make([]*common.Payload, len(payloads))
		for i, p := range payloads {
			data, err := p.Marshal()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal payload: %w", err)
			}

			msg, err := v.Seal(ctx, ns, data)
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
// payloads produced elsewhere survive the round trip.
func decryptPayloads(v Vault) func(*proxy.VisitPayloadsContext, []*common.Payload) ([]*common.Payload, error) {
	return func(ctx *proxy.VisitPayloadsContext, payloads []*common.Payload) ([]*common.Payload, error) {
		res := make([]*common.Payload, len(payloads))
		for i, p := range payloads {
			// Only decrypt what we've encrypted
			if enc := string(p.Metadata[metadataEncoding]); enc != encryptionEncoding {
				res[i] = p
				continue
			}

			pt, err := v.Open(ctx, &crypto.Message{
				Ciphertext: p.Data,
				KeyMaterial: &crypto.DEKMaterial{
					KEKID:        string(p.Metadata[metadataEncryptionKeyID]),
					EncryptedDEK: string(p.Metadata[metadataEncryptionDEK]),
				},
			})
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
