package auth

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// CredentialProvider supplies per-RPC metadata for outbound calls to an
// upstream. Header reports the metadata header it sets, so the proxy can strip
// any forwarded value on that header before the credential adds its own.
type CredentialProvider interface {
	credentials.PerRPCCredentials
	Header() string
}

// CredentialProviderFor maps configuration to the selected CredentialProvider.
// It returns (nil, nil) only when cfg is nil (no credential configured); a
// present-but-empty block fails closed with an error rather than silently
// dialing the upstream without the configured credential.
func CredentialProviderFor(cfg *config.CredentialConfig) (CredentialProvider, error) {
	if cfg == nil {
		return nil, nil
	}

	if cfg.Static != nil {
		cp, err := NewStaticCredentialProvider(cfg.Static.APIKey, cfg.Static.Header, cfg.Static.Scheme)
		if err != nil {
			return nil, err
		}
		return cp, nil
	}

	return nil, errors.New("auth: a credentials block was configured but no provider (static) was selected")
}

// DialOptions returns the dial options that install cp on an outbound
// connection: the per-RPC credential itself, plus client interceptors that
// strip cp's header from each call's forwarded metadata so a forwarded value on
// the same header cannot collide with the credential.
func DialOptions(cp CredentialProvider) []grpc.DialOption {
	opts := []grpc.DialOption{grpc.WithPerRPCCredentials(cp)}
	if header := cp.Header(); header != "" {
		opts = append(
			opts,
			grpc.WithChainUnaryInterceptor(stripOutgoingUnary(header)),
			grpc.WithChainStreamInterceptor(stripOutgoingStream(header)),
		)
	}

	return opts
}

// stripOutgoingHeader returns ctx with header removed from its outgoing
// metadata (a copy; the caller's metadata is not mutated).
func stripOutgoingHeader(ctx context.Context, header string) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return ctx
	}

	md = md.Copy()
	md.Delete(header)
	return metadata.NewOutgoingContext(ctx, md)
}

func stripOutgoingUnary(header string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(stripOutgoingHeader(ctx, header), method, req, reply, cc, opts...)
	}
}

func stripOutgoingStream(header string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(stripOutgoingHeader(ctx, header), desc, cc, method, opts...)
	}
}
