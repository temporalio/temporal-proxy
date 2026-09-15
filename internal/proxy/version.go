package proxy

import (
	"context"

	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

// VersionDialOptions returns the dial options that stamp the proxy's own build
// version on every outbound request as meta.VersionHeader. Callers fold them
// into the dial options for the upstream connection, and only for an upstream
// that is Temporal Cloud: Cloud reads the header to tell which proxy build a
// request came from, and no other upstream has asked for it.
//
// An empty version installs nothing, so a build with no version to report sends
// no header rather than an empty one.
func VersionDialOptions(version string) []grpc.DialOption {
	if version == "" {
		return nil
	}

	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(versionUnaryInterceptor(version)),
		grpc.WithChainStreamInterceptor(versionStreamInterceptor(version)),
	}
}

// versionUnaryInterceptor stamps the header on a unary call.
func versionUnaryInterceptor(version string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		return invoker(meta.WithVersion(ctx, version), method, req, reply, cc, opts...)
	}
}

// versionStreamInterceptor stamps the header once per stream open rather than
// once per message, since metadata travels with the headers a stream opens with
// and cannot be changed afterwards.
func versionStreamInterceptor(version string) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		return streamer(meta.WithVersion(ctx, version), desc, cc, method, opts...)
	}
}
