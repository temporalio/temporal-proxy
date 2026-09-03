package proxy

import (
	"context"

	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/cloud"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// CloudNamespaceDialOptions returns the dial options that report a request whose
// translated namespace is not shaped like a Temporal Cloud namespace. out maps a
// local namespace to its remote name, and log may be nil, which reports nothing.
// Callers fold them into the dial options for the upstream connection, and only
// for an upstream that is Temporal Cloud.
//
// This is diagnostic: nothing is rejected and the request travels unchanged.
func CloudNamespaceDialOptions(out func(string) string, log logger.Logger) []grpc.DialOption {
	if log != nil {
		log = log.With(tag.Component("translation"))
	}

	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(cloudNamespaceUnaryInterceptor(out, log)),
		grpc.WithChainStreamInterceptor(cloudNamespaceStreamInterceptor(out, log)),
	}
}

// cloudNamespaceUnaryInterceptor checks the translated namespace once per call.
func cloudNamespaceUnaryInterceptor(out func(string) string, log logger.Logger) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		logNonCloudNamespace(ctx, log, method, out)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// cloudNamespaceStreamInterceptor checks the translated namespace once per stream
// open rather than once per message, since every message on a stream carries the
// same namespace.
func cloudNamespaceStreamInterceptor(out func(string) string, log logger.Logger) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		logNonCloudNamespace(ctx, log, method, out)

		return streamer(ctx, desc, cc, method, opts...)
	}
}

// logNonCloudNamespace emits a debug entry when the request's local namespace,
// translated through out, is not shaped like a Temporal Cloud namespace. A Cloud
// upstream derives its endpoint and authorizes requests by that name, so a
// malformed one otherwise fails as an opaque DNS or NotFound error.
//
// It is a no-op when log is nil, and when the request carries no local
// namespace: the router stamps the namespace header on every request, so
// translating an absent one would report a namespace no client asked for.
func logNonCloudNamespace(ctx context.Context, log logger.Logger, method string, out func(string) string) {
	if log == nil {
		return
	}

	localNS := meta.NamespaceFrom(ctx)
	if localNS == "" {
		return
	}

	remoteNS := out(localNS)

	err := cloud.ValidateNamespace(remoteNS)
	if err == nil {
		return
	}

	log.Debug(
		"outbound namespace is not valid",
		tag.String("method", method),
		tag.String("localNamespace", localNS),
		tag.String("remoteNamespace", remoteNS),
		tag.Error(err),
	)
}
