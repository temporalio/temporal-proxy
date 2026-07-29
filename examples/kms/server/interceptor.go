package main

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// authHeader and bearerPrefix are what the proxy's static credential sends:
	// its header and scheme default to "authorization" and "Bearer".
	authHeader   = "authorization"
	bearerPrefix = "Bearer "
)

// authInterceptor requires every call to present "Bearer <token>" in the
// authorization header. The proxy attaches this from its credentials block, and
// gRPC refuses to send such a credential over a plaintext connection, so this
// pairs with the server's TLS configuration rather than replacing it.
func authInterceptor(token string) grpc.UnaryServerInterceptor {
	want := []byte(bearerPrefix + token)

	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		got := md.Get(authHeader)
		if len(got) != 1 {
			return nil, status.Error(codes.Unauthenticated, "missing credentials")
		}

		// Constant time so a rejected call leaks nothing about the real token.
		if subtle.ConstantTimeCompare([]byte(got[0]), want) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid credentials")
		}

		return handler(ctx, req)
	}
}
