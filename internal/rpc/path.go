package rpc

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FullMethod returns the full method name of the call ctx belongs to, in the
// "/pkg.Service/Method" form the other helpers here parse. It fails when ctx
// carries no server transport stream, which means it is not a server call's
// context and there is no method to name.
func FullMethod(ctx context.Context) (string, error) {
	sts := grpc.ServerTransportStreamFromContext(ctx)
	if sts == nil {
		return "", status.Error(codes.Internal, "no server transport stream in context")
	}

	return sts.Method(), nil
}

// ServiceMethod splits a gRPC full method ("/pkg.Service/Method", with or
// without the leading slash gRPC supplies) into its service and method halves.
// It reports false when fullMethod carries no method to split off, in which case
// both halves are empty.
func ServiceMethod(fullMethod string) (service, method string, ok bool) {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	slash := strings.LastIndex(trimmed, "/")
	if slash < 0 {
		return "", "", false
	}

	return trimmed[:slash], trimmed[slash+1:], true
}

// Service returns the service half of a gRPC full method, or "" when fullMethod
// carries no method to strip. Callers must treat "" as "no service" rather than
// as a name to match on.
func Service(fullMethod string) string {
	service, _, _ := ServiceMethod(fullMethod)
	return service
}

// Method returns the method half of a gRPC full method, or "" when fullMethod
// carries no method to strip.
func Method(fullMethod string) string {
	_, method, _ := ServiceMethod(fullMethod)
	return method
}
