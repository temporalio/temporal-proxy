package rpc

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rejection is an authentication failure that reports a generic, client-safe
// gRPC status to the caller while carrying a detailed reason for server-side
// logging. The detail must never contain secrets (tokens or key material).
type rejection struct {
	st     *status.Status
	detail string
}

// Reject builds a rejection carrying a client-safe code+message and a
// server-side detail for logging.
//
// Return the result unwrapped. gRPC reads GRPCStatus verbatim only for an error
// it can type-assert directly; for a wrapped one it keeps the code but replaces
// the message with err.Error(), which is the detail. Adding context with %w
// therefore sends the caller exactly what this type withholds, so put the
// context in the detail instead.
func Reject(code codes.Code, clientMsg, detail string) error {
	return &rejection{st: status.New(code, clientMsg), detail: detail}
}

// StatusError maps a forwarding error to the gRPC status returned to the caller,
// naming the step that failed as what. It forwards an error already carrying a
// status verbatim, maps a raw context error to its status, and otherwise reports
// Internal. Callers own the whole message, so what carries their own package
// prefix.
func StatusError(what string, err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}

	if st := status.FromContextError(err); st.Code() != codes.Unknown {
		return st.Err()
	}

	return status.Errorf(codes.Internal, "%s: %v", what, err)
}

// Error returns the detailed, server-side rejection reason. It must never be
// sent to the client directly; gRPC surfaces GRPCStatus() instead.
func (r *rejection) Error() string { return r.detail }

// GRPCStatus lets gRPC surface the client-safe status while Error keeps the
// detail server-side.
func (r *rejection) GRPCStatus() *status.Status { return r.st }
