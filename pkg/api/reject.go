package api

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

// Error returns the detailed, server-side rejection reason. It must never be
// sent to the client directly; gRPC surfaces GRPCStatus() instead.
func (r *rejection) Error() string { return r.detail }

// GRPCStatus lets gRPC surface the client-safe status while Error keeps the
// detail server-side.
func (r *rejection) GRPCStatus() *status.Status { return r.st }
