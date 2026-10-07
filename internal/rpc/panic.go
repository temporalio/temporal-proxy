package rpc

import (
	"fmt"
	"runtime/debug"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errPanic is the status a caller sees in place of a recovered panic.
var errPanic = status.New(codes.Internal, "internal error")

// PanicError is a recovered panic, carrying the panic value and the stack it was
// raised on for server-side logging. Like a rejection, it reports a generic
// Internal status to the caller: the panic value can carry request contents, so
// it must never go over the wire.
//
// Return the result unwrapped, for the same reason as [Reject]: gRPC reads
// GRPCStatus verbatim only for an error it can type-assert directly.
type PanicError struct {
	Value any
	Stack []byte
}

// NewPanicError builds a PanicError for p, capturing the current stack. Call it
// from the deferred function that recovered p, before the stack unwinds, so the
// stack still holds the frame that panicked.
func NewPanicError(p any) *PanicError {
	return &PanicError{Value: p, Stack: debug.Stack()}
}

// Error returns the panic value. It must never be sent to the client directly;
// gRPC surfaces GRPCStatus() instead.
func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }

// GRPCStatus lets gRPC surface the generic status while Error keeps the panic
// value server-side.
func (e *PanicError) GRPCStatus() *status.Status { return errPanic }
