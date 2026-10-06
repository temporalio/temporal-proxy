package server

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/rpc"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// RecoveryHandler is told of each panic [WithRecoveryHandler] recovers, after it
// has been logged. ctx is the request's and method its full gRPC method name.
// [Reporter.Panic] is one, counting the panic by method.
type RecoveryHandler func(ctx context.Context, method string)

// WithRecoveryHandler appends a stream server interceptor that turns a panic
// further down the chain into a generic Internal status, logging the panic and
// its stack to log and then calling fn, which may be nil. It covers both a panic
// on the handler's goroutine and one an [rpc.Pump] direction recovered and
// returned as an [rpc.PanicError], since forwarding runs interceptors on those
// goroutines too.
//
// The interceptor takes its place in the chain in the order supplied alongside
// [WithStreamInterceptor], and recovers only what runs after it, and only on
// streams: a registered unary method, such as the health service's Check,
// bypasses stream interceptors. Forwarded methods are all served as streams. Put
// it after [Reporter.StreamInterceptor], so a recovered RPC is still recorded
// with its Internal code; that interceptor does nothing that can panic on its
// own.
//
// Recovery keeps the process serving, not its state sound: a panic can leave a
// lock held or a cache half-written, and nothing here can tell.
func WithRecoveryHandler(log logger.Logger, fn RecoveryHandler) Option {
	return WithStreamInterceptor(recoveryInterceptor(log, fn))
}

func recoveryInterceptor(log logger.Logger, fn RecoveryHandler) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = rpc.NewPanicError(p)
			}

			var pe *rpc.PanicError
			if !errors.As(err, &pe) {
				return
			}

			log.Error("recovered panic serving RPC",
				tag.String("method", info.FullMethod),
				tag.String("panic", fmt.Sprint(pe.Value)),
				tag.String("stack", string(pe.Stack)),
			)
			if fn != nil {
				fn(ss.Context(), info.FullMethod)
			}

			// Unwrapped, so the caller sees the generic status and not the panic.
			err = pe
		}()

		return handler(srv, ss)
	}
}
