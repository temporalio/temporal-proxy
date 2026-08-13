package router

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/rpc"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

type (
	// Reflector extracts the Temporal namespace from a request. Namespace
	// receives the full method and the raw bytes of the first request message
	// and returns the namespace, or "" when it cannot determine one.
	Reflector interface {
		Namespace(string, []byte) string
	}

	// replayStream overrides Context so a handler sees the [meta.Target] the
	// interceptor resolved, and hands back the first frame the interceptor
	// consumed before deferring to the real stream. A stream that was not peeked
	// starts out replayed, so every read goes straight to the real stream.
	replayStream struct {
		grpc.ServerStream
		ctx      context.Context
		first    *frame
		replayed bool
	}
)

// PeekInterceptor returns a stream server interceptor that resolves the stream's
// [meta.Target] before any later stage runs, so authentication and routing decide
// on the same view of the request. Every stream gets the method it named. For a
// service a forwards, it also buffers the first client frame, reads the namespace
// out of it with r, and hands the handler a stream that replays that frame; for
// any other service it leaves the stream untouched, so whatever serves it reads
// its own first message.
//
// Install it ahead of anything that reads the Target, and note that it is what
// makes [Handler] usable at all: the handler refuses a stream this never saw.
func PeekInterceptor(r Reflector, a services.Allowlist) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// The method travels for every stream, because authentication runs for
		// every stream and a provider weighs the method being invoked.
		target := meta.Target{FullName: info.FullMethod}

		// Only a service the router forwards has its payload read. A service
		// registered on this server reads the first message into its own type, so
		// consuming it here would take it away. The rejection stays with the
		// handler: answering here would tell a caller which services exist before
		// it has authenticated.
		if !a.Allows(rpc.Service(info.FullMethod)) {
			// Nothing is buffered, so every read goes to the real stream.
			return handler(srv, &replayStream{
				ServerStream: ss,
				ctx:          meta.WithTarget(ss.Context(), target),
				replayed:     true,
			})
		}

		// io.EOF means the client half-closed without sending a message, which is
		// legal: there is simply no payload to read a namespace from.
		first := &frame{}
		err := ss.RecvMsg(first)
		eof := errors.Is(err, io.EOF)
		if err != nil && !eof {
			return rpc.StatusError("router: reading the first request failed", err)
		}

		replay := &replayStream{ServerStream: ss}
		if !eof {
			target.Namespace = r.Namespace(info.FullMethod, first.payload)
			replay.first = first
		}

		replay.ctx = meta.WithTarget(ss.Context(), target)

		return handler(srv, replay)
	}
}

// Context returns the context carrying the resolved Target.
func (s *replayStream) Context() context.Context { return s.ctx }

// RecvMsg returns the buffered first frame once, then reads from the real stream:
// the interceptor took that frame off the wire, so this hands it back rather than
// leaving the handler a message short. A nil buffered frame means the interceptor
// saw the client half-close, so the handler observes the same io.EOF the real
// stream would have given it.
func (s *replayStream) RecvMsg(m any) error {
	if s.replayed {
		return s.ServerStream.RecvMsg(m)
	}

	// NB: unlocked deliberately. gRPC delivers one RecvMsg at a time per stream, so
	// replayed and first are only ever touched by that stream's own goroutine.
	s.replayed = true
	if s.first == nil {
		return io.EOF
	}

	payload := s.first.payload
	s.first = nil

	// Only a forwarded service is peeked and the router reads frames, so any other
	// destination type means the two have drifted apart. Reported rather than
	// decoded: decoding would quietly support a pairing nothing produces, and an
	// error here cannot take the process down, which a type assertion could since
	// the server installs no panic recovery.
	f, ok := m.(*frame)
	if !ok {
		return status.Errorf(codes.Internal, "router: cannot replay the first request into %T, want a frame", m)
	}

	f.payload = payload

	return nil
}
