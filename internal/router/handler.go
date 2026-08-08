package router

import (
	"context"
	"errors"
	"io"
	"maps"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/rpc"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

type (
	// Target is the routing result returned by Director.Resolve on success: the
	// chosen upstream's name (always non-empty) and the connection to forward the
	// stream over. On a non-nil error the Target is unused and callers must ignore
	// its fields.
	Target struct {
		Upstream string
		Conn     *grpc.ClientConn
	}

	// Director selects the upstream for a request. Resolve receives the full
	// method, the namespace peeked from the first request message (empty when the
	// client sent no message), and the incoming metadata, and returns the Target to
	// forward over. A non-nil error aborts the stream and is returned to the caller
	// verbatim, so implementations should return a gRPC status error.
	Director interface {
		Resolve(ctx context.Context, method, namespace string, md map[string][]string) (Target, error)
	}

	// Reflector extracts the Temporal namespace from a request. Namespace
	// receives the full method and the raw bytes of the first request message
	// and returns the namespace, or "" when it cannot determine one.
	Reflector interface {
		Namespace(string, []byte) string
	}

	// Gate reports whether the proxy will forward the named service. Allows
	// receives a service full name, never a full method; the Module wires a
	// services.Allowlist built from the configured allowlist.
	Gate interface {
		Allows(string) bool
	}
)

// Handler returns a grpc.StreamHandler suitable for grpc.UnknownServiceHandler,
// reporting a stream_setup forwarding error via rep when opening the upstream
// stream fails. A method whose service g does not allow is rejected with
// Unimplemented before any upstream work, so the proxy answers as a server that
// does not implement it rather than revealing that an upstream might.
// It buffers the first request frame so r can peek the request
// namespace, asks d for the upstream connection, then transparently forwards
// the stream to that upstream using the same full method name: it replays the
// buffered first frame, pumps raw frames in both directions, and propagates
// header, trailer, and status verbatim.
func Handler(d Director, r Reflector, g Gate, rep *Reporter) grpc.StreamHandler {
	return func(_ any, serverStream grpc.ServerStream) error {
		ctx := serverStream.Context()
		method, err := rpc.FullMethod(ctx)
		if err != nil {
			return err
		}

		if svc := rpc.Service(method); !g.Allows(svc) {
			return status.Errorf(codes.Unimplemented, "unknown service %q", svc)
		}

		var md map[string][]string
		outCtx := ctx
		if inMD, ok := metadata.FromIncomingContext(ctx); ok {
			outCtx = metadata.NewOutgoingContext(ctx, inMD.Copy())
			md = inMD
		}

		// Buffer the first client frame so we can read the namespace before
		// choosing an upstream. io.EOF means the client half-closed without
		// sending a message (namespace is empty).
		first := &frame{}
		firstErr := serverStream.RecvMsg(first)
		eof := errors.Is(firstErr, io.EOF)
		if firstErr != nil && !eof {
			return rpc.StatusError("router: reading the first request failed", firstErr)
		}

		namespace := ""
		if !eof {
			namespace = r.Namespace(method, first.payload)
		}

		// Carry the extracted namespace to the upstream proxy so it can resolve a
		// templated address without re-parsing the payload. Set (not append) so a
		// client-supplied value cannot influence routing.
		outCtx = meta.WithNamespace(outCtx, namespace)

		target, err := d.Resolve(ctx, method, namespace, maps.Clone(md))
		if err != nil {
			return err
		}

		stream, err := target.Conn.NewStream(
			outCtx,
			&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
			method,
			grpc.ForceCodecV2(Codec()),
		)
		if err != nil {
			if rep != nil {
				rep.ForwardingError(target.Upstream, reasonStreamSetup)
			}

			return err
		}

		if eof {
			if err := stream.CloseSend(); err != nil {
				return rpc.StatusError("router: closing the upstream send side failed", err)
			}
		} else if err := stream.SendMsg(first); err != nil {
			return rpc.StatusError("router: forwarding the first request failed", err)
		}

		return rpc.NewPump(stream, serverStream).Forward(
			func() any { return &frame{} },
			func() any { return &frame{} },
		)
	}
}
