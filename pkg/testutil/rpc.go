package testutil

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type (
	// ClientConn is a [google.golang.org/grpc.ClientConnInterface] that succeeds or
	// fails as configured, so a test can drive an outbound call's failure modes
	// without standing up an upstream. The zero value succeeds and yields no
	// response metadata.
	ClientConn struct {
		InvokeErr error
		StreamErr error
		Stream    grpc.ClientStream
		Header    metadata.MD
		Trailer   metadata.MD
	}

	// ClientStream is a [google.golang.org/grpc.ClientStream] whose steps fail as
	// configured. When BlockRecv is non-nil, RecvMsg waits on it and then reports
	// io.EOF, which lets a test park the stream and control which of two concurrent
	// readers reports first.
	ClientStream struct {
		RecvErr   error
		SendErr   error
		HeaderErr error
		BlockRecv chan struct{}
	}

	// ServerStream is a [google.golang.org/grpc.ServerStream] whose steps fail as
	// configured. A nil RecvErr means a request message arrived.
	ServerStream struct {
		Ctx       context.Context
		RecvErr   error
		SendErr   error
		HeaderErr error
	}

	// ServerTransportStream carries the full method name a handler reads from its
	// request context, for installing via
	// [google.golang.org/grpc.NewContextWithServerTransportStream].
	ServerTransportStream struct {
		FullMethodName string
	}
)

// Invoke reports InvokeErr, having first filled the caller's header and trailer
// the way a real upstream would, so code that relays response metadata has
// something to pass along. Call options other than
// [google.golang.org/grpc.Header] and [google.golang.org/grpc.Trailer] are
// ignored.
func (c *ClientConn) Invoke(_ context.Context, _ string, _, _ any, opts ...grpc.CallOption) error {
	for _, opt := range opts {
		switch o := opt.(type) {
		case grpc.HeaderCallOption:
			*o.HeaderAddr = c.Header
		case grpc.TrailerCallOption:
			*o.TrailerAddr = c.Trailer
		}
	}

	return c.InvokeErr
}

// NewStream reports StreamErr when set, and otherwise returns the configured
// Stream, which may be nil for a caller that never reads it.
func (c *ClientConn) NewStream(
	context.Context,
	*grpc.StreamDesc,
	string,
	...grpc.CallOption,
) (grpc.ClientStream, error) {
	if c.StreamErr != nil {
		return nil, c.StreamErr
	}

	return c.Stream, nil
}

// Header reports HeaderErr and never returns metadata.
func (s *ClientStream) Header() (metadata.MD, error) { return nil, s.HeaderErr }

// Trailer returns no metadata.
func (*ClientStream) Trailer() metadata.MD { return nil }

// CloseSend always succeeds.
func (*ClientStream) CloseSend() error { return nil }

// Context returns a background context.
func (*ClientStream) Context() context.Context { return context.Background() }

// SendMsg reports SendErr.
func (s *ClientStream) SendMsg(any) error { return s.SendErr }

// RecvMsg waits on BlockRecv and reports io.EOF when one is configured, and
// otherwise reports RecvErr immediately.
func (s *ClientStream) RecvMsg(any) error {
	if s.BlockRecv != nil {
		<-s.BlockRecv

		return io.EOF
	}

	return s.RecvErr
}

// Context returns Ctx.
func (s ServerStream) Context() context.Context { return s.Ctx }

// SetHeader reports HeaderErr.
func (s ServerStream) SetHeader(metadata.MD) error { return s.HeaderErr }

// SendHeader reports HeaderErr.
func (s ServerStream) SendHeader(metadata.MD) error { return s.HeaderErr }

// SetTrailer discards the trailer.
func (ServerStream) SetTrailer(metadata.MD) {}

// SendMsg reports SendErr.
func (s ServerStream) SendMsg(any) error { return s.SendErr }

// RecvMsg reports RecvErr.
func (s ServerStream) RecvMsg(any) error { return s.RecvErr }

// Method returns FullMethodName.
func (s ServerTransportStream) Method() string { return s.FullMethodName }

// SetHeader always succeeds.
func (ServerTransportStream) SetHeader(metadata.MD) error { return nil }

// SendHeader always succeeds.
func (ServerTransportStream) SendHeader(metadata.MD) error { return nil }

// SetTrailer always succeeds.
func (ServerTransportStream) SetTrailer(metadata.MD) error { return nil }
