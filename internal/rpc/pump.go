package rpc

import (
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// Pump copies messages between the inbound server stream and the outbound
	// client stream of one forwarded call, one goroutine per direction.
	Pump struct {
		cs grpc.ClientStream
		ss grpc.ServerStream
	}

	// Frame allocates the value one direction receives into, and is called once per
	// message. What that value is decides what the streams' codec does with it: a
	// typed proto message is one client interceptors can read, while an opaque byte
	// frame passes through unparsed. Returning the same value every call reuses one
	// buffer for the direction, which is safe because the direction is sequential.
	Frame func() any
)

// NewPump returns a Pump that reads requests from ss and writes them to cs, and
// reads responses from cs and writes them to ss.
func NewPump(cs grpc.ClientStream, ss grpc.ServerStream) *Pump {
	return &Pump{
		cs: cs,
		ss: ss,
	}
}

// Forward forwards the whole call, relaying the upstream's header, trailer, and
// status to the caller. A half-close from the caller closes the upstream's send
// side and the call continues, so the loop runs at most twice: the request
// direction ending is not the call ending, and only the response direction
// finishing is. Both directions are started once, up front, because starting them
// inside the select would spawn a fresh pair per iteration.
func (p *Pump) Forward(in, out Frame) error {
	reqErr := p.requests(in)
	respErr := p.responses(out)

	for range 2 {
		select {
		case err := <-reqErr:
			if errors.Is(err, io.EOF) {
				// CloseSend is documented to always report nil: a send side that broke
				// surfaces on the response direction's RecvMsg instead, which is what
				// this loop goes on to wait for.
				_ = p.cs.CloseSend()
				continue
			}

			return StatusError("forwarding the request stream failed", err)
		case err := <-respErr:
			p.ss.SetTrailer(p.cs.Trailer())
			if !errors.Is(err, io.EOF) {
				return StatusError("forwarding response stream failed", err)
			}

			return nil
		}
	}

	// Defensive: the response direction always returns above, so reaching here
	// means one of the pumps stopped without reporting why.
	return status.Error(codes.Internal, "forwarding ended without completion")
}

// requests forwards request messages the caller sends to the upstream, one in per
// message, until either side fails, and reports that first failure on the
// returned channel. io.EOF means the caller half-closed cleanly, which is the caller's cue
// to close the upstream's send side rather than to fail the call.
func (p *Pump) requests(in Frame) <-chan error {
	errs := make(chan error, 1)

	go func() {
		for {
			msg := in()
			if err := p.ss.RecvMsg(msg); err != nil {
				errs <- err // io.EOF on clean half-close.
				return
			}

			if err := p.cs.SendMsg(msg); err != nil {
				errs <- err
				return
			}
		}
	}()

	return errs
}

// responses relays the upstream's response header and then forwards the
// upstream's response messages to the caller, one out per message, until either
// side fails, reporting that first failure on the returned channel. io.EOF means the upstream completed
// cleanly; any other error is the upstream's status or a failure sending to the
// caller. The header is relayed before the first message because gRPC flushes it
// on the first send, and a header sent late is a header the caller never sees.
func (p *Pump) responses(out Frame) <-chan error {
	errs := make(chan error, 1)

	go func() {
		md, err := p.cs.Header()
		if err != nil {
			errs <- err
			return
		}

		if err := p.ss.SendHeader(md); err != nil {
			errs <- err
			return
		}

		for {
			msg := out()
			if err := p.cs.RecvMsg(msg); err != nil {
				errs <- err // io.EOF on clean completion, else the upstream status.
				return
			}

			if err := p.ss.SendMsg(msg); err != nil {
				errs <- err
				return
			}
		}
	}()

	return errs
}
