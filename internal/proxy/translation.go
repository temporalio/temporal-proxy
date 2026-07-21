package proxy

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
)

// translatingClientStream wraps a ClientStream to translate sent messages local
// to remote and received messages remote to local.
type translatingClientStream struct {
	grpc.ClientStream

	translator *protoutil.Translator
	out        func(string) string
	in         func(string) string
}

// SendMsg translates the outbound message local to remote before sending.
func (s *translatingClientStream) SendMsg(m any) error {
	if pm, ok := m.(proto.Message); ok {
		s.translator.Translate(pm, s.out)
	}

	return s.ClientStream.SendMsg(m)
}

// RecvMsg translates the inbound message remote to local after receiving, and
// translates typed status details on a terminal error.
func (s *translatingClientStream) RecvMsg(m any) error {
	if err := s.ClientStream.RecvMsg(m); err != nil {
		return translateStatusError(s.translator, err, s.in)
	}

	if pm, ok := m.(proto.Message); ok {
		s.translator.Translate(pm, s.in)
	}

	return nil
}

// translationDialOptions returns the dial options that install namespace
// translation on the outbound connection: t rewrites message bodies and typed
// error details, out maps local names to remote on the way out, and in maps
// remote names to local on the way back.
func translationDialOptions(t *protoutil.Translator, out, in func(string) string) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(unaryClientInterceptor(t, out, in)),
		grpc.WithChainStreamInterceptor(streamClientInterceptor(t, out, in)),
	}
}

// unaryClientInterceptor translates the request local to remote before the call
// and the reply remote to local after it. On error it translates the namespace
// names in any typed status details remote to local. out maps local to remote;
// in maps remote to local.
func unaryClientInterceptor(t *protoutil.Translator, out, in func(string) string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if m, ok := req.(proto.Message); ok {
			t.Translate(m, out)
		}

		if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
			return translateStatusError(t, err, in)
		}

		if m, ok := reply.(proto.Message); ok {
			t.Translate(m, in)
		}

		return nil
	}
}

// streamClientInterceptor wraps the stream so each sent message is translated
// local to remote and each received message remote to local. out maps local to
// remote; in maps remote to local.
func streamClientInterceptor(t *protoutil.Translator, out, in func(string) string) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, translateStatusError(t, err, in)
		}

		return &translatingClientStream{ClientStream: cs, translator: t, out: out, in: in}, nil
	}
}

// translateStatusError rewrites namespace names carried in the typed status
// details of a gRPC error using fn. Errors that do not carry a gRPC status, and
// the status code and free-text message, are returned unchanged; only typed
// details are translated. A nil error returns nil.
func translateStatusError(t *protoutil.Translator, err error, fn func(string) string) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	pb := st.Proto()
	if pb == nil || len(pb.Details) == 0 {
		return err
	}

	changed := false
	for i, detail := range pb.Details {
		msg, uerr := detail.UnmarshalNew()
		if uerr != nil {
			continue
		}

		original := proto.Clone(msg)
		t.Translate(msg, fn)
		if proto.Equal(original, msg) {
			// This detail carried no namespace; leave the original Any in place.
			continue
		}

		repacked, perr := anypb.New(msg)
		if perr != nil {
			continue
		}

		// Assign by index; do not struct-copy the Any (it embeds a sync.Mutex
		// via MessageState, which go vet copylocks rejects).
		pb.Details[i] = repacked
		changed = true
	}

	if !changed {
		// Nothing was translated; return the original error so its identity is
		// preserved for callers that compare by pointer.
		return err
	}

	return status.FromProto(pb).Err()
}
