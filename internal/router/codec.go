package router

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
)

var defaultCodec = passthroughCodec{proto: encoding.GetCodecV2(proto.Name)}

type (
	// frame is the message type the pass-through codec recognizes. It carries
	// already-serialized bytes so the router never parses payloads.
	frame struct {
		payload []byte
	}

	// passthroughCodec passes frames through unmodified and delegates every
	// other message to the standard proto codec, so a server using it can both
	// transparently forward unknown methods and host real proto services.
	passthroughCodec struct {
		proto encoding.CodecV2
	}
)

// Codec returns the hybrid pass-through codec. It must be applied per-call via
// grpc.ForceServerCodecV2 / grpc.ForceCodecV2; it is deliberately not registered
// globally so it never shadows the real proto codec process-wide.
func Codec() encoding.CodecV2 {
	return defaultCodec
}

func (c passthroughCodec) Marshal(v any) (mem.BufferSlice, error) {
	if f, ok := v.(*frame); ok {
		return mem.BufferSlice{mem.SliceBuffer(f.payload)}, nil
	}

	return c.proto.Marshal(v)
}

func (c passthroughCodec) Unmarshal(data mem.BufferSlice, v any) error {
	if f, ok := v.(*frame); ok {
		// Materialize copies the bytes; data is freed when Unmarshal returns.
		f.payload = data.Materialize()
		return nil
	}

	return c.proto.Unmarshal(data, v)
}

// Name reports "proto" so relayed traffic keeps the standard content-subtype and
// remains transparent to clients and upstreams.
func (c passthroughCodec) Name() string {
	return proto.Name
}
