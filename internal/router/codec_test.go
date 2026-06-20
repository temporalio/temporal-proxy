// package router (not router_test): these tests exercise the unexported frame type.
package router

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/mem"
)

func TestCodecName(t *testing.T) {
	t.Parallel()
	require.Equal(t, "proto", Codec().Name())
}

func TestCodecPassesFramesThrough(t *testing.T) {
	t.Parallel()

	c := Codec()
	payload := []byte{0x00, 0x01, 0x02, 0xff, 0x10}

	encoded, err := c.Marshal(&frame{payload: payload})
	require.NoError(t, err)
	require.Equal(t, payload, encoded.Materialize())

	var out frame
	require.NoError(t, c.Unmarshal(mem.BufferSlice{mem.SliceBuffer(payload)}, &out))
	require.Equal(t, payload, out.payload)

	// Materialize must copy: mutating the source must not affect the decoded frame.
	payload[0] = 0xFF
	require.NotEqual(t, payload, out.payload)
}

func TestCodecDelegatesNonFrameToProto(t *testing.T) {
	t.Parallel()

	c := Codec()
	proto := encoding.GetCodecV2("proto")

	msg := &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}

	viaCodec, err := c.Marshal(msg)
	require.NoError(t, err)
	viaProto, err := proto.Marshal(msg)
	require.NoError(t, err)
	require.Equal(t, viaProto.Materialize(), viaCodec.Materialize())

	var out grpc_health_v1.HealthCheckResponse
	require.NoError(t, c.Unmarshal(viaCodec, &out))
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, out.GetStatus())
}
