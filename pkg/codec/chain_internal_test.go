// These tests live in package codec because NewChain hides its codec list, so a
// chain of more than one codec cannot be built from outside the package until a
// second codec exists. The order Chain applies codecs in is the thing most likely
// to break silently when one does, so it is pinned here rather than left untested.
package codec

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
)

// markerCodec appends its own name to a payload's data on Encode and requires it
// to be the trailing marker on Decode, so a chain that applies codecs in the
// wrong order fails loudly instead of quietly producing the wrong bytes.
type markerCodec struct {
	name string
}

func TestChainAppliesCodecsInOrder(t *testing.T) {
	t.Parallel()

	chain := Chain{codecs: []Codec{&markerCodec{name: "A"}, &markerCodec{name: "B"}}}

	encoded, err := chain.Encode([]*common.Payload{{Data: []byte("data")}})
	require.NoError(t, err)
	require.Len(t, encoded, 1)

	// A ran first, then B, so B's marker is outermost. Reversed, this would read
	// "dataBA".
	require.Equal(t, "dataAB", string(encoded[0].Data))
}

func TestChainDecodesInReverseOrder(t *testing.T) {
	t.Parallel()

	chain := Chain{codecs: []Codec{&markerCodec{name: "A"}, &markerCodec{name: "B"}}}

	decoded, err := chain.Decode([]*common.Payload{{Data: []byte("dataAB")}})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, "data", string(decoded[0].Data))
}

func TestChainRoundtripsThroughEveryCodec(t *testing.T) {
	t.Parallel()

	chain := Chain{codecs: []Codec{&markerCodec{name: "A"}, &markerCodec{name: "B"}}}
	original := []*common.Payload{{Data: []byte("data")}}

	encoded, err := chain.Encode(original)
	require.NoError(t, err)

	decoded, err := chain.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, "data", string(decoded[0].Data))
}

func TestChainStopsAtTheFirstError(t *testing.T) {
	t.Parallel()

	// "B" is missing from the data, so B's Decode fails and A's never runs. If it
	// did run, it would fail differently: on the missing "A" marker.
	chain := Chain{codecs: []Codec{&markerCodec{name: "A"}, &markerCodec{name: "B"}}}

	_, err := chain.Decode([]*common.Payload{{Data: []byte("data")}})
	require.ErrorContains(t, err, `marker "B" not found`)
}

func (m *markerCodec) Encode(payloads []*common.Payload) ([]*common.Payload, error) {
	res := make([]*common.Payload, len(payloads))
	for i, p := range payloads {
		res[i] = &common.Payload{
			Metadata: p.Metadata,
			Data:     append(bytes.Clone(p.Data), m.name...),
		}
	}

	return res, nil
}

func (m *markerCodec) Decode(payloads []*common.Payload) ([]*common.Payload, error) {
	res := make([]*common.Payload, len(payloads))
	for i, p := range payloads {
		if !bytes.HasSuffix(p.Data, []byte(m.name)) {
			return nil, fmt.Errorf("marker %q not found", m.name)
		}

		res[i] = &common.Payload{
			Metadata: p.Metadata,
			Data:     bytes.TrimSuffix(bytes.Clone(p.Data), []byte(m.name)),
		}
	}

	return res, nil
}
