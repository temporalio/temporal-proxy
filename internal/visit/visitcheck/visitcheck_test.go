package visitcheck_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/temporalio/temporal-proxy/internal/visit/visitcheck"
)

func TestCollect_FindsPayloadsInMapRepeatedAndSingular(t *testing.T) {
	t.Parallel()

	p1 := &common.Payload{Data: []byte("1")}
	p2 := &common.Payload{Data: []byte("2")}
	p3 := &common.Payload{Data: []byte("3")}

	msg := &failure.Failure{
		EncodedAttributes: p1, // singular *Payload
		FailureInfo: &failure.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failure.ApplicationFailureInfo{
				Details: &common.Payloads{Payloads: []*common.Payload{p2, p3}}, // oneof -> repeated
			},
		},
	}

	got := visitcheck.Collect(msg, (&common.Payload{}).ProtoReflect().Descriptor().FullName())
	require.ElementsMatch(t, visitcheck.Addrs([]*common.Payload{p1, p2, p3}), visitcheck.Addrs(got))
}

func TestCollect_FindsPayloadsInMap(t *testing.T) {
	t.Parallel()

	pa := &common.Payload{Data: []byte("a")}
	pb := &common.Payload{Data: []byte("b")}
	memo := &common.Memo{Fields: map[string]*common.Payload{"a": pa, "b": pb}}

	got := visitcheck.Collect(memo, (&common.Payload{}).ProtoReflect().Descriptor().FullName())
	require.ElementsMatch(t, visitcheck.Addrs([]*common.Payload{pa, pb}), visitcheck.Addrs(got))
}

func TestCollect_TargetIsTreatedAsLeaf(t *testing.T) {
	t.Parallel()

	// Memo is a target. Collecting Memos must return the Memo itself and must
	// NOT descend into it (mirroring the visitor, which cb's a Memo and stops).
	memo := &common.Memo{Fields: map[string]*common.Payload{"a": {Data: []byte("x")}}}
	got := visitcheck.Collect(memo, (&common.Memo{}).ProtoReflect().Descriptor().FullName())
	require.ElementsMatch(t, visitcheck.Addrs([]*common.Memo{memo}), visitcheck.Addrs(got))
}

func TestVariants_CoversEveryOneofCase(t *testing.T) {
	t.Parallel()

	fmd := (&failure.Failure{}).ProtoReflect().Descriptor()
	oneof := fmd.Oneofs().Get(0) // failure_info
	require.Greater(t, oneof.Fields().Len(), 1, "Failure should have a multi-case oneof")

	variants := visitcheck.Variants(&failure.Failure{})
	require.GreaterOrEqual(t, len(variants), oneof.Fields().Len())

	// Each oneof case must be the set case in at least one variant.
	setCases := map[protoreflect.FullName]bool{}
	for _, v := range variants {
		m := v.ProtoReflect()
		if fd := m.WhichOneof(oneof); fd != nil {
			setCases[fd.FullName()] = true
		}
	}
	for i := 0; i < oneof.Fields().Len(); i++ {
		fd := oneof.Fields().Get(i)
		require.True(t, setCases[fd.FullName()], "oneof case %s never set across variants", fd.Name())
	}
}

func TestVariants_PopulatesSingularMessageAndScalars(t *testing.T) {
	t.Parallel()

	variants := visitcheck.Variants(&common.Memo{})
	require.NotEmpty(t, variants)

	// Every variant of Memo should have at least one populated Fields entry.
	for _, v := range variants {
		memo := v.(*common.Memo)
		require.NotEmpty(t, memo.GetFields())
		for _, p := range memo.GetFields() {
			require.NotNil(t, p)
		}
	}
}

func TestVariants_TerminatesOnRecursiveType(t *testing.T) {
	t.Parallel()

	// Failure.Cause is *Failure (self-referential). Variants must terminate and
	// still populate EncodedAttributes so the Payload visitor can be exercised.
	variants := visitcheck.Variants(&failure.Failure{})
	require.NotEmpty(t, variants)
	for _, v := range variants {
		f := v.(*failure.Failure)
		require.NotNil(t, f.GetEncodedAttributes())
	}
}
