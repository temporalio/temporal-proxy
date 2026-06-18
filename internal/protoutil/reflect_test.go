package protoutil_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
)

type (
	fakeReq  struct{}
	fakeResp struct{}

	fakeStream interface {
		Send(*fakeResp) error
	}

	// fakeServer mimics a generated gRPC server interface: two unary RPCs, one
	// streaming RPC, and an unexported bookkeeping method. Only the unary RPCs
	// should be parsed.
	fakeServer interface {
		StartThing(context.Context, *fakeReq) (*fakeResp, error)
		StopThing(context.Context, *fakeReq) (*fakeResp, error)
		StreamThing(*fakeReq, fakeStream) error
		mustEmbedUnimplementedFakeServer()
	}
)

func TestIsUnaryRPC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   any
		want bool
	}{
		{"unary", func(context.Context, *fakeReq) (*fakeResp, error) { return nil, nil }, true},
		{"variadic", func(context.Context, ...*fakeReq) (*fakeResp, error) { return nil, nil }, false},
		{"too few params", func(context.Context) (*fakeResp, error) { return nil, nil }, false},
		{"first param not context", func(*fakeReq, *fakeReq) (*fakeResp, error) { return nil, nil }, false},
		{"request not pointer", func(context.Context, fakeReq) (*fakeResp, error) { return nil, nil }, false},
		{"too few results", func(context.Context, *fakeReq) error { return nil }, false},
		{"first result not pointer", func(context.Context, *fakeReq) (fakeResp, error) { return fakeResp{}, nil }, false},
		{"second result not error", func(context.Context, *fakeReq) (*fakeResp, *fakeResp) { return nil, nil }, false},
		{"no params or results", func() {}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, protoutil.IsUnaryRPC(reflect.TypeOf(tt.fn)))
		})
	}
}

func TestParseService(t *testing.T) {
	t.Parallel()

	svc, err := protoutil.ParseService(reflect.TypeFor[fakeServer]())
	require.NoError(t, err)
	require.Equal(t, "fakeServer", svc.Name)
	require.Equal(t, reflect.TypeFor[fakeServer](), svc.Type)

	names := make([]string, 0, len(svc.RPCs))
	for _, rpc := range svc.RPCs {
		names = append(names, rpc.Name)
		require.True(t, rpc.Unary)
		require.Equal(t, reflect.TypeFor[fakeReq](), rpc.Req)
		require.Equal(t, reflect.TypeFor[fakeResp](), rpc.Resp)
	}

	require.ElementsMatch(t, []string{"StartThing", "StopThing"}, names,
		"only unary RPCs are parsed; streaming and bookkeeping methods are skipped")
}

func TestParseService_RealWorkflowService(t *testing.T) {
	t.Parallel()

	svc, err := protoutil.ParseService(reflect.TypeFor[workflowservice.WorkflowServiceServer]())
	require.NoError(t, err)
	require.Equal(t, "WorkflowServiceServer", svc.Name)
	require.NotEmpty(t, svc.RPCs)

	for _, rpc := range svc.RPCs {
		require.True(t, rpc.Unary)
		require.Equal(t, reflect.Struct, rpc.Req.Kind(), "%s request should be a message struct", rpc.Name)
		require.Equal(t, reflect.Struct, rpc.Resp.Kind(), "%s response should be a message struct", rpc.Name)
	}
}
