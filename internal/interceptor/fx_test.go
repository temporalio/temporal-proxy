package interceptor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/interceptor"
)

// unaryGroup collects the interceptors Module contributes. The group name
// mirrors proxy.UnaryInterceptorsTag, where the proxy consumes them.
type unaryGroup struct {
	fx.In

	Interceptors []grpc.UnaryClientInterceptor `group:"proxy_unary_interceptors"`
}

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("contributes an interceptor that applies the supplied codecs", func(t *testing.T) {
		t.Parallel()

		var got unaryGroup
		app := fx.New(
			interceptor.Module,
			fx.Supply([]interceptor.PayloadCodec{tagCodec{tag: 0x01}}),
			fx.Populate(&got),
			fx.NopLogger,
		)
		require.NoError(t, app.Err())
		require.Len(t, got.Interceptors, 1)

		// Running the contributed interceptor over a request encodes its payload,
		// proving the supplied codec was wired through to Payloads.
		req := &workflowservice.StartWorkflowExecutionRequest{Input: payload('h', 'i')}
		_, err := invoke(t, got.Interceptors[0], req, &workflowservice.QueryWorkflowResponse{})
		require.NoError(t, err)
		require.Equal(t, []byte{'h', 'i', 0x01}, req.GetInput().GetPayloads()[0].GetData())
	})

	t.Run("contributes a passthrough interceptor when no codecs are supplied", func(t *testing.T) {
		t.Parallel()

		var got unaryGroup
		app := fx.New(
			interceptor.Module,
			fx.Populate(&got),
			fx.NopLogger,
		)
		require.NoError(t, app.Err())
		require.Len(t, got.Interceptors, 1)

		req := &workflowservice.StartWorkflowExecutionRequest{Input: payload('h', 'i')}
		_, err := invoke(t, got.Interceptors[0], req, &workflowservice.QueryWorkflowResponse{})
		require.NoError(t, err)
		require.Equal(t, []byte{'h', 'i'}, req.GetInput().GetPayloads()[0].GetData())
	})
}
