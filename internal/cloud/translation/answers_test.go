package translation_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/cloud/translation"
)

func TestDefaultAnswersEmpty(t *testing.T) {
	t.Parallel()

	// Each reply is pre-filled, so the test shows the answer leaves it empty rather
	// than merely not touching it: a reused message must not leak a value. The
	// fake upstream implements none of these methods, so a call that reached it
	// would fail Unimplemented instead.
	tests := []struct {
		name   string
		method string
		req    proto.Message
		reply  proto.Message
		empty  proto.Message
	}{
		{
			name:   "GetClusterInfo",
			method: "/temporal.api.workflowservice.v1.WorkflowService/GetClusterInfo",
			req:    &workflowservice.GetClusterInfoRequest{},
			reply:  &workflowservice.GetClusterInfoResponse{ClusterName: "stale"},
			empty:  &workflowservice.GetClusterInfoResponse{},
		},
	}

	r, err := translation.Default()
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			upstream := newSystemInfoService(t)
			cc := dial(t, upstream, translation.DialOptions(r)...)

			require.NoError(t, cc.Invoke(t.Context(), tt.method, tt.req, tt.reply))
			require.True(t, proto.Equal(tt.empty, tt.reply), "got %v", tt.reply)
			require.False(t, upstream.wasCalled())
		})
	}
}
