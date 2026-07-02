package interceptor_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/interceptor"
)

// tagCodec is a test PayloadCodec that appends a single tag byte on Encode and
// strips it on Decode. Decode fails if the trailing byte is not its own tag,
// which surfaces incorrect ordering (decode must run in reverse of encode).
type tagCodec struct {
	tag    byte
	encErr error
	decErr error
}

func TestPayloadsOutbound(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")

	tests := []struct {
		name       string
		codecs     []interceptor.PayloadCodec
		wantErr    error
		wantCalled bool
		wantData   []byte
	}{
		{
			name:       "encodes in order",
			codecs:     []interceptor.PayloadCodec{tagCodec{tag: 0x01}, tagCodec{tag: 0x02}},
			wantCalled: true,
			wantData:   []byte{'h', 'i', 0x01, 0x02},
		},
		{
			name:    "encode error skips invoker",
			codecs:  []interceptor.PayloadCodec{tagCodec{tag: 0x01}, tagCodec{tag: 0x02, encErr: sentinel}},
			wantErr: sentinel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in, err := interceptor.Payloads(tt.codecs...)
			require.NoError(t, err)

			req := &workflowservice.StartWorkflowExecutionRequest{Input: payload('h', 'i')}

			called, err := invoke(t, in, req, &workflowservice.QueryWorkflowResponse{})
			require.Equal(t, tt.wantCalled, called)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantData, req.GetInput().GetPayloads()[0].GetData())
		})
	}
}

func TestPayloadsInbound(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")

	tests := []struct {
		name     string
		codecs   []interceptor.PayloadCodec
		respData []byte
		wantErr  error
		wantData []byte
	}{
		{
			name:     "decodes in reverse",
			codecs:   []interceptor.PayloadCodec{tagCodec{tag: 0x01}, tagCodec{tag: 0x02}},
			respData: []byte{'h', 'i', 0x01, 0x02},
			wantData: []byte{'h', 'i'},
		},
		{
			name:     "decode error propagates",
			codecs:   []interceptor.PayloadCodec{tagCodec{tag: 0x01, decErr: sentinel}},
			respData: []byte{'h', 'i', 0x01},
			wantErr:  sentinel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in, err := interceptor.Payloads(tt.codecs...)
			require.NoError(t, err)

			resp := &workflowservice.QueryWorkflowResponse{QueryResult: payload(tt.respData...)}

			// Inbound decoding runs after the RPC, so the invoker is always reached.
			called, err := invoke(t, in, &workflowservice.StartWorkflowExecutionRequest{}, resp)
			require.True(t, called)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantData, resp.GetQueryResult().GetPayloads()[0].GetData())
		})
	}
}

func TestPayloadsNoCodecs(t *testing.T) {
	t.Parallel()

	// With no codecs, Payloads short-circuits to a plain pass-through: it invokes
	// the next handler and leaves request and response payloads untouched, without
	// constructing a payload visitor.
	in, err := interceptor.Payloads()
	require.NoError(t, err)

	req := &workflowservice.StartWorkflowExecutionRequest{Input: payload('h', 'i')}
	resp := &workflowservice.QueryWorkflowResponse{QueryResult: payload('y', 'o')}

	called, err := invoke(t, in, req, resp)
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, []byte{'h', 'i'}, req.GetInput().GetPayloads()[0].GetData())
	require.Equal(t, []byte{'y', 'o'}, resp.GetQueryResult().GetPayloads()[0].GetData())
}

func (c tagCodec) Encode(_ context.Context, p *common.Payload) (*common.Payload, error) {
	if c.encErr != nil {
		return nil, c.encErr
	}

	return &common.Payload{Data: append(slices.Clone(p.Data), c.tag)}, nil
}

func (c tagCodec) Decode(_ context.Context, p *common.Payload) (*common.Payload, error) {
	if c.decErr != nil {
		return nil, c.decErr
	}

	data := p.Data
	if len(data) == 0 || data[len(data)-1] != c.tag {
		return nil, errors.New("tagCodec: unexpected trailing byte")
	}

	return &common.Payload{Data: slices.Clone(data[:len(data)-1])}, nil
}

// payload builds a single-payload Payloads message from the given bytes.
func payload(data ...byte) *common.Payloads {
	return &common.Payloads{Payloads: []*common.Payload{{Data: data}}}
}

// invoke runs interceptor over req/resp with a no-op invoker, reporting whether
// the invoker was reached.
func invoke(t *testing.T, in grpc.UnaryClientInterceptor, req, resp any) (called bool, err error) {
	t.Helper()

	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		called = true
		return nil
	}

	err = in(t.Context(), "/temporal.api.workflowservice.v1.WorkflowService/Method", req, resp, nil, invoker)
	return called, err
}
