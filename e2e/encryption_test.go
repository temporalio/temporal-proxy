package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// These mirror the proxy's on-the-wire encryption metadata contract
// (internal/proxy/encryption.go). They are duplicated here because e2e is an
// external package and the constants are unexported; this test verifies the
// observable wire format, so pinning the literals is intentional.
const (
	wireEncoding        = "encoding"
	wireEncryptedMarker = "binary/encrypted"
	wireKeyID           = "encryption-key-id"
	wireDEK             = "encryption-dek"
)

// queryEchoService is a fake upstream WorkflowService frontend that records the
// QueryArgs it receives and echoes them straight back as the QueryResult. The
// echo lets a single QueryWorkflow call exercise both directions of the proxy's
// encryption interceptor: the recorded args prove outbound sealing, and the
// echoed result (still ciphertext on the wire) is what the proxy opens on the
// way back.
type queryEchoService struct {
	workflowservice.UnimplementedWorkflowServiceServer

	mu      sync.Mutex
	gotArgs *common.Payloads
}

// TestEndToEndPayloadEncryption drives a QueryWorkflow call through the full
// stack (client -> inbound server -> router -> per-upstream proxy -> fake
// upstream) with encryption enabled via a local testing:// key, and proves the
// interceptor is wired in both directions: the upstream receives sealed
// QueryArgs (outbound encryption), and the client receives the original
// plaintext QueryResult the upstream echoed back (inbound decryption).
func TestEndToEndPayloadEncryption(t *testing.T) {
	t.Parallel()

	svc := &queryEchoService{}
	upstreamAddr := newPlaintextUpstream(t, svc)

	inboundAddr := freeTCPAddr(t)
	app := newFullApp(t, &config.Config{
		Listen:  config.ListenConfig{HostPort: inboundAddr},
		Routing: config.Routing{DefaultUpstream: "workers"},
		Encryption: config.Encryption{
			Enabled: true,
			Default: &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
		},
		Upstreams: []config.Upstream{{
			Name:   "workers",
			Listen: config.ListenConfig{HostPort: upstreamAddr},
		}},
	})
	require.NoError(t, app.Err())

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		_ = app.Stop(stopCtx)
	})

	conn := dialInbound(t, inboundAddr)
	defer func() { _ = conn.Close() }()

	secret := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
		Data:     []byte(`"the-answer-is-42"`),
	}

	resp, err := workflowservice.NewWorkflowServiceClient(conn).QueryWorkflow(
		startCtx,
		&workflowservice.QueryWorkflowRequest{
			Namespace: "ns1",
			Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
			Query: &query.WorkflowQuery{
				QueryType: "state",
				QueryArgs: &common.Payloads{Payloads: []*common.Payload{secret}},
			},
		},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	// Inbound: the echoed QueryResult was decrypted back to the original plaintext.
	require.Len(t, resp.GetQueryResult().GetPayloads(), 1)
	require.True(t, proto.Equal(secret, resp.GetQueryResult().GetPayloads()[0]),
		"client must receive the original plaintext payload after inbound decryption")

	// Outbound: the upstream saw ciphertext, not the plaintext the client sent.
	got := svc.received()
	require.Len(t, got.GetPayloads(), 1)
	sealed := got.GetPayloads()[0]
	require.Equal(t, wireEncryptedMarker, string(sealed.Metadata[wireEncoding]),
		"upstream must see the encryption marker, proving outbound sealing ran")
	require.NotEqual(t, secret.Data, sealed.Data, "upstream payload data must be ciphertext, not plaintext")
	require.NotEmpty(t, sealed.Metadata[wireKeyID], "sealed payload must carry the wrapping key id")
	require.NotEmpty(t, sealed.Metadata[wireDEK], "sealed payload must carry the wrapped DEK")
}

func (s *queryEchoService) QueryWorkflow(
	_ context.Context, req *workflowservice.QueryWorkflowRequest,
) (*workflowservice.QueryWorkflowResponse, error) {
	args := req.GetQuery().GetQueryArgs()

	s.mu.Lock()
	s.gotArgs = args
	s.mu.Unlock()

	return &workflowservice.QueryWorkflowResponse{QueryResult: args}, nil
}

func (s *queryEchoService) received() *common.Payloads {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.gotArgs
}

// newPlaintextUpstream stands up a fake WorkflowService frontend over plaintext
// TCP (this test exercises payload encryption, not transport TLS) and returns
// its address.
func newPlaintextUpstream(t *testing.T, svc workflowservice.WorkflowServiceServer) string {
	t.Helper()

	srv := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(srv, svc)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// testingKeyURI builds a local testing:// key URI with a fixed 32-byte key. The
// kms module rewrites testing:// to gocloud's base64key:// local keeper, so no
// cloud KMS is needed.
func testingKeyURI(t *testing.T) url.URL {
	t.Helper()

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x2a}, 32))
	u, err := url.Parse("testing://" + key)
	require.NoError(t, err)

	return *u
}
