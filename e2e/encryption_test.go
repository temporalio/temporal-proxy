package e2e

import (
	"bytes"
	"encoding/base64"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
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

// TestEndToEndPayloadEncryption drives a QueryWorkflow call through the full
// stack (client -> gateway -> router -> per-upstream proxy -> fake
// upstream) with encryption enabled via a local testing:// key, and proves the
// interceptor is wired in both directions: the upstream receives sealed
// QueryArgs (outbound encryption), and the client receives the original
// plaintext QueryResult the upstream echoed back (inbound decryption).
func TestEndToEndPayloadEncryption(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)

	cfg := dataplanetest.Config(up)
	cfg.Encryption = config.Encryption{
		Enabled: true,
		Default: &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
	}

	f := dataplanetest.StartApp(t, cfg)

	secret := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
		Data:     []byte(`"the-answer-is-42"`),
	}

	resp, err := f.Client().QueryWorkflow(
		f.Context(),
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
	reqs := up.Requests()
	require.Len(t, reqs, 1)

	got, ok := reqs[0].(*workflowservice.QueryWorkflowRequest)
	require.True(t, ok)

	sealedArgs := got.GetQuery().GetQueryArgs()
	require.Len(t, sealedArgs.GetPayloads(), 1)

	sealed := sealedArgs.GetPayloads()[0]
	require.Equal(t, wireEncryptedMarker, string(sealed.GetMetadata()[wireEncoding]),
		"upstream must see the encryption marker, proving outbound sealing ran")
	require.NotEqual(t, secret.GetData(), sealed.GetData(), "upstream payload data must be ciphertext, not plaintext")
	require.NotEmpty(t, sealed.GetMetadata()[wireKeyID], "sealed payload must carry the wrapping key id")
	require.NotEmpty(t, sealed.GetMetadata()[wireDEK], "sealed payload must carry the wrapped DEK")
}

// TestEndToEndSkipsWorkerEncryptedPayloads drives a payload a worker's own codec
// already encrypted through the full stack. Not copying SkipEncodings into
// CodecOptions in dataplane.New fails the byte-for-byte check.
func TestEndToEndSkipsWorkerEncryptedPayloads(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)

	cfg := dataplanetest.Config(up)
	cfg.Encryption = config.Encryption{
		Enabled:       true,
		SkipEncodings: []string{"acme/aes-gcm"},
		Default:       &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
	}

	f := dataplanetest.StartApp(t, cfg)

	workerSealed := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("acme/aes-gcm")},
		Data:     []byte("ciphertext-from-a-worker"),
	}

	resp, err := f.Client().QueryWorkflow(f.Context(), queryWith(workerSealed), grpc.WaitForReady(true))
	require.NoError(t, err)
	require.True(t, proto.Equal(workerSealed, resp.GetQueryResult().GetPayloads()[0]))

	reqs := up.Requests()
	require.Len(t, reqs, 1)
	sent := reqs[0].(*workflowservice.QueryWorkflowRequest).GetQuery().GetQueryArgs().GetPayloads()
	require.Len(t, sent, 1)
	require.True(t, proto.Equal(workerSealed, sent[0]), "upstream must receive the worker's payload unchanged")
}

// TestEndToEndChainedProxiesSealOnce runs client -> hop A -> hop B -> upstream,
// where only A holds A's key and B lists binary/encrypted. A round trip alone
// can't prove B skipped, since B would decode its own extra layer, so B's skip
// metric is the evidence. Removing the unknown-key pass-through in
// Encryptor.Decode fails the QueryWorkflow call.
func TestEndToEndChainedProxiesSealOnce(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)

	cfgB := dataplanetest.Config(up)
	cfgB.Encryption = config.Encryption{
		Enabled:       true,
		SkipEncodings: []string{wireEncryptedMarker},
		Default:       &config.KeyPolicy{URI: testingKeyURIOf(t, 0x0b), Duration: time.Hour},
	}
	hopB := dataplanetest.StartApp(t, cfgB)

	cfgA := dataplanetest.Config(up)
	cfgA.Upstreams[0].Listen = config.ListenConfig{HostPort: hopB.Addr(), Insecure: true}
	cfgA.Encryption = config.Encryption{
		Enabled: true,
		Default: &config.KeyPolicy{URI: testingKeyURIOf(t, 0x0a), Duration: time.Hour},
	}
	hopA := dataplanetest.StartApp(t, cfgA)

	secret := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
		Data:     []byte(`"the-answer-is-42"`),
	}

	resp, err := hopA.Client().QueryWorkflow(hopA.Context(), queryWith(secret), grpc.WaitForReady(true))
	require.NoError(t, err)
	require.True(t, proto.Equal(secret, resp.GetQueryResult().GetPayloads()[0]))

	reqs := up.Requests()
	require.Len(t, reqs, 1)
	payloads := reqs[0].(*workflowservice.QueryWorkflowRequest).GetQuery().GetQueryArgs().GetPayloads()
	require.Len(t, payloads, 1)
	require.Equal(t, wireEncryptedMarker, string(payloads[0].GetMetadata()[wireEncoding]))

	requireLabel(t, hopB, "test_encryption_payloads_skipped_total", "operation", "encrypt")
	requireLabel(t, hopB, "test_encryption_payloads_skipped_total", "operation", "decrypt")
}

// testingKeyURI builds a local testing:// key URI with a fixed 32-byte key. The
// kms module rewrites testing:// to gocloud's base64key:// local keeper, so no
// cloud KMS is needed.
func testingKeyURI(t *testing.T) url.URL {
	t.Helper()

	return testingKeyURIOf(t, 0x2a)
}

// testingKeyURIOf is testingKeyURI with the key filled with b, so two proxies in
// one test can hold different keys.
func testingKeyURIOf(t *testing.T, b byte) url.URL {
	t.Helper()

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32))
	u, err := url.Parse("testing://" + key)
	require.NoError(t, err)

	return *u
}

// queryWith is a QueryWorkflow request carrying p as its only query argument.
func queryWith(p *common.Payload) *workflowservice.QueryWorkflowRequest {
	return &workflowservice.QueryWorkflowRequest{
		Namespace: "ns1",
		Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
		Query: &query.WorkflowQuery{
			QueryType: "state",
			QueryArgs: &common.Payloads{Payloads: []*common.Payload{p}},
		},
	}
}
