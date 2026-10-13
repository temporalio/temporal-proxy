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
	wireNamespace       = "encryption-namespace"
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
		Enabled:          true,
		NamespaceBinding: config.NamespaceBindingStrict,
		Default:          &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
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
		Enabled:          true,
		NamespaceBinding: config.NamespaceBindingStrict,
		SkipEncodings:    []string{"acme/aes-gcm"},
		Default:          &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
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
		Enabled:          true,
		NamespaceBinding: config.NamespaceBindingStrict,
		SkipEncodings:    []string{wireEncryptedMarker},
		Default:          &config.KeyPolicy{URI: testingKeyURIOf(t, 0x0b), Duration: time.Hour},
	}
	hopB := dataplanetest.StartApp(t, cfgB)

	cfgA := dataplanetest.Config(up)
	cfgA.Upstreams[0].Listen = config.ListenConfig{HostPort: hopB.Addr(), Insecure: true}
	cfgA.Encryption = config.Encryption{
		Enabled:          true,
		NamespaceBinding: config.NamespaceBindingStrict,
		Default:          &config.KeyPolicy{URI: testingKeyURIOf(t, 0x0a), Duration: time.Hour},
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

// queryWith is a QueryWorkflow request in ns1 carrying p as its only query
// argument.
func queryWith(p *common.Payload) *workflowservice.QueryWorkflowRequest {
	return queryIn("ns1", p)
}

// queryIn is queryWith in namespace ns.
func queryIn(ns string, p *common.Payload) *workflowservice.QueryWorkflowRequest {
	return &workflowservice.QueryWorkflowRequest{
		Namespace: ns,
		Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
		Query: &query.WorkflowQuery{
			QueryType: "state",
			QueryArgs: &common.Payloads{Payloads: []*common.Payload{p}},
		},
	}
}

// TestEndToEndNamespaceBinding drives a payload sealed by a writer proxy into a
// reader proxy holding the same key, through the real vault, for each rollout
// step. Not passing the mode in createVault fails the label checks; strict
// accepting unbound payloads fails the last row.
func TestEndToEndNamespaceBinding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		writer    config.NamespaceBinding
		reader    config.NamespaceBinding
		wantLabel bool
		wantErr   bool
	}{
		{name: "off writer, permissive reader", writer: config.NamespaceBindingOff, reader: config.NamespaceBindingPermissive},
		{name: "permissive writer, off reader", writer: config.NamespaceBindingPermissive, reader: config.NamespaceBindingOff, wantLabel: true},
		{name: "permissive writer, strict reader", writer: config.NamespaceBindingPermissive, reader: config.NamespaceBindingStrict, wantLabel: true},
		{name: "off writer, strict reader", writer: config.NamespaceBindingOff, reader: config.NamespaceBindingStrict, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := testingKeyURI(t)
			secret := &common.Payload{
				Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
				Data:     []byte(`"the-answer-is-42"`),
			}

			sealed := sealThrough(t, key, tc.writer, secret)
			label, ok := sealed.GetPayloads()[0].GetMetadata()[wireNamespace]
			require.Equal(t, tc.wantLabel, ok)
			if ok {
				require.Equal(t, "ns1", string(label))
			}

			rup := dataplanetest.NewUpstream(t)
			rup.SetQueryResult(sealed)
			rcfg := dataplanetest.Config(rup)
			rcfg.Metrics.Labels.Namespace = true
			rcfg.Encryption = config.Encryption{
				Enabled:          true,
				NamespaceBinding: tc.reader,
				Default:          &config.KeyPolicy{URI: key, Duration: time.Hour},
			}
			reader := dataplanetest.StartApp(t, rcfg)

			plain := &common.Payload{Metadata: map[string][]byte{wireEncoding: []byte("json/plain")}, Data: []byte(`"q"`)}
			resp, err := reader.Client().QueryWorkflow(reader.Context(), queryWith(plain), grpc.WaitForReady(true))
			if tc.wantErr {
				require.Error(t, err)
				requireLabel(t, reader, "test_encryption_vault_ops_total", "result", "unbound")
				requireLabel(t, reader, "test_encryption_vault_unbound_opens_total", "namespace", "ns1")
				return
			}

			require.NoError(t, err)
			require.True(t, proto.Equal(secret, resp.GetQueryResult().GetPayloads()[0]))
			if !tc.wantLabel {
				requireLabel(t, reader, "test_encryption_vault_unbound_opens_total", "namespace", "ns1")
			}
		})
	}
}

// TestEndToEndNamespaceComparison seals a payload through one proxy and reads it
// back through another holding the same key, from the same or another
// namespace. Not wiring the upstream's translator into the codec interceptor
// fails the translated row's label check; not comparing on the gRPC path fails
// the strict row.
func TestEndToEndNamespaceComparison(t *testing.T) {
	t.Parallel()

	acct := config.NamespaceRules{Suffix: ".acct"}

	tests := []struct {
		name      string
		writerNS  string
		readerNS  string
		rules     config.NamespaceRules
		reader    config.NamespaceBinding
		wantLabel string
		wantErr   bool
		wantCount bool
	}{
		{
			name: "strict refuses another namespace", writerNS: "ns-a", readerNS: "ns-b",
			reader: config.NamespaceBindingStrict, wantLabel: "ns-a", wantErr: true, wantCount: true,
		},
		{
			name: "permissive opens another namespace and counts it", writerNS: "ns-a", readerNS: "ns-b",
			reader: config.NamespaceBindingPermissive, wantLabel: "ns-a", wantCount: true,
		},
		{
			name: "strict opens its own namespace through translation", writerNS: "ns1", readerNS: "ns1",
			rules: acct, reader: config.NamespaceBindingStrict, wantLabel: "ns1.acct",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := testingKeyURI(t)
			secret := &common.Payload{
				Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
				Data:     []byte(`"the-answer-is-42"`),
			}

			sealed := sealThroughIn(t, key, config.NamespaceBindingPermissive, tc.writerNS, tc.rules, secret)
			require.Equal(t, tc.wantLabel, string(sealed.GetPayloads()[0].GetMetadata()[wireNamespace]))

			rup := dataplanetest.NewUpstream(t)
			rup.SetQueryResult(sealed)
			rcfg := dataplanetest.Config(rup)
			rcfg.Upstreams[0].Namespaces.Rules = tc.rules
			rcfg.Metrics.Labels.Namespace = true
			rcfg.Encryption = config.Encryption{
				Enabled:          true,
				NamespaceBinding: tc.reader,
				Default:          &config.KeyPolicy{URI: key, Duration: time.Hour},
			}
			reader := dataplanetest.StartApp(t, rcfg)

			plain := &common.Payload{Metadata: map[string][]byte{wireEncoding: []byte("json/plain")}, Data: []byte(`"q"`)}
			resp, err := reader.Client().QueryWorkflow(reader.Context(), queryIn(tc.readerNS, plain), grpc.WaitForReady(true))
			if tc.wantCount {
				requireLabel(t, reader, "test_encryption_vault_namespace_mismatches_total", "namespace", tc.readerNS)
			}

			if tc.wantErr {
				require.Error(t, err)
				requireLabel(t, reader, "test_encryption_vault_ops_total", "result", "mismatch")
				return
			}

			require.NoError(t, err)
			require.True(t, proto.Equal(secret, resp.GetQueryResult().GetPayloads()[0]))
		})
	}
}

// TestEndToEndChainedStrictHopPassesForeignPayloads runs client -> hop A
// (permissive) -> hop B (strict, lists binary/encrypted, lacks A's key) ->
// upstream, which answers with a legacy payload sealed unbound under A's key.
// B must pass it through as foreign rather than refuse it as unbound, and A
// must open it. Moving the strict check ahead of the KEK lookup in Vault.Open
// fails the call.
func TestEndToEndChainedStrictHopPassesForeignPayloads(t *testing.T) {
	t.Parallel()

	keyA := testingKeyURIOf(t, 0x0a)
	secret := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
		Data:     []byte(`"the-answer-is-42"`),
	}
	// Sealed unbound under A's key by an older writer, as legacy history would be.
	legacy := sealThrough(t, keyA, config.NamespaceBindingOff, secret)

	up := dataplanetest.NewUpstream(t)
	up.SetQueryResult(legacy)

	cfgB := dataplanetest.Config(up)
	cfgB.Encryption = config.Encryption{
		Enabled:          true,
		NamespaceBinding: config.NamespaceBindingStrict,
		SkipEncodings:    []string{wireEncryptedMarker},
		Default:          &config.KeyPolicy{URI: testingKeyURIOf(t, 0x0b), Duration: time.Hour},
	}
	hopB := dataplanetest.StartApp(t, cfgB)

	cfgA := dataplanetest.Config(up)
	cfgA.Upstreams[0].Listen = config.ListenConfig{HostPort: hopB.Addr(), Insecure: true}
	cfgA.Encryption = config.Encryption{
		Enabled:          true,
		NamespaceBinding: config.NamespaceBindingPermissive,
		Default:          &config.KeyPolicy{URI: keyA, Duration: time.Hour},
	}
	hopA := dataplanetest.StartApp(t, cfgA)

	resp, err := hopA.Client().QueryWorkflow(hopA.Context(), queryWith(secret), grpc.WaitForReady(true))
	require.NoError(t, err)
	require.True(t, proto.Equal(secret, resp.GetQueryResult().GetPayloads()[0]))

	requireLabel(t, hopB, "test_encryption_payloads_skipped_total", "operation", "decrypt")
}

// sealThrough runs secret through a proxy sealing under key with the given
// binding mode, and returns the payloads its upstream received.
func sealThrough(t *testing.T, key url.URL, mode config.NamespaceBinding, secret *common.Payload) *common.Payloads {
	t.Helper()

	return sealThroughIn(t, key, mode, "ns1", config.NamespaceRules{}, secret)
}

// sealThroughIn is sealThrough for a request in ns, on an upstream translating
// namespaces by rules.
func sealThroughIn(
	t *testing.T,
	key url.URL,
	mode config.NamespaceBinding,
	ns string,
	rules config.NamespaceRules,
	secret *common.Payload,
) *common.Payloads {
	t.Helper()

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.Upstreams[0].Namespaces.Rules = rules
	cfg.Encryption = config.Encryption{
		Enabled:          true,
		NamespaceBinding: mode,
		Default:          &config.KeyPolicy{URI: key, Duration: time.Hour},
	}
	f := dataplanetest.StartApp(t, cfg)

	_, err := f.Client().QueryWorkflow(f.Context(), queryIn(ns, secret), grpc.WaitForReady(true))
	require.NoError(t, err)

	reqs := up.Requests()
	require.Len(t, reqs, 1)

	return reqs[0].(*workflowservice.QueryWorkflowRequest).GetQuery().GetQueryArgs()
}
