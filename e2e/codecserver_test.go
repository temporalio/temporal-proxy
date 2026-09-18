package e2e

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

// namespaceHeader mirrors internal/codecserver/handler.go's own header name.
// Duplicated here for the same reason as the wire constants in
// e2e/encryption_test.go: this test verifies the observable HTTP contract.
const namespaceHeader = "X-Namespace"

// TestCodecServerDecodesWhatTheGatewaySealed proves the codec server applies
// the same chain the request path does: a payload sealed on its way through
// the gateway opens through the HTTP route, with no shared state between the
// two beyond the configured keys.
func TestCodecServerDecodesWhatTheGatewaySealed(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.Encryption = config.Encryption{
		Enabled: true,
		Default: &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
	}
	cfg.CodecServer = config.CodecServer{
		Enabled: true,
		Listen:  config.ListenConfig{HostPort: "127.0.0.1:0", Insecure: true},
	}

	f := dataplanetest.StartApp(t, cfg)

	secret := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
		Data:     []byte(`"top secret"`),
	}

	// Drive a call through the gateway so the upstream records the sealed form.
	_, err := f.Client().QueryWorkflow(f.Context(), &workflowservice.QueryWorkflowRequest{
		Namespace: "ns1",
		Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
		Query: &query.WorkflowQuery{
			QueryType: "state",
			QueryArgs: &common.Payloads{Payloads: []*common.Payload{secret}},
		},
	}, grpc.WaitForReady(true))
	require.NoError(t, err)

	reqs := up.Requests()
	require.Len(t, reqs, 1)

	got, ok := reqs[0].(*workflowservice.QueryWorkflowRequest)
	require.True(t, ok)

	sealed := got.GetQuery().GetQueryArgs().GetPayloads()
	require.Len(t, sealed, 1)
	require.Equal(t, wireEncryptedMarker, string(sealed[0].GetMetadata()[wireEncoding]),
		"the upstream must have seen ciphertext, or this test proves nothing")

	// Now open it through the codec server, the way the Cloud UI would.
	opened := decodeThroughCodecServer(t, f, sealed, "ns1")
	require.Len(t, opened, 1)
	require.True(t, proto.Equal(secret, opened[0]),
		"the codec server must return the plaintext the client originally sent")
}

// TestCodecServerEncodeOpensThroughTheGateway proves the reverse direction:
// a payload sealed through the codec server's /encode route, the same call a
// Cloud UI makes before writing input a Worker never sees in plaintext, opens
// correctly when the gateway's own inbound codec later reads it back. Unlike
// /decode, /encode needs a Namespace to pick the right key policy.
func TestCodecServerEncodeOpensThroughTheGateway(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.Encryption = config.Encryption{
		Enabled: true,
		Default: &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
	}
	cfg.CodecServer = config.CodecServer{
		Enabled: true,
		Listen:  config.ListenConfig{HostPort: "127.0.0.1:0", Insecure: true},
	}

	f := dataplanetest.StartApp(t, cfg)

	secret := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
		Data:     []byte(`"sealed by the codec server"`),
	}

	sealed := encodeThroughCodecServer(t, f, []*common.Payload{secret}, "ns1")
	require.Equal(t, wireEncryptedMarker, string(sealed[0].GetMetadata()[wireEncoding]),
		"the codec server must have sealed the payload, or this test proves nothing")

	// Stand in for the upstream already holding that ciphertext, e.g. because
	// some earlier call wrote it, and returning it on a call whose own request
	// carries nothing: only the gateway's inbound codec should touch it.
	up.SetQueryResult(&common.Payloads{Payloads: sealed})

	resp, err := f.Client().QueryWorkflow(f.Context(), &workflowservice.QueryWorkflowRequest{
		Namespace: "ns1",
		Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
		Query:     &query.WorkflowQuery{QueryType: "state"},
	}, grpc.WaitForReady(true))
	require.NoError(t, err)

	got := resp.GetQueryResult().GetPayloads()
	require.Len(t, got, 1)
	require.True(t, proto.Equal(secret, got[0]),
		"the gateway must open what the codec server's /encode route sealed")
}

// TestCodecServerDecodesTheRemoteNamespaceCloudSendsAfterTranslation proves
// the reason [codecserver.OverrideMap] exists: a Cloud UI names the namespace
// it sees on the wire, which is the remote (translated) name, not the local
// one an operator's encryption.overrides key is written under. The codec
// server must resolve the remote name back to the local one before it ever
// reaches the vault, so a decrypt attributed to a namespace does not carry the
// remote spelling.
func TestCodecServerDecodesTheRemoteNamespaceCloudSendsAfterTranslation(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.Upstreams[0].Namespaces.Rules.Suffix = ".a8x72"
	cfg.Metrics.Labels.Namespace = true
	cfg.Encryption = config.Encryption{
		Enabled: true,
		Default: &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
		Overrides: map[string]config.KeyPolicy{
			"ns1": {URI: secondaryKeyURI(t), Duration: time.Hour},
		},
	}
	cfg.CodecServer = config.CodecServer{
		Enabled: true,
		Listen:  config.ListenConfig{HostPort: "127.0.0.1:0", Insecure: true},
	}

	f := dataplanetest.StartApp(t, cfg)

	secret := &common.Payload{
		Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
		Data:     []byte(`"remote name path"`),
	}

	_, err := f.Client().QueryWorkflow(f.Context(), &workflowservice.QueryWorkflowRequest{
		Namespace: "ns1",
		Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
		Query: &query.WorkflowQuery{
			QueryType: "state",
			QueryArgs: &common.Payloads{Payloads: []*common.Payload{secret}},
		},
	}, grpc.WaitForReady(true))
	require.NoError(t, err)

	reqs := up.Requests()
	require.Len(t, reqs, 1)

	got, ok := reqs[0].(*workflowservice.QueryWorkflowRequest)
	require.True(t, ok)

	sealed := got.GetQuery().GetQueryArgs().GetPayloads()
	require.Len(t, sealed, 1)
	require.Equal(t, wireEncryptedMarker, string(sealed[0].GetMetadata()[wireEncoding]))

	// Send the remote name, the one the upstream (and so the Cloud UI) knows
	// "ns1" by, not the local name the override map and encryption.overrides
	// key are written under.
	opened := decodeThroughCodecServer(t, f, sealed, "ns1.a8x72")
	require.Len(t, opened, 1)
	require.True(t, proto.Equal(secret, opened[0]), "decode must still open the payload")

	// The decisive check: the vault must never see the remote spelling. If the
	// override map did nothing, the decrypt this /decode call performed would
	// be labeled with the raw header value instead.
	requireNoLabelValue(t, f, "test_encryption_vault_ops_total", "namespace", "ns1.a8x72")
}

// TestCodecServerRequiresConfiguredAuth proves the fx wiring in
// internal/codecserver/fx.go actually hands the configured authenticator to
// the handler: this surface holds KMS unwrap permission, so a build that
// silently dropped auth would leave every payload readable by anyone who could
// reach the port. A loopback bind is used because validation permits auth on
// loopback (it is only required beyond it), which lets the case exercise auth
// without also standing up TLS.
func TestCodecServerRequiresConfiguredAuth(t *testing.T) {
	t.Parallel()

	const token = "s3cret"

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.CodecServer = config.CodecServer{
		Enabled: true,
		Listen:  config.ListenConfig{HostPort: "127.0.0.1:0", Insecure: true},
		Auth:    &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: token}},
	}

	f := dataplanetest.StartApp(t, cfg)

	t.Run("no Authorization header is rejected", func(t *testing.T) {
		t.Parallel()

		res := postCodecServer(t, f, "/decode", `{"payloads":[]}`, nil)
		require.Equal(t, http.StatusUnauthorized, res.StatusCode)
	})

	t.Run("the configured token is accepted", func(t *testing.T) {
		t.Parallel()

		res := postCodecServer(t, f, "/decode", `{"payloads":[]}`, map[string]string{
			"Authorization": "Bearer " + token,
		})
		require.Equal(t, http.StatusOK, res.StatusCode)
	})
}

// TestCodecServerEncodeRequiresNamespaceWhenOverridesConfigured proves the fx
// wiring passes WithNamespaceRequired(true) whenever a per-namespace key
// policy exists: sealing a payload that cannot be attributed to a namespace
// would silently use the default policy rather than the one an operator wrote
// for it, so /encode with no namespace named must be rejected rather than
// falling back.
func TestCodecServerEncodeRequiresNamespaceWhenOverridesConfigured(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.Encryption = config.Encryption{
		Overrides: map[string]config.KeyPolicy{
			"ns1": {URI: testingKeyURI(t), Duration: time.Hour},
		},
	}
	cfg.CodecServer = config.CodecServer{
		Enabled: true,
		Listen:  config.ListenConfig{HostPort: "127.0.0.1:0", Insecure: true},
	}

	f := dataplanetest.StartApp(t, cfg)

	res := postCodecServer(t, f, "/encode", `{"payloads":[]}`, nil)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Contains(t, string(raw), "a namespace is required")
}

// postCodecServer posts body to route on the fixture's codec server with the
// given extra headers (Content-Type is always set), and returns the response.
// Its body is closed on test cleanup, so the caller need not close it.
func postCodecServer(
	t *testing.T, f *dataplanetest.Fixture, route, body string, headers map[string]string,
) *http.Response {
	t.Helper()

	addr := f.CodecServerAddr()
	require.NotEmpty(t, addr, "the fixture must have started a codec server")

	req, err := http.NewRequestWithContext(f.Context(), http.MethodPost, "http://"+addr+route, bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = res.Body.Close() })

	return res
}

// decodeThroughCodecServer posts payloads to the running fixture's /decode
// route with ns as the X-Namespace header, and returns the opened payloads.
func decodeThroughCodecServer(
	t *testing.T, f *dataplanetest.Fixture, payloads []*common.Payload, ns string,
) []*common.Payload {
	t.Helper()

	return callCodecServerRoute(t, f, "/decode", payloads, ns)
}

// encodeThroughCodecServer posts payloads to the running fixture's /encode
// route with ns as the X-Namespace header, and returns the sealed payloads.
func encodeThroughCodecServer(
	t *testing.T, f *dataplanetest.Fixture, payloads []*common.Payload, ns string,
) []*common.Payload {
	t.Helper()

	return callCodecServerRoute(t, f, "/encode", payloads, ns)
}

// callCodecServerRoute drives one request against the fixture's codec server
// and returns the payloads its response carries.
func callCodecServerRoute(
	t *testing.T, f *dataplanetest.Fixture, route string, payloads []*common.Payload, ns string,
) []*common.Payload {
	t.Helper()

	body, err := protojson.Marshal(&common.Payloads{Payloads: payloads})
	require.NoError(t, err)

	addr := f.CodecServerAddr()
	require.NotEmpty(t, addr, "the fixture must have started a codec server")

	req, err := http.NewRequestWithContext(f.Context(), http.MethodPost, "http://"+addr+route, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(namespaceHeader, ns)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()

	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode, "response body: %s", raw)

	var out common.Payloads
	require.NoError(t, protojson.Unmarshal(raw, &out))

	return out.Payloads
}

// secondaryKeyURI builds a second local testing:// key, distinct from
// [testingKeyURI], so a namespace override policy is provably its own rather
// than a coincidental match with the default.
func secondaryKeyURI(t *testing.T) url.URL {
	t.Helper()

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x5c}, 32))
	u, err := url.Parse("testing://" + key)
	require.NoError(t, err)

	return *u
}

// requireNoLabelValue asserts that no series in the named metric family
// carries label=value.
func requireNoLabelValue(t *testing.T, f *dataplanetest.Fixture, family, label, value string) {
	t.Helper()

	mfs, err := f.Gatherer().Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}

		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				require.False(t, lp.GetName() == label && lp.GetValue() == value,
					"family %s unexpectedly carries %s=%q", family, label, value)
			}
		}

		return
	}

	t.Fatalf("no metric family named %s was gathered", family)
}
