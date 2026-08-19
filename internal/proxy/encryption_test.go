package proxy_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/codec"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

const (
	testKEKID  = "test-kek"
	sealPrefix = "sealed:"
)

// fakeVault is a reversible in-memory Vault. Seal prefixes the plaintext with
// sealPrefix (so sealed data is observably distinct from cleartext) and records
// the namespace it was called with; Open strips the prefix. Errors can be
// injected to exercise the interceptor's failure paths. It is safe for
// concurrent use because the payload visitor may seal/open payloads from
// multiple goroutines within a single call.
type fakeVault struct {
	mu         sync.Mutex
	namespaces []string // namespaces passed to Seal, in call order
	opens      int      // number of Open calls
	sealErr    error    // when set, Seal returns it
	openErr    error    // when set, Open returns it
}

func TestEncryptionRoundtrip(t *testing.T) {
	t.Parallel()

	v := &fakeVault{}
	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{Vault: v, Encrypt: true, Reporter: newTestReporter(t)})
	require.NoError(t, err)

	original := []*common.Payload{
		testPayload("json/plain", `"first"`),
		testPayload("json/plain", `"second"`),
	}
	want := []*common.Payload{
		proto.Clone(original[0]).(*common.Payload),
		proto.Clone(original[1]).(*common.Payload),
	}

	req := startRequest(original...)
	resp := &workflowservice.StartWorkflowExecutionRequest{}

	invoker := func(_ context.Context, _ string, gotReq, gotResp any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		// Outbound: every payload reached the upstream sealed. What the sealed form
		// looks like is pinned in pkg/codec; what matters here is that the upstream
		// sees ciphertext rather than the plaintext the caller handed in.
		sent := gotReq.(*workflowservice.StartWorkflowExecutionRequest).Input.Payloads
		require.Len(t, sent, len(original))

		for i, p := range sent {
			require.Equal(t, codec.EncryptionEncoding, string(p.Metadata[codec.MetadataEncoding]))
			require.True(t, bytes.HasPrefix(p.Data, []byte(sealPrefix)))
			require.NotEqual(t, want[i].Data, p.Data)
		}

		// Echo the sealed payloads back so the inbound path can open them.
		gotResp.(*workflowservice.StartWorkflowExecutionRequest).Input = &common.Payloads{Payloads: sent}
		return nil
	}

	ctx := meta.WithNamespace(t.Context(), "orders")
	require.NoError(t, interceptor(ctx, "/svc/Start", req, resp, nil, invoker))

	require.Len(t, resp.Input.Payloads, len(want))
	for i := range want {
		require.True(t, proto.Equal(want[i], resp.Input.Payloads[i]), "payload %d did not round-trip", i)
	}

	// One Seal per payload, each under the request's namespace.
	require.Equal(t, []string{"orders", "orders"}, v.namespaces)
}

func TestEncryptionDisabledSkipsOutbound(t *testing.T) {
	t.Parallel()

	v := &fakeVault{}
	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{Vault: v, Reporter: newTestReporter(t)})
	require.NoError(t, err)

	orig := testPayload("json/plain", `"hi"`)
	req := startRequest(testPayload("json/plain", `"hi"`))
	resp := &workflowservice.StartWorkflowExecutionRequest{}

	invoker := func(_ context.Context, _ string, gotReq, gotResp any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		// Outbound sealing is disabled: the request payload reaches the upstream
		// as cleartext with its original encoding.
		sent := gotReq.(*workflowservice.StartWorkflowExecutionRequest).Input.Payloads[0]
		require.Equal(t, "json/plain", string(sent.Metadata[codec.MetadataEncoding]))
		require.False(t, bytes.HasPrefix(sent.Data, []byte(sealPrefix)))

		gotResp.(*workflowservice.StartWorkflowExecutionRequest).Input = &common.Payloads{
			Payloads: []*common.Payload{sealedPayload(t, orig)},
		}
		return nil
	}

	ctx := meta.WithNamespace(t.Context(), "orders")
	require.NoError(t, interceptor(ctx, "/svc/Start", req, resp, nil, invoker))

	// Inbound decryption still runs even with sealing disabled.
	require.Len(t, resp.Input.Payloads, 1)
	require.True(t, proto.Equal(orig, resp.Input.Payloads[0]))
	require.Equal(t, 1, v.opens, "inbound payload should be opened once")
	require.Empty(t, v.namespaces, "interceptor must not seal when disabled")
}

func TestEncryptionNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ctx  func(t *testing.T) context.Context
		want string
	}{
		{
			name: "namespace from metadata",
			ctx:  func(t *testing.T) context.Context { return meta.WithNamespace(t.Context(), "orders") },
			want: "orders",
		},
		{
			name: "namespace appended to outgoing metadata",
			ctx: func(t *testing.T) context.Context {
				return metadata.AppendToOutgoingContext(t.Context(), meta.NamespaceHeader, "ns1")
			},
			want: "ns1",
		},
		{
			name: "absent namespace seals under empty string",
			ctx:  func(t *testing.T) context.Context { return t.Context() },
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := &fakeVault{}
			interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{Vault: v, Encrypt: true, Reporter: newTestReporter(t)})
			require.NoError(t, err)

			req := startRequest(testPayload("json/plain", "x"))
			resp := &workflowservice.StartWorkflowExecutionRequest{}
			require.NoError(t, interceptor(tc.ctx(t), "/svc/Start", req, resp, nil, respondWith()))

			require.Equal(t, []string{tc.want}, v.namespaces)
		})
	}
}

func TestEncryptionErrors(t *testing.T) {
	t.Parallel()

	t.Run("seal error aborts before the call", func(t *testing.T) {
		t.Parallel()

		v := &fakeVault{sealErr: errors.New("kms unavailable")}
		interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{Vault: v, Encrypt: true, Reporter: newTestReporter(t)})
		require.NoError(t, err)

		called := false
		invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			called = true
			return nil
		}

		req := startRequest(testPayload("json/plain", "x"))
		resp := &workflowservice.StartWorkflowExecutionRequest{}
		err = interceptor(t.Context(), "/svc/Start", req, resp, nil, invoker)

		require.ErrorContains(t, err, "failed to encrypt payload")
		require.False(t, called, "upstream must not be called when sealing fails")
	})

	// The remaining decode failures are pinned in pkg/codec. What is left to show
	// here is that one surfaces as the call's error rather than being swallowed,
	// leaving the caller to treat the response as unusable.
	t.Run("open error fails the call", func(t *testing.T) {
		t.Parallel()

		v := &fakeVault{openErr: errors.New("unwrap failed")}
		interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{Vault: v, Reporter: newTestReporter(t)})
		require.NoError(t, err)

		invoker := respondWith(sealedPayload(t, testPayload("json/plain", "x")))
		resp := &workflowservice.StartWorkflowExecutionRequest{}
		err = interceptor(t.Context(), "/svc/Start", startRequest(), resp, nil, invoker)

		require.ErrorContains(t, err, "failed to decrypt payload")
	})
}

func TestEncryptionRecordsVaultOps(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	reporter := proxy.NewReporter(metrics.New("proxy", promauto.With(reg)).ForSubsystem("encryption"))

	vault := &fakeVault{}
	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{Vault: vault, Encrypt: true, Reporter: reporter})
	require.NoError(t, err)

	ctx := metadata.AppendToOutgoingContext(t.Context(), meta.NamespaceHeader, "ns1")

	req := startRequest(&common.Payload{Data: []byte("hi")})
	resp := &workflowservice.StartWorkflowExecutionRequest{}

	// Echo the sealed request payload back so the inbound path opens it,
	// exercising the decrypt metric path alongside encrypt.
	invoker := func(_ context.Context, _ string, gotReq, gotResp any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		sent := gotReq.(*workflowservice.StartWorkflowExecutionRequest).Input.Payloads[0]
		gotResp.(*workflowservice.StartWorkflowExecutionRequest).Input = &common.Payloads{
			Payloads: []*common.Payload{sent},
		}
		return nil
	}
	require.NoError(t, interceptor(ctx, "/method", req, resp, nil, invoker))

	ops := gatherFamily(t, reg, "proxy_encryption_vault_ops_total")
	require.NotNil(t, ops)
	require.True(t, hasLabels(ops, map[string]string{"operation": "encrypt", "result": "success", "namespace": "ns1"}))
	require.True(t, hasLabels(ops, map[string]string{"operation": "decrypt", "result": "success", "namespace": "ns1"}))

	dur := gatherFamily(t, reg, "proxy_encryption_vault_ops_duration_secs")
	require.NotNil(t, dur)
	require.True(t, hasLabels(dur, map[string]string{"operation": "encrypt", "namespace": "ns1"}))
	require.True(t, hasLabels(dur, map[string]string{"operation": "decrypt", "namespace": "ns1"}))
}

func TestEncryptionSkipsMetricsForPassThrough(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	reporter := proxy.NewReporter(metrics.New("proxy", promauto.With(reg)).ForSubsystem("encryption"))

	vault := &fakeVault{}
	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{Vault: vault, Encrypt: true, Reporter: reporter})
	require.NoError(t, err)

	ctx := metadata.AppendToOutgoingContext(t.Context(), meta.NamespaceHeader, "ns1")

	req := startRequest(&common.Payload{Data: []byte("hi")})
	resp := &workflowservice.StartWorkflowExecutionRequest{}

	// Return an unencrypted response payload; the inbound path must pass it
	// through without opening it or recording a decrypt op.
	invoker := respondWith(testPayload("json/plain", `"plain"`))
	require.NoError(t, interceptor(ctx, "/method", req, resp, nil, invoker))

	require.Equal(t, 0, vault.opens)

	ops := gatherFamily(t, reg, "proxy_encryption_vault_ops_total")
	require.NotNil(t, ops)
	require.True(t, hasLabels(ops, map[string]string{"operation": "encrypt", "result": "success", "namespace": "ns1"}))
	require.False(t, hasLabels(ops, map[string]string{"operation": "decrypt", "result": "success", "namespace": "ns1"}))
}

func (f *fakeVault) Seal(_ context.Context, ns string, data []byte) (*crypto.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.sealErr != nil {
		return nil, f.sealErr
	}

	f.namespaces = append(f.namespaces, ns)
	return &crypto.Message{
		Ciphertext:  append([]byte(sealPrefix), data...),
		KeyMaterial: &crypto.DEKMaterial{KEKID: testKEKID, EncryptedDEK: "dek:" + ns},
	}, nil
}

func (f *fakeVault) Open(_ context.Context, msg *crypto.Message) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.opens++
	if f.openErr != nil {
		return nil, f.openErr
	}

	if !bytes.HasPrefix(msg.Ciphertext, []byte(sealPrefix)) {
		return nil, fmt.Errorf("ciphertext was not sealed by fakeVault")
	}

	return bytes.TrimPrefix(msg.Ciphertext, []byte(sealPrefix)), nil
}

func testPayload(encoding, data string) *common.Payload {
	return &common.Payload{
		Metadata: map[string][]byte{codec.MetadataEncoding: []byte(encoding)},
		Data:     []byte(data),
	}
}

// sealedPayload returns p as [fakeVault.Seal] would have sealed it, so a
// response payload can arrive already encrypted without the interceptor having
// sealed it on the way out.
func sealedPayload(t *testing.T, p *common.Payload) *common.Payload {
	t.Helper()

	data, err := p.Marshal()
	require.NoError(t, err)

	return &common.Payload{
		Metadata: map[string][]byte{
			codec.MetadataEncoding:        []byte(codec.EncryptionEncoding),
			codec.MetadataEncryptionKeyID: []byte(testKEKID),
			codec.MetadataEncryptionDEK:   []byte("dek:orders"),
		},
		Data: append([]byte(sealPrefix), data...),
	}
}

// startRequest builds the request the interceptor is handed, carrying payloads
// as its workflow input. StartWorkflowExecution is used throughout because its
// input is the payload field the visitor walks.
func startRequest(payloads ...*common.Payload) *workflowservice.StartWorkflowExecutionRequest {
	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "local"}
	if len(payloads) > 0 {
		req.Input = &common.Payloads{Payloads: payloads}
	}

	return req
}

// respondWith returns an invoker that ignores the request and hands back
// payloads as the response's workflow input.
func respondWith(payloads ...*common.Payload) grpc.UnaryInvoker {
	return func(_ context.Context, _ string, _, gotResp any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		if len(payloads) > 0 {
			gotResp.(*workflowservice.StartWorkflowExecutionRequest).Input = &common.Payloads{Payloads: payloads}
		}

		return nil
	}
}

// newTestReporter builds a Reporter backed by its own private registry, so
// call sites under test have a Reporter to record into without colliding with
// any other test's metrics.
func newTestReporter(t *testing.T) *proxy.Reporter {
	t.Helper()
	return proxy.NewReporter(metrics.New("test", promauto.With(prometheus.NewRegistry())).ForSubsystem("encryption"))
}

// gatherFamily returns the metric family named name from reg, or nil if no
// such family has been registered/observed.
func gatherFamily(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()

	mfs, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}

	return nil
}

// hasLabels reports whether mf contains a metric whose label set matches
// labels exactly.
func hasLabels(mf *dto.MetricFamily, labels map[string]string) bool {
	for _, m := range mf.GetMetric() {
		got := make(map[string]string, len(m.GetLabel()))
		for _, l := range m.GetLabel() {
			got[l.GetName()] = l.GetValue()
		}

		if len(got) != len(labels) {
			continue
		}

		match := true
		for k, v := range labels {
			if got[k] != v {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}

	return false
}
