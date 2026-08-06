package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

const (
	testKEKID  = "test-kek"
	sealPrefix = "sealed:"
)

type (
	// fakeVault is a reversible in-memory Vault. Seal prefixes the plaintext with
	// sealPrefix (so sealed data is observably distinct from cleartext) and records
	// the namespace it was called with; Open strips the prefix. Errors and a fixed
	// Open result can be injected to exercise the interceptor's failure paths. It is
	// safe for concurrent use because the payload visitor may seal/open payloads
	// from multiple goroutines within a single call.
	fakeVault struct {
		mu         sync.Mutex
		namespaces []string // namespaces passed to Seal, in call order
		opens      int      // number of Open calls
		sealErr    error    // when set, Seal returns it
		openErr    error    // when set, Open returns it
		openReturn []byte   // when set, Open returns these bytes instead of the unsealed plaintext
	}

	// recordingStreamer is a grpc.Streamer whose stream records sent messages and
	// replays the most recent one on Recv, so a test can inspect what went on the
	// wire and what comes back.
	recordingStreamer struct {
		sent []any
	}

	// recordingStream is the grpc.ClientStream recordingStreamer hands back; it
	// clones every SendMsg argument at the moment it is called (rather than
	// keeping the live pointer) and replays the most recent one into RecvMsg,
	// so what a test inspects is exactly what "went on the wire" at send time.
	recordingStream struct {
		grpc.ClientStream

		parent *recordingStreamer
		ctx    context.Context
	}
)

func TestEncryptionInterceptorRoundtrip(t *testing.T) {
	t.Parallel()

	v := &fakeVault{}
	interceptor, err := EncryptionInterceptor(true, v, newTestReporter(t))
	require.NoError(t, err)

	input := &common.Payloads{Payloads: []*common.Payload{testPayload("json/plain", `"hi"`)}}
	want := proto.Clone(input.Payloads[0]).(*common.Payload)

	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "local", Input: input}
	resp := &workflowservice.StartWorkflowExecutionRequest{}

	invoker := func(_ context.Context, _ string, gotReq, gotResp any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		// Outbound: the request payload reached the upstream sealed.
		sent := gotReq.(*workflowservice.StartWorkflowExecutionRequest).Input.Payloads[0]
		require.Equal(t, encryptionEncoding, string(sent.Metadata[metadataEncoding]))
		require.True(t, bytes.HasPrefix(sent.Data, []byte(sealPrefix)))

		// Echo the sealed payload back so the inbound path can open it.
		gotResp.(*workflowservice.StartWorkflowExecutionRequest).Input = &common.Payloads{
			Payloads: []*common.Payload{sent},
		}
		return nil
	}

	ctx := meta.WithNamespace(t.Context(), "orders")
	require.NoError(t, interceptor(ctx, "/svc/Start", req, resp, nil, invoker))

	require.Len(t, resp.Input.Payloads, 1)
	require.True(t, proto.Equal(want, resp.Input.Payloads[0]))
	require.Equal(t, []string{"orders"}, v.namespaces)
}

func TestEncryptionInterceptorDisabledSkipsOutbound(t *testing.T) {
	t.Parallel()

	v := &fakeVault{}
	interceptor, err := EncryptionInterceptor(false, v, newTestReporter(t))
	require.NoError(t, err)

	// A response payload sealed exactly as fakeVault.Seal would produce it, so
	// the inbound path can open it without the interceptor ever sealing.
	orig := testPayload("json/plain", `"hi"`)
	sealed := &common.Payload{
		Metadata: map[string][]byte{
			metadataEncoding:        []byte(encryptionEncoding),
			metadataEncryptionKeyID: []byte(testKEKID),
			metadataEncryptionDEK:   []byte("dek:orders"),
		},
		Data: append([]byte(sealPrefix), mustMarshal(t, orig)...),
	}

	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: "local",
		Input:     &common.Payloads{Payloads: []*common.Payload{testPayload("json/plain", `"hi"`)}},
	}
	resp := &workflowservice.StartWorkflowExecutionRequest{}

	invoker := func(_ context.Context, _ string, gotReq, gotResp any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		// Outbound sealing is disabled: the request payload reaches the upstream
		// as cleartext with its original encoding.
		sent := gotReq.(*workflowservice.StartWorkflowExecutionRequest).Input.Payloads[0]
		require.Equal(t, "json/plain", string(sent.Metadata[metadataEncoding]))
		require.False(t, bytes.HasPrefix(sent.Data, []byte(sealPrefix)))

		gotResp.(*workflowservice.StartWorkflowExecutionRequest).Input = &common.Payloads{
			Payloads: []*common.Payload{sealed},
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

func TestEncryptionInterceptorRecordsVaultOps(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	reporter := NewReporter(metrics.New("proxy", promauto.With(reg)).ForSubsystem("encryption"))

	vault := &fakeVault{}
	interceptor, err := EncryptionInterceptor(true, vault, reporter)
	require.NoError(t, err)

	ctx := metadata.AppendToOutgoingContext(t.Context(), meta.NamespaceHeader, "ns1")

	req := &workflowservice.StartWorkflowExecutionRequest{
		Input: &common.Payloads{Payloads: []*common.Payload{{Data: []byte("hi")}}},
	}
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

func TestEncryptionInterceptorSkipsMetricsForPassThrough(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	reporter := NewReporter(metrics.New("proxy", promauto.With(reg)).ForSubsystem("encryption"))

	vault := &fakeVault{}
	interceptor, err := EncryptionInterceptor(true, vault, reporter)
	require.NoError(t, err)

	ctx := metadata.AppendToOutgoingContext(t.Context(), meta.NamespaceHeader, "ns1")

	req := &workflowservice.StartWorkflowExecutionRequest{
		Input: &common.Payloads{Payloads: []*common.Payload{{Data: []byte("hi")}}},
	}
	resp := &workflowservice.StartWorkflowExecutionRequest{}

	// Return an unencrypted response payload; the inbound path must pass it
	// through without opening it or recording a decrypt op.
	invoker := func(_ context.Context, _ string, _, gotResp any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotResp.(*workflowservice.StartWorkflowExecutionRequest).Input = &common.Payloads{
			Payloads: []*common.Payload{testPayload("json/plain", `"plain"`)},
		}
		return nil
	}
	require.NoError(t, interceptor(ctx, "/method", req, resp, nil, invoker))

	require.Equal(t, 0, vault.opens)

	ops := gatherFamily(t, reg, "proxy_encryption_vault_ops_total")
	require.NotNil(t, ops)
	require.True(t, hasLabels(ops, map[string]string{"operation": "encrypt", "result": "success", "namespace": "ns1"}))
	require.False(t, hasLabels(ops, map[string]string{"operation": "decrypt", "result": "success", "namespace": "ns1"}))
}

func TestEncryptionStreamInterceptorSealsAndOpensFrames(t *testing.T) {
	t.Parallel()

	vault := &fakeVault{}
	rep := newTestReporter(t)

	interceptor := EncryptionStreamInterceptor(true, vault, rep)

	// The fake streamer stands in for the upstream: it records what was sent
	// (which must be ciphertext) and replays it back (which must come back as
	// the original plaintext).
	fake := &recordingStreamer{}
	cs, err := interceptor(
		metadata.NewOutgoingContext(t.Context(), metadata.Pairs(meta.NamespaceHeader, "ns-1")),
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		nil,
		"/test.Service/Method",
		fake.stream,
	)
	require.NoError(t, err)

	sent := &common.Payloads{Payloads: []*common.Payload{{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     []byte(`"hello"`),
	}}}
	require.NoError(t, cs.SendMsg(sent))

	onWire := fake.sent[0].(*common.Payloads).GetPayloads()[0]
	require.Equal(t, "binary/encrypted", string(onWire.GetMetadata()["encoding"]),
		"a streamed payload must be sealed like a unary one")
	require.NotEqual(t, []byte(`"hello"`), onWire.GetData())
	require.True(t, bytes.HasPrefix(onWire.GetData(), []byte(sealPrefix)),
		"the payload sent upstream must actually be ciphertext, not merely different bytes")

	got := &common.Payloads{}
	require.NoError(t, cs.RecvMsg(got))
	require.Equal(t, []byte(`"hello"`), got.GetPayloads()[0].GetData(),
		"the sealed frame must open again on the way back")
}

func TestEncryptionStreamInterceptorDisabledStillOpens(t *testing.T) {
	t.Parallel()

	vault := &fakeVault{}
	interceptor := EncryptionStreamInterceptor(false, vault, newTestReporter(t))

	fake := &recordingStreamer{}
	cs, err := interceptor(t.Context(), &grpc.StreamDesc{}, nil, "/test.Service/Method", fake.stream)
	require.NoError(t, err)

	sent := &common.Payloads{Payloads: []*common.Payload{{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     []byte(`"hello"`),
	}}}
	require.NoError(t, cs.SendMsg(sent))

	onWire := fake.sent[0].(*common.Payloads).GetPayloads()[0]
	require.Equal(t, []byte(`"hello"`), onWire.GetData(),
		"sealing is gated, so a disabled interceptor forwards cleartext")

	// Simulate a frame that was sealed earlier, while encryption was still
	// enabled for that data, arriving from upstream now that sealing is
	// disabled for new traffic. Opening must still run regardless.
	sealed, err := encryptPayloads(vault, newTestReporter(t))(
		visitCtx(meta.WithNamespace(t.Context(), "ns-1")),
		[]*common.Payload{testPayload("json/plain", `"already-sealed"`)},
	)
	require.NoError(t, err)
	fake.sent = append(fake.sent, &common.Payloads{Payloads: sealed})

	got := &common.Payloads{}
	require.NoError(t, cs.RecvMsg(got))
	require.Equal(t, []byte(`"already-sealed"`), got.GetPayloads()[0].GetData(),
		"opening must still run even though sealing is disabled for new traffic")
}

func TestEncryptDecryptPayloadsRoundtrip(t *testing.T) {
	t.Parallel()

	v := &fakeVault{}
	r := newTestReporter(t)
	vc := visitCtx(meta.WithNamespace(t.Context(), "ns1"))

	original := []*common.Payload{
		testPayload("json/plain", `"first"`),
		testPayload("json/plain", `"second"`),
	}
	want := []*common.Payload{
		proto.Clone(original[0]).(*common.Payload),
		proto.Clone(original[1]).(*common.Payload),
	}

	sealed, err := encryptPayloads(v, r)(vc, original)
	require.NoError(t, err)
	require.Len(t, sealed, len(original))

	for _, p := range sealed {
		require.Equal(t, encryptionEncoding, string(p.Metadata[metadataEncoding]))
		require.Equal(t, testKEKID, string(p.Metadata[metadataEncryptionKeyID]))
		require.NotEmpty(t, p.Metadata[metadataEncryptionDEK])
		// The data on the wire is ciphertext, never the marshaled plaintext.
		require.True(t, bytes.HasPrefix(p.Data, []byte(sealPrefix)))
	}

	require.Equal(t, []string{"ns1", "ns1"}, v.namespaces)

	opened, err := decryptPayloads(v, r)(vc, sealed)
	require.NoError(t, err)
	require.Len(t, opened, len(want))

	for i := range want {
		require.True(t, proto.Equal(want[i], opened[i]), "payload %d did not round-trip", i)
	}
}

func TestDecryptPayloadsPassesThroughUnencrypted(t *testing.T) {
	t.Parallel()

	v := &fakeVault{}
	r := newTestReporter(t)
	vc := visitCtx(meta.WithNamespace(t.Context(), "ns1"))

	orig := testPayload("json/plain", `"secret"`)
	sealed, err := encryptPayloads(v, r)(vc, []*common.Payload{orig})
	require.NoError(t, err)

	plain := testPayload("json/plain", `"visible"`)

	out, err := decryptPayloads(v, r)(vc, []*common.Payload{sealed[0], plain})
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.True(t, proto.Equal(orig, out[0]))
	require.Same(t, plain, out[1], "unencrypted payload should pass through by reference")
	require.Equal(t, 1, v.opens, "Open must be called only for the sealed payload")
}

func TestEncryptPayloadsNamespace(t *testing.T) {
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
			name: "absent namespace seals under empty string",
			ctx:  func(t *testing.T) context.Context { return t.Context() },
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := &fakeVault{}
			_, err := encryptPayloads(v, newTestReporter(t))(visitCtx(tc.ctx(t)), []*common.Payload{testPayload("json/plain", "x")})
			require.NoError(t, err)
			require.Equal(t, []string{tc.want}, v.namespaces)
		})
	}
}

func TestEncryptDecryptPayloadsErrors(t *testing.T) {
	t.Parallel()

	t.Run("seal error", func(t *testing.T) {
		t.Parallel()

		v := &fakeVault{sealErr: errors.New("kms unavailable")}
		_, err := encryptPayloads(v, newTestReporter(t))(visitCtx(t.Context()), []*common.Payload{testPayload("json/plain", "x")})
		require.ErrorContains(t, err, "failed to encrypt payload")
	})

	t.Run("open error", func(t *testing.T) {
		t.Parallel()

		v := &fakeVault{}
		r := newTestReporter(t)
		vc := visitCtx(meta.WithNamespace(t.Context(), "ns1"))
		sealed, err := encryptPayloads(v, r)(vc, []*common.Payload{testPayload("json/plain", "x")})
		require.NoError(t, err)

		v.openErr = errors.New("unwrap failed")
		_, err = decryptPayloads(v, r)(vc, sealed)
		require.ErrorContains(t, err, "failed to decrypt payload")
	})

	t.Run("unmarshal error", func(t *testing.T) {
		t.Parallel()

		v := &fakeVault{openReturn: []byte{0xFF, 0xFF, 0xFF}}
		r := newTestReporter(t)
		vc := visitCtx(meta.WithNamespace(t.Context(), "ns1"))
		sealed, err := encryptPayloads(v, r)(vc, []*common.Payload{testPayload("json/plain", "x")})
		require.NoError(t, err)

		_, err = decryptPayloads(v, r)(vc, sealed)
		require.ErrorContains(t, err, "failed to unmarshal payload")
	})
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
	if f.openReturn != nil {
		return f.openReturn, nil
	}

	if !bytes.HasPrefix(msg.Ciphertext, []byte(sealPrefix)) {
		return nil, fmt.Errorf("ciphertext was not sealed by fakeVault")
	}

	return bytes.TrimPrefix(msg.Ciphertext, []byte(sealPrefix)), nil
}

func (r *recordingStreamer) stream(
	ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return &recordingStream{parent: r, ctx: ctx}, nil
}

func (s *recordingStream) Context() context.Context { return s.ctx }

func (s *recordingStream) SendMsg(m any) error {
	// Record a clone taken at this exact call, not the live pointer: a real
	// gRPC transport marshals the message to bytes at send time, so what went
	// "on the wire" is whatever m held the instant SendMsg was invoked here.
	// Recording the pointer instead would let a caller who seals m only
	// after delegating to SendMsg still show sealed data if inspected later,
	// since the caller's own in-place mutation would apply to the same
	// object this fake is holding.
	s.parent.sent = append(s.parent.sent, proto.Clone(m.(proto.Message)))
	return nil
}

func (s *recordingStream) RecvMsg(m any) error {
	// Replay the last sent message into m, which is how a frame sealed on the
	// way out gets opened on the way back.
	last, ok := s.parent.sent[len(s.parent.sent)-1].(proto.Message)
	if !ok {
		return io.EOF
	}

	proto.Reset(m.(proto.Message))
	proto.Merge(m.(proto.Message), last)
	return nil
}

func testPayload(encoding, data string) *common.Payload {
	return &common.Payload{
		Metadata: map[string][]byte{metadataEncoding: []byte(encoding)},
		Data:     []byte(data),
	}
}

func mustMarshal(t *testing.T, p *common.Payload) []byte {
	t.Helper()

	data, err := p.Marshal()
	require.NoError(t, err)
	return data
}

func visitCtx(ctx context.Context) *proxy.VisitPayloadsContext {
	return &proxy.VisitPayloadsContext{Context: ctx}
}

// newTestReporter builds a Reporter backed by its own private registry, so
// call sites under test have a Reporter to record into without colliding with
// any other test's metrics.
func newTestReporter(t *testing.T) *Reporter {
	t.Helper()
	return NewReporter(metrics.New("test", promauto.With(prometheus.NewRegistry())).ForSubsystem("encryption"))
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
