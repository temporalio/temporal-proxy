package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

const (
	testKEKID  = "test-kek"
	sealPrefix = "sealed:"
)

// fakeVault is a reversible in-memory Vault. Seal prefixes the plaintext with
// sealPrefix (so sealed data is observably distinct from cleartext) and records
// the namespace it was called with; Open strips the prefix. Errors and a fixed
// Open result can be injected to exercise the interceptor's failure paths. It is
// safe for concurrent use because the payload visitor may seal/open payloads
// from multiple goroutines within a single call.
type fakeVault struct {
	mu         sync.Mutex
	namespaces []string // namespaces passed to Seal, in call order
	opens      int      // number of Open calls
	sealErr    error    // when set, Seal returns it
	openErr    error    // when set, Open returns it
	openReturn []byte   // when set, Open returns these bytes instead of the unsealed plaintext
}

func TestEncryptionInterceptorRoundtrip(t *testing.T) {
	t.Parallel()

	v := &fakeVault{}
	interceptor, err := EncryptionInterceptor(true, v)
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
	interceptor, err := EncryptionInterceptor(false, v)
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

func TestEncryptDecryptPayloadsRoundtrip(t *testing.T) {
	t.Parallel()

	v := &fakeVault{}
	vc := visitCtx(meta.WithNamespace(t.Context(), "ns1"))

	original := []*common.Payload{
		testPayload("json/plain", `"first"`),
		testPayload("json/plain", `"second"`),
	}
	want := []*common.Payload{
		proto.Clone(original[0]).(*common.Payload),
		proto.Clone(original[1]).(*common.Payload),
	}

	sealed, err := encryptPayloads(v)(vc, original)
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

	opened, err := decryptPayloads(v)(vc, sealed)
	require.NoError(t, err)
	require.Len(t, opened, len(want))

	for i := range want {
		require.True(t, proto.Equal(want[i], opened[i]), "payload %d did not round-trip", i)
	}
}

func TestDecryptPayloadsPassesThroughUnencrypted(t *testing.T) {
	t.Parallel()

	v := &fakeVault{}
	vc := visitCtx(meta.WithNamespace(t.Context(), "ns1"))

	orig := testPayload("json/plain", `"secret"`)
	sealed, err := encryptPayloads(v)(vc, []*common.Payload{orig})
	require.NoError(t, err)

	plain := testPayload("json/plain", `"visible"`)

	out, err := decryptPayloads(v)(vc, []*common.Payload{sealed[0], plain})
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
			_, err := encryptPayloads(v)(visitCtx(tc.ctx(t)), []*common.Payload{testPayload("json/plain", "x")})
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
		_, err := encryptPayloads(v)(visitCtx(t.Context()), []*common.Payload{testPayload("json/plain", "x")})
		require.ErrorContains(t, err, "failed to encrypt payload")
	})

	t.Run("open error", func(t *testing.T) {
		t.Parallel()

		v := &fakeVault{}
		vc := visitCtx(meta.WithNamespace(t.Context(), "ns1"))
		sealed, err := encryptPayloads(v)(vc, []*common.Payload{testPayload("json/plain", "x")})
		require.NoError(t, err)

		v.openErr = errors.New("unwrap failed")
		_, err = decryptPayloads(v)(vc, sealed)
		require.ErrorContains(t, err, "failed to decrypt payload")
	})

	t.Run("unmarshal error", func(t *testing.T) {
		t.Parallel()

		v := &fakeVault{openReturn: []byte{0xFF, 0xFF, 0xFF}}
		vc := visitCtx(meta.WithNamespace(t.Context(), "ns1"))
		sealed, err := encryptPayloads(v)(vc, []*common.Payload{testPayload("json/plain", "x")})
		require.NoError(t, err)

		_, err = decryptPayloads(v)(vc, sealed)
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
