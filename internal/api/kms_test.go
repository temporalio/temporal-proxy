package api_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/api"
	kmsv1 "github.com/temporalio/temporal-proxy/pkg/api/kms/v1"
)

// fakeConn is a grpc.ClientConnInterface that records the last unary request and
// returns a canned response, letting us assert what the KMS KEK puts on the wire
// without a real server.
type fakeConn struct {
	gotReq any
	reply  any
	invoke error
}

func TestKMSEncryptSendsNamespace(t *testing.T) {
	t.Parallel()

	conn := &fakeConn{reply: &kmsv1.EncryptResponse{Ciphertext: []byte("wrapped")}}
	k := api.NewKMS("kms-1", conn)

	ct, err := k.Encrypt(t.Context(), "ns1", []byte("dek"))
	require.NoError(t, err)
	require.Equal(t, []byte("wrapped"), ct)

	req, ok := conn.gotReq.(*kmsv1.EncryptRequest)
	require.True(t, ok, "expected an *EncryptRequest, got %T", conn.gotReq)
	require.Equal(t, "ns1", req.GetNamespace())
	require.Equal(t, []byte("dek"), req.GetPlaintext())
}

func TestKMSDecryptSendsCiphertext(t *testing.T) {
	t.Parallel()

	conn := &fakeConn{reply: &kmsv1.DecryptResponse{Plaintext: []byte("dek")}}
	k := api.NewKMS("kms-1", conn)

	pt, err := k.Decrypt(t.Context(), []byte("wrapped"))
	require.NoError(t, err)
	require.Equal(t, []byte("dek"), pt)

	req, ok := conn.gotReq.(*kmsv1.DecryptRequest)
	require.True(t, ok, "expected a *DecryptRequest, got %T", conn.gotReq)
	require.Equal(t, []byte("wrapped"), req.GetCiphertext())
}

func (f *fakeConn) Invoke(_ context.Context, _ string, args, reply any, _ ...grpc.CallOption) error {
	f.gotReq = args
	if f.invoke != nil {
		return f.invoke
	}

	// Copy the canned reply's payload into the caller-supplied out message. Set
	// fields individually: proto messages embed a mutex, so copying by value
	// trips govet's copylocks check.
	switch out := reply.(type) {
	case *kmsv1.EncryptResponse:
		if r, ok := f.reply.(*kmsv1.EncryptResponse); ok {
			out.Ciphertext = r.Ciphertext
		}
	case *kmsv1.DecryptResponse:
		if r, ok := f.reply.(*kmsv1.DecryptResponse); ok {
			out.Plaintext = r.Plaintext
		}
	}

	return nil
}

func (f *fakeConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("streaming not supported")
}
