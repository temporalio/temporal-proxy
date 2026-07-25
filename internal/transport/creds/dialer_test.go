package creds_test

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

func TestDialer_Mode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opts []creds.Option
		want creds.Mode
	}{
		{
			name: "explicit insecure",
			opts: []creds.Option{creds.Insecure()},
			want: creds.ModeInsecure,
		},
		{
			name: "no material verifies against system roots",
			opts: nil,
			want: creds.ModeSystemTLS,
		},
		{
			name: "CA only verifies against a private anchor",
			opts: []creds.Option{creds.WithCA("ca.pem")},
			want: creds.ModeCustomCA,
		},
		{
			name: "client certificate is mutual TLS",
			opts: []creds.Option{creds.WithCA("ca.pem"), creds.WithCertificate("cert.pem", "key.pem")},
			want: creds.ModeMutualTLS,
		},
		{
			name: "client certificate without CA still resolves as mutual TLS",
			opts: []creds.Option{creds.WithCertificate("cert.pem", "key.pem")},
			want: creds.ModeMutualTLS,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, creds.NewDialer(tc.opts...).Mode())
		})
	}
}

func TestDialer_Validate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		opts    []creds.Option
		wantErr string
	}{
		{
			name:    "certificate without key",
			opts:    []creds.Option{creds.WithCA("ca.pem"), creds.WithCertificate("cert.pem", "")},
			wantErr: "certificate and key must be set together",
		},
		{
			name:    "key without certificate",
			opts:    []creds.Option{creds.WithCA("ca.pem"), creds.WithCertificate("", "key.pem")},
			wantErr: "certificate and key must be set together",
		},
		{
			name:    "client certificate requires a CA",
			opts:    []creds.Option{creds.WithCertificate("cert.pem", "key.pem")},
			wantErr: "certificate authority is required when a client certificate is set",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorContains(t, creds.NewDialer(tc.opts...).Validate(), tc.wantErr)
		})
	}

	t.Run("insecure", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, creds.NewDialer(creds.Insecure()).Validate())
	})
}

func TestDialer_Validate_Files(t *testing.T) {
	t.Parallel()

	t.Run("mutual TLS with valid material", func(t *testing.T) {
		t.Parallel()
		ca, cert, key := testutil.GenerateMTLSCerts(t)
		err := creds.NewDialer(creds.WithCA(ca), creds.WithCertificate(cert, key)).Validate()
		require.NoError(t, err)
	})

	t.Run("system roots needs no files", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, creds.NewDialer().Validate())
	})

	t.Run("custom CA accepts a pinned non-CA anchor", func(t *testing.T) {
		t.Parallel()
		// The leaf cert is not a CA. A client trust anchor may be a pinned
		// self-signed leaf, so custom-CA validation must NOT require IsCA.
		_, leaf, _ := testutil.GenerateMTLSCerts(t)
		require.NoError(t, creds.NewDialer(creds.WithCA(leaf)).Validate())
	})

	t.Run("mutual TLS rejects a non-CA used as the CA", func(t *testing.T) {
		t.Parallel()
		// Same leaf, but in mutual TLS the CA must actually be a CA.
		_, leaf, key := testutil.GenerateMTLSCerts(t)
		err := creds.NewDialer(creds.WithCA(leaf), creds.WithCertificate(leaf, key)).Validate()
		require.ErrorContains(t, err, "not a CA")
	})
}

func TestDialer_DialOption(t *testing.T) {
	t.Parallel()

	t.Run("insecure", func(t *testing.T) {
		t.Parallel()
		opt, err := creds.NewDialer(creds.Insecure()).DialOption("")
		require.NoError(t, err)
		require.NotNil(t, opt)
	})

	t.Run("system roots", func(t *testing.T) {
		t.Parallel()
		opt, err := creds.NewDialer().DialOption("upstream.example.com")
		require.NoError(t, err)
		require.NotNil(t, opt)
	})

	t.Run("custom CA", func(t *testing.T) {
		t.Parallel()
		ca, _, _ := testutil.GenerateMTLSCerts(t)
		opt, err := creds.NewDialer(creds.WithCA(ca)).DialOption("upstream.example.com")
		require.NoError(t, err)
		require.NotNil(t, opt)
	})

	t.Run("mutual TLS", func(t *testing.T) {
		t.Parallel()
		ca, cert, key := testutil.GenerateMTLSCerts(t)
		opt, err := creds.NewDialer(creds.WithCA(ca), creds.WithCertificate(cert, key)).DialOption("upstream.example.com")
		require.NoError(t, err)
		require.NotNil(t, opt)
	})

	t.Run("illegal configuration fails without dialing", func(t *testing.T) {
		t.Parallel()
		// Client certificate with no CA: the same legality guard as Validate.
		ca, cert, key := testutil.GenerateMTLSCerts(t)
		_ = ca
		_, err := creds.NewDialer(creds.WithCertificate(cert, key)).DialOption("x")
		require.ErrorContains(t, err, "certificate authority is required")
	})
}

func TestDialer_DialOption_ParsesMaterialOnce(t *testing.T) {
	t.Parallel()

	ca, cert, key := testutil.GenerateMTLSCerts(t)
	d := creds.NewDialer(creds.WithCA(ca), creds.WithCertificate(cert, key))

	// First call parses and caches the certificate/CA material.
	_, err := d.DialOption("first")
	require.NoError(t, err)

	// Remove the files. A Dialer that reused its cached material does not need
	// them again; a Dialer that re-reads per call would now fail.
	require.NoError(t, os.Remove(cert))
	require.NoError(t, os.Remove(key))
	require.NoError(t, os.Remove(ca))

	_, err = d.DialOption("second")
	require.NoError(t, err)
}

func TestDialer_DialOption_ConcurrentReuse(t *testing.T) {
	t.Parallel()

	ca, cert, key := testutil.GenerateMTLSCerts(t)
	d := creds.NewDialer(creds.WithCA(ca), creds.WithCertificate(cert, key))

	const n = 20
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := d.DialOption("host")
			errs <- err
			_ = i
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}
