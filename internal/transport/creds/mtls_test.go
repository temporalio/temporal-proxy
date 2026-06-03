package creds_test

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestMTLS_DialOption(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		setup   func(t *testing.T) (caFile, certFile, keyFile string)
		wantErr string
	}{
		{
			name: "success",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				return testutil.GenerateMTLSCerts(t)
			},
		},
		{
			name: "missing cert file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				caFile, _, keyFile := testutil.GenerateMTLSCerts(t)
				return caFile, filepath.Join(t.TempDir(), "missing.pem"), keyFile
			},
			wantErr: "failed to load client key pair",
		},
		{
			name: "missing key file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				caFile, certFile, _ := testutil.GenerateMTLSCerts(t)
				return caFile, certFile, filepath.Join(t.TempDir(), "missing.pem")
			},
			wantErr: "failed to load client key pair",
		},
		{
			name: "missing CA file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				_, certFile, keyFile := testutil.GenerateMTLSCerts(t)
				return filepath.Join(t.TempDir(), "missing.pem"), certFile, keyFile
			},
			wantErr: "failed to load CA certificate",
		},
		{
			name: "invalid CA PEM",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				dir := t.TempDir()
				_, certFile, keyFile := testutil.GenerateMTLSCerts(t)
				caFile := testutil.WriteFile(t, dir, "ca.pem", []byte("not pem"))
				return caFile, certFile, keyFile
			},
			wantErr: "failed to parse CA file",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			caFile, certFile, keyFile := tc.setup(t)
			opt, err := creds.NewMTLS(caFile, certFile, keyFile, creds.MTLSOptions{}).DialOption()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, opt)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, opt)
		})
	}
}

func TestMTLS_Options(t *testing.T) {
	t.Parallel()

	t.Run("InsecureSkipVerify is propagated", func(t *testing.T) {
		t.Parallel()

		caFile, certFile, keyFile := testutil.GenerateMTLSCerts(t)
		opt, err := creds.NewMTLS(caFile, certFile, keyFile, creds.MTLSOptions{
			InsecureSkipVerify: true,
		}).DialOption()
		require.NoError(t, err)
		require.NotNil(t, opt)
	})

	t.Run("ServerName is propagated", func(t *testing.T) {
		t.Parallel()

		caFile, certFile, keyFile := testutil.GenerateMTLSCerts(t)
		opt, err := creds.NewMTLS(caFile, certFile, keyFile, creds.MTLSOptions{
			ServerName: "example.com",
		}).DialOption()
		require.NoError(t, err)
		require.NotNil(t, opt)
	})
}

func TestMTLS_ServerOption(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		setup   func(t *testing.T) (caFile, certFile, keyFile string)
		wantErr string
	}{
		{
			name: "success",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				return testutil.GenerateMTLSCerts(t)
			},
		},
		{
			name: "missing cert file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				caFile, _, keyFile := testutil.GenerateMTLSCerts(t)
				return caFile, filepath.Join(t.TempDir(), "missing.pem"), keyFile
			},
			wantErr: "failed to load server key pair",
		},
		{
			name: "missing key file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				caFile, certFile, _ := testutil.GenerateMTLSCerts(t)
				return caFile, certFile, filepath.Join(t.TempDir(), "missing.pem")
			},
			wantErr: "failed to load server key pair",
		},
		{
			name: "missing CA file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				_, certFile, keyFile := testutil.GenerateMTLSCerts(t)
				return filepath.Join(t.TempDir(), "missing.pem"), certFile, keyFile
			},
			wantErr: "failed to load CA certificate",
		},
		{
			name: "invalid CA PEM",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				dir := t.TempDir()
				_, certFile, keyFile := testutil.GenerateMTLSCerts(t)
				caFile := testutil.WriteFile(t, dir, "ca.pem", []byte("not pem"))
				return caFile, certFile, keyFile
			},
			wantErr: "failed to parse CA file",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			caFile, certFile, keyFile := tc.setup(t)
			opt, err := creds.NewMTLS(caFile, certFile, keyFile, creds.MTLSOptions{}).ServerOption()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, opt)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, opt)
		})
	}
}

func TestMTLS_Validate(t *testing.T) {
	t.Parallel()

	validLeaf := func(t *testing.T) string {
		t.Helper()
		return writePEMFile(t, testutil.RSACert(t, validTemplate()))
	}

	validCA := func(t *testing.T) string {
		t.Helper()
		return writePEMFile(t, testutil.RSACert(t, caTemplate()))
	}

	t.Run("valid leaf and CA pass", func(t *testing.T) {
		t.Parallel()

		err := creds.NewMTLS(validCA(t), validLeaf(t), "", creds.MTLSOptions{}).Validate()
		require.NoError(t, err)
	})

	t.Run("missing leaf cert file", func(t *testing.T) {
		t.Parallel()

		err := creds.NewMTLS(validCA(t), filepath.Join(t.TempDir(), "missing.pem"), "", creds.MTLSOptions{}).Validate()
		require.ErrorContains(t, err, "failed to read PEM file")
	})

	t.Run("missing CA file", func(t *testing.T) {
		t.Parallel()

		err := creds.NewMTLS(filepath.Join(t.TempDir(), "missing.pem"), validLeaf(t), "", creds.MTLSOptions{}).Validate()
		require.ErrorContains(t, err, "failed to read PEM file")
	})

	t.Run("non-CA cert in CA slot fails", func(t *testing.T) {
		t.Parallel()

		// validTemplate has IsCA=false; using it as the CA file should fail the
		// IsCACertificate validator.
		nonCAFile := writePEMFile(t, testutil.RSACert(t, validTemplate()))
		err := creds.NewMTLS(nonCAFile, validLeaf(t), "", creds.MTLSOptions{}).Validate()
		require.ErrorContains(t, err, "not a CA")
	})

	t.Run("leaf and CA failures are both reported", func(t *testing.T) {
		t.Parallel()

		// Validate runs both checks and combines failures into a single
		// validation.Errors, so a bad leaf does not hide a bad CA.
		expiredLeaf := writePEMFile(t, testutil.RSACert(t, &x509.Certificate{
			SerialNumber: big.NewInt(7),
			Subject:      pkix.Name{CommonName: "expired-leaf"},
			NotBefore:    time.Now().Add(-2 * time.Hour),
			NotAfter:     time.Now().Add(-time.Hour),
		}))
		nonCAFile := writePEMFile(t, testutil.RSACert(t, validTemplate()))

		err := creds.NewMTLS(nonCAFile, expiredLeaf, "", creds.MTLSOptions{}).Validate()
		require.ErrorContains(t, err, "expired-leaf")
		require.ErrorContains(t, err, "not a CA")

		var verrs validation.Errors
		require.ErrorAs(t, err, &verrs)
		require.Len(t, verrs, 2)
	})
}
