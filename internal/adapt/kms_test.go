package adapt_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"

	"github.com/temporalio/temporal-proxy/internal/adapt"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/crypto"
)

// testKeyURI is a deterministic local key for tests — no KMS required.
const testKeyURI = "base64key://smGbjm71Nxd1Ig5FS0wj9SlbzAIrnolCz9bQQ6uAhl4="

func TestCryptoPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enc     config.Encryption
		wantErr bool
		check   func(t *testing.T, p adapt.CryptoParams)
	}{
		{
			name: "no encryption configured",
			check: func(t *testing.T, p adapt.CryptoParams) {
				require.Nil(t, p.DefaultPolicy.KEK)
				require.Empty(t, p.Policies)
			},
		},
		{
			name: "default policy",
			enc: config.Encryption{
				DefaultKeyPolicy: config.KeyPolicy{
					URI:         testKeyURI,
					Duration:    time.Hour,
					RenewBefore: 5 * time.Minute,
				},
			},
			check: func(t *testing.T, p adapt.CryptoParams) {
				require.NotNil(t, p.DefaultPolicy.KEK)
				require.Equal(t, testKeyURI, p.DefaultPolicy.KEK.ID())
				require.Equal(t, time.Hour, p.DefaultPolicy.Duration)
				require.Equal(t, 5*time.Minute, p.DefaultPolicy.RenewBefore)
			},
		},
		{
			name: "namespace policies",
			enc: config.Encryption{
				Policies: map[string]config.KeyPolicy{
					"ns1": {URI: testKeyURI, Duration: time.Hour, RenewBefore: 5 * time.Minute},
				},
			},
			check: func(t *testing.T, p adapt.CryptoParams) {
				require.Nil(t, p.DefaultPolicy.KEK)
				require.Contains(t, p.Policies, "ns1")
				require.Equal(t, testKeyURI, p.Policies["ns1"].KEK.ID())
			},
		},
		{
			name: "duration below minimum is clamped",
			enc: config.Encryption{
				DefaultKeyPolicy: config.KeyPolicy{URI: testKeyURI, Duration: 0},
			},
			check: func(t *testing.T, p adapt.CryptoParams) {
				require.Equal(t, 1*time.Minute, p.DefaultPolicy.Duration)
			},
		},
		{
			name: "negative renewBefore defaults to 10% of duration",
			enc: config.Encryption{
				DefaultKeyPolicy: config.KeyPolicy{
					URI:         testKeyURI,
					Duration:    time.Hour,
					RenewBefore: -1,
				},
			},
			check: func(t *testing.T, p adapt.CryptoParams) {
				require.Equal(t, 6*time.Minute, p.DefaultPolicy.RenewBefore)
			},
		},
		{
			name:    "unregistered URI scheme returns error",
			enc:     config.Encryption{DefaultKeyPolicy: config.KeyPolicy{URI: "hashivault://localhost/v1/transit/keys/k"}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.Config{Encryption: tc.enc}
			p, err := adapt.CryptoPolicies(context.Background(), cfg, log.NewNoopLogger())
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tc.check != nil {
				tc.check(t, p)
			}
		})
	}
}

func TestRotateDEKs_StopsOnCancel(t *testing.T) {
	t.Parallel()

	reg := crypto.NewKEKRegistry()
	s, err := crypto.NewSealer(reg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled — goroutine exits on the first select iteration

	adapt.RotateDEKs(ctx, s, log.NewNoopLogger())
	// A goroutine leak would be caught by the test timeout.
}
