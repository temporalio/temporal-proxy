package config_test

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestEncryptionValidate(t *testing.T) {
	t.Parallel()

	valid := validKeyPolicy(t)

	tests := []struct {
		name    string
		cfg     config.Encryption
		wantErr string // substring; "" means valid
	}{
		{
			name: "disabled with no default",
			cfg:  config.Encryption{Enabled: false},
		},
		{
			name: "enabled with valid default",
			cfg:  config.Encryption{Enabled: true, Default: &valid},
		},
		{
			name:    "negative cache size",
			cfg:     config.Encryption{CacheSize: -1},
			wantErr: "cacheSize",
		},
		{
			name:    "enabled without default",
			cfg:     config.Encryption{Enabled: true},
			wantErr: "default",
		},
		{
			name: "default present but invalid, even when disabled",
			cfg: config.Encryption{
				Enabled: false,
				Default: &config.KeyPolicy{URI: mustURL(t, "awskms://alias/primary"), Duration: time.Hour, RenewBefore: 2 * time.Hour},
			},
			wantErr: "renewBefore",
		},
		{
			name: "valid overrides",
			cfg: config.Encryption{
				Enabled:   true,
				Default:   &valid,
				Overrides: map[string]config.KeyPolicy{"payments": valid},
			},
		},
		{
			name: "override with invalid policy",
			cfg: config.Encryption{
				Enabled: true,
				Default: &valid,
				Overrides: map[string]config.KeyPolicy{
					"payments": {URI: mustURL(t, "https://example.com/key"), Duration: time.Hour},
				},
			},
			wantErr: "overrides[payments]",
		},
		{
			name: "empty override namespace key",
			cfg: config.Encryption{
				Enabled:   true,
				Default:   &valid,
				Overrides: map[string]config.KeyPolicy{"": valid},
			},
			wantErr: "overrides",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestKeyPolicyValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*config.KeyPolicy)
		wantErr string
	}{
		{
			name:   "valid",
			mutate: func(*config.KeyPolicy) {},
		},
		{
			name:   "renewBefore zero is allowed",
			mutate: func(p *config.KeyPolicy) { p.RenewBefore = 0 },
		},
		{
			name:    "invalid primary uri scheme",
			mutate:  func(p *config.KeyPolicy) { p.URI = mustURL(t, "https://example.com/key") },
			wantErr: "uri",
		},
		{
			name:    "invalid decrypt uri scheme",
			mutate:  func(p *config.KeyPolicy) { p.DecryptURIs = []url.URL{mustURL(t, "ftp://example.com/key")} },
			wantErr: "decryptURIs",
		},
		{
			name:    "zero duration",
			mutate:  func(p *config.KeyPolicy) { p.Duration = 0 },
			wantErr: "duration",
		},
		{
			name:    "negative renewBefore",
			mutate:  func(p *config.KeyPolicy) { p.RenewBefore = -1 * time.Minute },
			wantErr: "renewBefore",
		},
		{
			name:    "renewBefore equal to duration",
			mutate:  func(p *config.KeyPolicy) { p.RenewBefore = p.Duration },
			wantErr: "renewBefore",
		},
		{
			name:    "renewBefore greater than duration",
			mutate:  func(p *config.KeyPolicy) { p.RenewBefore = 2 * p.Duration },
			wantErr: "renewBefore",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := validKeyPolicy(t)
			tt.mutate(&p)

			err := p.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func mustURL(t *testing.T, raw string) url.URL {
	t.Helper()

	u, err := url.Parse(raw)
	require.NoError(t, err)

	return *u
}

func validKeyPolicy(t *testing.T) config.KeyPolicy {
	t.Helper()

	return config.KeyPolicy{
		URI:         mustURL(t, "awskms://alias/primary"),
		DecryptURIs: []url.URL{mustURL(t, "gcpkms://projects/p/locations/l/keyRings/r/cryptoKeys/k")},
		Duration:    time.Hour,
		RenewBefore: 10 * time.Minute,
	}
}
