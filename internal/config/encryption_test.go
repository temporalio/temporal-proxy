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

func TestEncryption_ValidateAcceptsExtensionKeyScheme(t *testing.T) {
	t.Parallel()

	// The referential check lives at the Config level; Encryption.Validate only
	// sees the scheme, which must now be recognized.
	e := &config.Encryption{Default: &config.KeyPolicy{
		URI:         mustURL(t, "extension://audit/payments"),
		Duration:    time.Hour,
		RenewBefore: time.Minute,
	}}

	require.NoError(t, e.Validate())
}

func TestConfig_ValidateExtensionKeyReferences(t *testing.T) {
	t.Parallel()

	policy := func(uri string, decrypt ...string) *config.KeyPolicy {
		p := &config.KeyPolicy{
			URI:         mustURL(t, uri),
			Duration:    time.Hour,
			RenewBefore: time.Minute,
		}
		for _, d := range decrypt {
			p.DecryptURIs = append(p.DecryptURIs, mustURL(t, d))
		}

		return p
	}

	base := func(e config.Encryption) *config.Config {
		return &config.Config{
			Listen:     config.ListenConfig{HostPort: ":8080"},
			Encryption: e,
			ExtensionServers: config.ExtensionServerList{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
			},
			Upstreams: config.UpstreamList{
				{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
			},
		}
	}

	tests := []struct {
		name       string
		encryption config.Encryption
		wantTuples [][2]string
	}{
		{
			name:       "a configured extension server resolves",
			encryption: config.Encryption{Default: policy("extension://audit/payments")},
		},
		{
			name:       "several keys may name the same server",
			encryption: config.Encryption{Default: policy("extension://audit/payments", "extension://audit/billing")},
		},
		{
			// An ARN goes in the path (gocloud's "awskms:///<ARN>" form, since a
			// bare ARN has colons that url.Parse reads as a port), so the host is
			// empty. That must not be mistaken for an extension server reference.
			name:       "a hostless non-extension uri is not treated as a reference",
			encryption: config.Encryption{Default: policy("awskms:///arn:aws:kms:us-east-1:932528106278:key/8be0dcc5")},
		},
		{
			name:       "a non-extension uri with a host is not treated as a reference",
			encryption: config.Encryption{Default: policy("awskms://alias/payments")},
		},
		{
			name:       "an unknown server on the primary uri",
			encryption: config.Encryption{Default: policy("extension://missing/payments")},
			wantTuples: [][2]string{{"encryption.default", "uri"}},
		},
		{
			name:       "an unknown server on a decrypt uri keeps its index",
			encryption: config.Encryption{Default: policy("extension://audit/v2", "extension://audit/v1", "extension://gone/v0")},
			wantTuples: [][2]string{{"encryption.default", "decryptURIs[1]"}},
		},
		{
			name: "an unknown server in an override is stamped with its namespace",
			encryption: config.Encryption{
				Default:   policy("extension://audit/payments"),
				Overrides: map[string]config.KeyPolicy{"billing": *policy("extension://nope/k")},
			},
			wantTuples: [][2]string{{"encryption.overrides[billing]", "uri"}},
		},
		{
			name: "failures across default and overrides aggregate",
			encryption: config.Encryption{
				Default:   policy("extension://gone/a"),
				Overrides: map[string]config.KeyPolicy{"billing": *policy("extension://nope/b")},
			},
			wantTuples: [][2]string{
				{"encryption.default", "uri"},
				{"encryption.overrides[billing]", "uri"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertTuples(t, base(tt.encryption).Validate(), tt.wantTuples)
		})
	}
}

func TestEncryption_ValidateRejectsHostlessExtensionURI(t *testing.T) {
	t.Parallel()

	// A hostless extension URI names no server, so the referential check has
	// nothing to report against. Catch it with the scheme instead, at config
	// validation rather than when the KEK is opened.
	tests := []struct {
		name    string
		uri     string
		wantErr string
	}{
		{
			name:    "no host at all",
			uri:     "extension://",
			wantErr: "must name an extension server",
		},
		{
			name:    "a path but no host",
			uri:     "extension:///payments",
			wantErr: "must name an extension server",
		},
		{
			name: "a host is accepted",
			uri:  "extension://audit/payments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := &config.Encryption{Default: &config.KeyPolicy{
				URI:         mustURL(t, tt.uri),
				Duration:    time.Hour,
				RenewBefore: time.Minute,
			}}

			err := e.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
