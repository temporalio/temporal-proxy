package config_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// minimalCluster is prepended to every encryption test so Config.validate() passes
// the cluster requirement and reaches the encryption validation.
const minimalCluster = `
clusters:
  - type: inbound
    listener:
      hostPort: :8233
`

func TestEncryption_DefaultKeyPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		uri     string
		wantErr bool
	}{
		{name: "gcpkms", uri: "gcpkms://projects/p/locations/global/keyRings/r/cryptoKeys/k"},
		{name: "awskms", uri: "awskms:///arn:aws:kms:us-east-1:123456789012:key/abc?region=us-east-1"},
		{name: "azurekeyvault", uri: "azurekeyvault://my-vault.vault.azure.net/keys/my-key/v1"},
		{name: "base64key", uri: "base64key://smGbjm71Nxd1Ig5FS0wj9SlbzAIrnolCz9bQQ6uAhl4="},
		{name: "unknown scheme", uri: "hashivault://localhost/v1/transit/keys/my-key", wantErr: true},
		{name: "unparseable URI", uri: "://bad", wantErr: true},
		{
			name: "empty URI skips validation",
			uri:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			yaml := minimalCluster
			if tc.uri != "" {
				yaml += fmt.Sprintf("\nencryption:\n  default:\n    uri: %q\n", tc.uri)
			}

			_, err := config.Load(strings.NewReader(yaml))
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestEncryption_NamespacePolicies(t *testing.T) {
	t.Parallel()

	t.Run("valid policy", func(t *testing.T) {
		t.Parallel()

		yaml := minimalCluster + `
encryption:
  overrides:
    my-namespace:
      uri: "gcpkms://projects/p/locations/global/keyRings/r/cryptoKeys/k"
      duration: 20m
      renewBefore: 2m
`
		cfg, err := config.Load(strings.NewReader(yaml))
		require.NoError(t, err)
		require.Equal(t, config.KeyPolicy{
			URI:         "gcpkms://projects/p/locations/global/keyRings/r/cryptoKeys/k",
			Duration:    20 * time.Minute,
			RenewBefore: 2 * time.Minute,
		}, cfg.Encryption.Policies["my-namespace"])
	})

	t.Run("invalid scheme", func(t *testing.T) {
		t.Parallel()

		yaml := minimalCluster + `
encryption:
  overrides:
    my-namespace:
      uri: "hashivault://localhost/v1/transit/keys/my-key"
`
		_, err := config.Load(strings.NewReader(yaml))
		require.Error(t, err)
		require.ErrorContains(t, err, "my-namespace")
	})
}

func TestEncryption_MultipleErrors(t *testing.T) {
	t.Parallel()

	// Both default and namespace policies are invalid; all errors should be reported.
	yaml := minimalCluster + `
encryption:
  default:
    uri: "bad-scheme://key"
  overrides:
    ns1:
      uri: "also-bad://key"
    ns2:
      uri: "gcpkms://projects/p/locations/global/keyRings/r/cryptoKeys/k"
`
	_, err := config.Load(strings.NewReader(yaml))
	require.Error(t, err)
	require.ErrorContains(t, err, "default key")
	require.ErrorContains(t, err, "ns1")
}
