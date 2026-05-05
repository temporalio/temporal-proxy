package config

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"
)

// The set of valid KMS key schemes
var validKeySchemes = []string{
	"awskms",
	"azurekeyvault",
	"base64key",
	"gcpkms",
}

type (
	// Encryption holds envelope encryption configuration for the proxy.
	Encryption struct {
		// DefaultKeyPolicy is the fallback policy applied to namespaces not listed in Policies.
		// When zero, no envelope encryption is performed.
		DefaultKeyPolicy KeyPolicy `yaml:"default"`

		// Policies maps Temporal namespace names to their per-namespace [KeyPolicy].
		// Entries here take precedence over [DefaultKeyPolicy].
		Policies map[string]KeyPolicy `yaml:"overrides"`
	}

	// KeyPolicy describes the cloud KMS key and DEK rotation schedule for a namespace.
	KeyPolicy struct {
		// URI is the vendor-specific URL identifying the cloud KMS key used to wrap DEKs.
		// URIs follow the gocloud.dev/secrets URL scheme:
		//
		//	GCP KMS:     gcpkms://projects/PROJECT/locations/LOCATION/keyRings/RING/cryptoKeys/KEY
		//	AWS KMS:     awskms:///arn:aws:kms:REGION:ACCOUNT:key/KEY-ID?region=REGION
		//	Azure Vault: azurekeyvault://VAULT.vault.azure.net/keys/KEY-NAME/KEY-VERSION
		//	Local/test:  base64key://smGbjm71Nxd1Ig5FS0wj9SlbzAIrnolCz9bQQ6uAhl4=
		URI string `yaml:"uri"`

		// Duration is how long each DEK is valid before it must be rotated.
		Duration time.Duration `yaml:"duration"`

		// RenewBefore is how far before a DEK expires the proxy should proactively rotate it.
		RenewBefore time.Duration `yaml:"renewBefore"`
	}
)

func (e *Encryption) validate() error {
	errs := make([]error, 0, len(e.Policies)+1)
	if e.DefaultKeyPolicy.URI != "" {
		if err := e.DefaultKeyPolicy.validate(); err != nil {
			errs = append(errs, fmt.Errorf("default key: %w", err))
		}
	}

	for ns, pol := range e.Policies {
		if err := pol.validate(); err != nil {
			errs = append(errs, fmt.Errorf("namespace: %s, %w", ns, err))
		}
	}

	return errors.Join(errs...)
}

func (k *KeyPolicy) validate() error {
	uri, err := url.Parse(k.URI)
	if err != nil {
		return fmt.Errorf("failed to parse key URI: %s, %w", k.URI, err)
	}

	if !slices.Contains(validKeySchemes, strings.ToLower(uri.Scheme)) {
		return fmt.Errorf(
			"invalid key URI: %s, valid schemes: [%s]",
			k.URI,
			strings.Join(validKeySchemes, ","),
		)
	}

	return nil
}
