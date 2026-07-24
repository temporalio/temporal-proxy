package config

import (
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

var validKeySchemes = []string{
	"awskms",
	"azurekeyvault",
	"gcpkms",
	"testing",
}

type (
	// Encryption configures envelope encryption of payloads. When Enabled, a
	// Default key policy is required and governs how DEKs are provisioned and
	// rotated. CacheSize bounds the in-memory DEK cache. Overrides maps a
	// namespace to a key policy that supersedes Default for that namespace; the
	// keys are pre-translation (local) namespace names, matching the namespace
	// the vault seals under at request time.
	Encryption struct {
		Enabled   bool                 `yaml:"enabled"`
		CacheSize int                  `yaml:"cacheSize"`
		Default   *KeyPolicy           `yaml:"default"`
		Overrides map[string]KeyPolicy `yaml:"overrides"`
	}

	// KeyPolicy describes the KMS key backing a DEK and its rotation schedule.
	// URI is the primary key used to wrap new DEKs; DecryptURIs are additional
	// keys accepted when unwrapping existing DEKs (for example, during key
	// migration). Duration is a DEK's lifetime and RenewBefore is the lead time
	// before that lifetime elapses at which rotation begins.
	KeyPolicy struct {
		URI         url.URL       `yaml:"uri"`
		DecryptURIs []url.URL     `yaml:"decryptURIs"`
		Duration    time.Duration `yaml:"duration"`
		RenewBefore time.Duration `yaml:"renewBefore"`
	}
)

// Validate requires a non-negative cache size, a Default policy whenever
// encryption is Enabled, and (when a Default is present at all) that the policy
// itself is valid.
func (e *Encryption) Validate() error {
	rules := []validation.Rule{
		validation.Field("cacheSize", e.CacheSize, validation.GTE(0)),
		validation.WhenRules(
			func() bool { return e.Enabled },
			validation.Field("default", e.Default, validation.Required[*KeyPolicy]()),
		),
		validation.WhenNested(func() bool { return e.Default != nil }, "default", e.Default),
	}

	// Sort the namespace keys so error ordering is deterministic across runs.
	for _, ns := range slices.Sorted(maps.Keys(e.Overrides)) {
		policy := e.Overrides[ns]
		subject := fmt.Sprintf("overrides[%s]", ns)
		rules = append(rules,
			validation.Field(subject, ns, validation.Required[string]()),
			validation.Nested(subject, &policy),
		)
	}

	return validation.Validate("", rules...)
}

// Validate requires a valid primary and decrypt key URIs, a positive Duration,
// and a RenewBefore in [0, Duration) so rotation is scheduled strictly before a
// DEK expires. The upper bound mirrors the crypto vault's own invariant.
func (p *KeyPolicy) Validate() error {
	var zd time.Duration

	return validation.Validate(
		"",
		validation.Field("uri", p.URI, validKeyURI()),
		validation.Children("decryptURIs", p.DecryptURIs, validKeyURIRef()),
		validation.Field("duration", p.Duration, validation.GT(zd)),
		validation.Field("renewBefore", p.RenewBefore, validation.GTE(zd), validation.LT(p.Duration)),
	)
}

// validKeyURI adapts validKeyURIRef to a by-value url.URL so it can check the
// KeyPolicy.URI field directly.
func validKeyURI() validation.Check[url.URL] {
	ref := validKeyURIRef()
	return func(u url.URL) error {
		return ref(&u)
	}
}

// validKeyURIRef rejects a key URI whose scheme is not one of the supported KMS
// providers. The scheme match is case-insensitive.
func validKeyURIRef() validation.Check[*url.URL] {
	return func(u *url.URL) error {
		if !slices.Contains(validKeySchemes, strings.ToLower(u.Scheme)) {
			return fmt.Errorf(
				"invalid key URI: %s, valid schemes: [%s]",
				u.String(),
				strings.Join(validKeySchemes, ","),
			)
		}

		return nil
	}
}
