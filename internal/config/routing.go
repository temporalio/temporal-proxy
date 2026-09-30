package config

import (
	"fmt"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// Routing selects which upstream serves a request. DefaultUpstream is the
	// fallback when no rule matches and SystemUpstream serves system-namespace
	// traffic; both name an upstream and are optional. Rules are evaluated in
	// order against the incoming request.
	Routing struct {
		DefaultUpstream string        `yaml:"default"`
		SystemUpstream  string        `yaml:"system"`
		Rules           []RoutingRule `yaml:"rules"`
	}

	// RoutingRule sends every request matched by Match to the named Upstream.
	RoutingRule struct {
		Upstream string       `yaml:"upstream"`
		Match    RoutingMatch `yaml:"match"`
	}

	// RoutingMatch describes the request attributes a rule matches on. A match
	// requires at least one of Namespace or Metadata: an empty match would
	// apply to every request, which is what DefaultUpstream is for.
	RoutingMatch struct {
		Namespace string            `yaml:"namespace"`
		Metadata  map[string]string `yaml:"metadata"`
	}
)

// Validate checks every rule. Per-rule failures are stamped with a "rules[i]"
// subject. It does not verify that the referenced upstreams exist; that check
// needs the full set of upstream names and lives in Config.Validate.
func (r *Routing) Validate() error {
	rules := make([]validation.Rule, len(r.Rules))
	for i := range r.Rules {
		rules[i] = validation.Nested(fmt.Sprintf("rules[%d]", i), &r.Rules[i])
	}

	return validation.Validate("", rules...)
}

// referentialRules returns the rules that check every upstream reference
// (DefaultUpstream, SystemUpstream, and each rule's Upstream) against the set
// of known upstream names. Each failure is stamped with the referring node's
// YAML path so it lands on the right key (e.g. "routing.rules[0]"/"upstream").
// Empty references are skipped: default and system are optional, and a rule's
// missing upstream is already reported as required by RoutingRule.Validate.
//
// The rules are appended at the Config level rather than composed under
// Routing.Validate because they need the full set of upstream names, which is
// only known there.
func (r *Routing) referentialRules(known map[string]struct{}) []validation.Rule {
	check := knownUpstream(known)
	ref := func(subject, field, name string) validation.Rule {
		return func() validation.Errors {
			if name == "" {
				return nil
			}

			err := check(name)
			if err == nil {
				return nil
			}

			return validation.Errors{{Subject: subject, Field: field, Message: err.Error()}}
		}
	}

	rules := []validation.Rule{
		ref("routing", "default", r.DefaultUpstream),
		ref("routing", "system", r.SystemUpstream),
	}

	for i := range r.Rules {
		rules = append(rules, ref(fmt.Sprintf("routing.rules[%d]", i), "upstream", r.Rules[i].Upstream))
	}

	return rules
}

// Validate requires the referenced upstream and checks the match.
func (r *RoutingRule) Validate() error {
	return validation.Validate(
		"",
		validation.Field("upstream", r.Upstream, validation.Required[string]()),
		validation.Nested("", &r.Match),
	)
}

// Validate requires at least one of Namespace or Metadata to be set.
func (m *RoutingMatch) Validate() error {
	return validation.Validate(
		"",
		validation.WhenRules(
			func() bool { return len(m.Metadata) == 0 },
			validation.Field("namespace", m.Namespace, validation.Required[string]()),
		),
	)
}

// knownUpstream returns a check that fails when its value is not a key in
// known.
func knownUpstream(known map[string]struct{}) validation.Check[string] {
	return func(name string) error {
		if _, ok := known[name]; !ok {
			return fmt.Errorf("references unknown upstream %q", name)
		}

		return nil
	}
}
