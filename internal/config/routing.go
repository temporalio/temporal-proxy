package config

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	glob "github.com/temporalio/temporal-proxy/pkg/match"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// Routing selects which upstream serves a request. DefaultUpstream is the
	// fallback when no rule matches and SystemUpstream serves a request that
	// carries no namespace and matches no rule; both name an upstream and are
	// optional. Rules are evaluated in order against the incoming request.
	Routing struct {
		DefaultUpstream string        `yaml:"default"`
		SystemUpstream  string        `yaml:"system"`
		Rules           []RoutingRule `yaml:"rules"`
	}

	// RoutingRule sends every request matched by Match to the named Upstream.
	RoutingRule struct {
		Upstream string       `yaml:"upstream"`
		Match    RoutingMatch `yaml:"match"`

		matchers *ruleMatchers
	}

	// RoutingMatch describes the request attributes a rule matches on. A match
	// requires at least one of Namespace or Metadata: an empty match would
	// apply to every request, which is what DefaultUpstream is for.
	RoutingMatch struct {
		Namespace string            `yaml:"namespace"`
		Metadata  map[string]string `yaml:"metadata"`
	}

	// ruleMatchers is a routing rule's compiled namespace and metadata patterns.
	ruleMatchers struct {
		ns   glob.Matcher
		meta map[string]glob.Matcher
	}
)

// Validate checks every rule. Per-rule failures are stamped with a "rules[i]"
// subject. It does not verify that the referenced upstreams exist; that check
// needs the full set of upstream names and runs in Config.Prepare.
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
// The rules are appended at the Config level, where the full set of names is
// known.
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

// compile builds the routing table's matchers. Validation has already passed.
func (r *Routing) compile() error {
	for i := range r.Rules {
		m, errs := r.Rules[i].Match.compile()
		if len(errs) > 0 {
			return fmt.Errorf("rules[%d]: %w", i, errs)
		}

		r.Rules[i].matchers = m
	}

	return nil
}

// reset drops every rule's matchers.
func (r *Routing) reset() {
	for i := range r.Rules {
		r.Rules[i].matchers = nil
	}
}

// Matchers returns the rule's compiled namespace matcher and its metadata
// matchers keyed by lowercased metadata key. An empty namespace pattern matches
// every namespace. Panics if the rule was never prepared by [Config.Prepare].
func (r *RoutingRule) Matchers() (ns glob.Matcher, meta map[string]glob.Matcher) {
	if r.matchers == nil {
		panic("config: RoutingRule used before Config.Prepare")
	}

	return r.matchers.ns, r.matchers.meta
}

// Validate requires the referenced upstream and checks the match.
func (r *RoutingRule) Validate() error {
	return validation.Validate(
		"",
		validation.Field("upstream", r.Upstream, validation.Required[string]()),
		validation.Nested("", &r.Match),
	)
}

// Validate requires at least one of Namespace or Metadata to be set and checks
// that every pattern compiles.
func (m *RoutingMatch) Validate() error {
	return validation.Validate(
		"",
		validation.WhenRules(
			func() bool { return len(m.Metadata) == 0 },
			validation.Field("namespace", m.Namespace, validation.Required[string]()),
		),
		func() validation.Errors {
			_, errs := m.compile()
			return errs
		},
	)
}

// compile compiles the namespace and metadata patterns. Metadata keys are
// lowercased to match canonical gRPC metadata, so two keys that differ only in
// case are rejected.
func (m *RoutingMatch) compile() (*ruleMatchers, validation.Errors) {
	var errs validation.Errors

	ns, err := glob.Compile(cmp.Or(m.Namespace, "*"))
	if err != nil {
		errs = append(errs, validation.Error{Field: "namespace", Message: err.Error()})
	}

	meta := make(map[string]glob.Matcher, len(m.Metadata))
	seen := make(map[string]string, len(m.Metadata))
	for _, k := range slices.Sorted(maps.Keys(m.Metadata)) {
		lk := strings.ToLower(k)
		if prev, ok := seen[lk]; ok {
			errs = append(errs, validation.Error{
				Field:   "metadata",
				Message: fmt.Sprintf("keys %q and %q both map to %q when lowercased", prev, k, lk),
			})

			continue
		}

		seen[lk] = k

		mm, err := glob.Compile(m.Metadata[k])
		if err != nil {
			errs = append(errs, validation.Error{Field: "metadata[" + k + "]", Message: err.Error()})
			continue
		}

		meta[lk] = mm
	}

	if len(errs) > 0 {
		return nil, errs
	}

	return &ruleMatchers{ns: ns, meta: meta}, nil
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
