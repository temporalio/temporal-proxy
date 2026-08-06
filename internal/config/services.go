package config

import (
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

// apiPackagePrefix bounds the message types the payload visitor in
// go.temporal.io/api was generated for. That visitor is a type switch with no
// default case, so a message outside this tree is skipped in silence rather
// than reported.
const apiPackagePrefix = "temporal.api."

// EncryptionBlindSpot reports whether sd has a method whose request or response
// carries payloads the payload visitor cannot see. Such a service would forward
// cleartext while configuration claims encryption is on, so startup refuses it
// rather than discovering it per request, or never.
//
// Coverage is approximated by the top-level message's package, which is what
// the visitor switches on. AdminService is the motivating case: its messages
// live in go.temporal.io/server and carry payloads.
func EncryptionBlindSpot(sd protoreflect.ServiceDescriptor) bool {
	methods := sd.Methods()
	for i := 0; i < methods.Len(); i++ {
		m := methods.Get(i)
		for _, md := range []protoreflect.MessageDescriptor{m.Input(), m.Output()} {
			if !strings.HasPrefix(string(md.FullName()), apiPackagePrefix) && protoutil.CarriesPayloads(md) {
				return true
			}
		}
	}

	return false
}

// EnabledServices returns the gRPC services the proxy exposes: the configured
// list, or the default set when configuration names none. An explicit list
// replaces the default rather than extending it, so a configuration that lists
// only OperatorService refuses WorkflowService.
func (c *Config) EnabledServices() []string {
	if len(c.AllowedServices) == 0 {
		return services.Default()
	}

	return c.AllowedServices
}

// serviceRules returns the rules checking allowedServices: names must be
// unique, each must be a forwardable service (see resolvableServices), and none
// may leave a payload-carrying message unreachable by the encryption visitor
// while encryption is enabled.
func (c *Config) serviceRules() []validation.Rule {
	return []validation.Rule{
		validation.Field("allowedServices", c.AllowedServices, validation.Unique[string]()),
		validation.Field("allowedServices", c.AllowedServices, resolvableServices()),
		c.encryptionCoverageRule(),
	}
}

// encryptionCoverageRule refuses a configuration that enables encryption while
// exposing a service whose payloads the visitor cannot reach.
func (c *Config) encryptionCoverageRule() validation.Rule {
	return func() validation.Errors {
		if !c.Encryption.Enabled {
			return nil
		}

		var errs validation.Errors
		for _, name := range c.EnabledServices() {
			sd, err := services.Resolve(name)
			if err != nil {
				// Resolution is reported by resolvableServices; do not double up.
				continue
			}

			if EncryptionBlindSpot(sd) {
				errs = append(errs, validation.Error{
					Subject: "allowedServices",
					Field:   "allowedServices",
					Message: fmt.Sprintf(
						"%q carries payloads the encryption visitor cannot reach; "+
							"remove it from allowedServices or disable encryption",
						name,
					),
				})
			}
		}

		return errs
	}
}

// resolvableServices checks that every name is forwardable: a member of
// services.All(), the expanded set this proxy can actually route requests to
// (so an explicitly-listed compatibility alias, like the reflection v1alpha
// spelling, still passes). Membership, not mere resolvability, is the bar:
// services.Resolve succeeds for anything registered in
// protoregistry.GlobalFiles, which is a superset of what the forwarder and the
// gateway's gate know how to admit -- grpc.health.v1.Health is the standing
// example, linked in for the proxy's own health server but not a service this
// proxy forwards.
//
// The failure message hints with services.Known() rather than services.All():
// Known is the spelling an operator should actually write (the alias exists so
// a client can probe for it, not so a config author names it directly), and
// hinting with All would tell someone to list exactly the alias, which maps
// nowhere back to the name that admits it. A name that resolves but is not
// forwardable gets a message saying so; a name that does not resolve at all
// gets the registry's own message appended. Every bad name in the list is
// reported together, not just the first, so an operator with several typos
// fixes them in one pass rather than one restart at a time.
func resolvableServices() validation.Check[[]string] {
	return func(names []string) error {
		forwardable := services.All()
		hint := strings.Join(services.Known(), ", ")

		var errs validation.Errors
		for _, name := range names {
			if slices.Contains(forwardable, name) {
				continue
			}

			msg := fmt.Sprintf("%q is registered but not forwardable (forwardable services: %s)", name, hint)
			if _, err := services.Resolve(name); err != nil {
				msg = fmt.Sprintf("%s (forwardable services: %s)", err, hint)
			}

			errs = append(errs, validation.Error{Message: msg})
		}

		if len(errs) == 0 {
			return nil
		}

		return errs
	}
}
