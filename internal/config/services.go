package config

import (
	"fmt"
	"slices"
	"strings"

	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

// Services is the set of gRPC services the proxy is allowed to forward, each
// named by its proto full name (e.g.
// "temporal.api.workflowservice.v1.WorkflowService").
type Services []string

// Validate rejects duplicate entries and any name the proxy cannot forward. An
// empty list is valid. Failures carry the "allowedServices" field and no
// subject, so Config nests this under an empty subject rather than restating
// the name.
func (s *Services) Validate() error {
	return validation.Validate(
		"",
		validation.Field[[]string](
			"allowedServices",
			*s,
			validation.Unique[string](),
			resolvableServices(),
		),
	)
}

// resolvableServices accepts only names the proxy can actually forward, which
// is stricter than "registered": services.All is the forwardable set plus its
// compatibility aliases, while the proto registry knows every descriptor linked
// into the binary, including ones that arrive transitively through a dependency.
func resolvableServices() validation.Check[[]string] {
	return func(names []string) error {
		forwardable := services.All()

		// Membership is checked against All so a compatibility alias is
		// accepted, but the hint lists Known: it should steer operators to the
		// canonical spelling rather than advertise the superseded one. The
		// wording therefore suggests rather than claims to be exhaustive.
		hint := strings.Join(services.Known(), ", ")

		var errs validation.Errors
		for _, name := range names {
			if slices.Contains(forwardable, name) {
				continue
			}

			msg := fmt.Sprintf("%q is registered but not forwardable (choose from: %s)", name, hint)
			if _, err := services.Resolve(name); err != nil {
				msg = fmt.Sprintf("%s (choose from: %s)", err, hint)
			}

			errs = append(errs, validation.Error{Message: msg})
		}

		if len(errs) == 0 {
			return nil
		}

		return errs
	}
}
