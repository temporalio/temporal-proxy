package authz

import (
	"github.com/golang-jwt/jwt/v5"
)

const (
	// Issuer is the iss every token in this example carries, and the only one the
	// extension server accepts.
	Issuer = "authz-example"

	// PermissionsClaim is the claim carrying a subject's permissions. It is
	// vendor-namespaced, as a private claim should be, and its shape is this
	// deployment's own: nothing the proxy ships can read it, which is the reason
	// the extension server exists.
	//
	// Claims.Permissions must be tagged with this exact value. A struct tag cannot
	// reference a constant, so the two are kept side by side here, where a mismatch
	// between them is visible, rather than in the commands that would drift apart.
	PermissionsClaim = "https://acme.example/temporal"

	// SystemScope names the cluster scope in the permissions claim, the authority a
	// subject holds regardless of namespace.
	SystemScope = "system"
)

type (
	// Claims is a token body: the registered claims the extension server verifies,
	// plus this deployment's permissions.
	Claims struct {
		jwt.RegisteredClaims
		Permissions *Permissions `json:"https://acme.example/temporal,omitempty"`
	}

	// Permissions is what a subject may do, as role names rather than a bitmask so
	// a token stays readable in a debugger. System applies across every namespace;
	// Namespaces applies within the one it names.
	//
	// A role name this server does not recognize is ignored rather than rejected,
	// so both fields are advisory: what a caller can actually do is decided by the
	// extension server, from the roles it understood.
	Permissions struct {
		System     []string            `json:"system,omitempty"`
		Namespaces map[string][]string `json:"namespaces,omitempty"`
	}
)
