package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/temporalio/temporal-proxy/examples/authz"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

const (
	// The roles a subject can hold within a scope, as a bitmask so they combine:
	// an effective role is the OR of every role granted at that scope. These are
	// go.temporal.io/server/common/authorization's Role values, with the same
	// numbering, because Authenticate compares them the way that package does.
	roleWorker = role(1 << iota)
	roleReader
	roleWriter
	roleAdmin

	// roleUndefined is holding nothing, which is what an absent namespace entry
	// yields from the map lookup in Authenticate.
	roleUndefined = role(0)
)

// roleNames pairs each role with the name a token uses for it, and is the only
// place either is written down: roleFor reads it forwards and String reads it
// backwards, so a role cannot be one without being the other.
var roleNames = []struct {
	role role
	name string
}{
	{roleWorker, "worker"},
	{roleReader, "reader"},
	{roleWriter, "writer"},
	{roleAdmin, "admin"},
}

type (
	// role is what a subject may do within one scope. See the bitmask above.
	role int16

	// claims is what a verified token says about its subject, and is this example's
	// equivalent of the Claims a Temporal ClaimMapper produces. system applies in
	// every namespace; namespaces applies only within the one it keys.
	claims struct {
		subject    string
		system     role
		namespaces map[string]role
	}

	// mapper turns a bearer credential into claims, which is the ClaimMapper half
	// of this server's job. It verifies the token and translates the permissions
	// claim; it does not decide anything, and knows nothing about what the caller
	// is trying to reach.
	mapper struct {
		secret []byte
		log    logger.Logger
	}
)

// newMapper returns a mapper verifying HS256 signatures against secret.
func newMapper(secret []byte, log logger.Logger) *mapper {
	return &mapper{secret: secret, log: log}
}

// claimsFrom verifies bearer and returns what it says about its subject.
//
// Every error means the credential could not be turned into claims at all: a
// broken signature, an expired or unsigned token, a foreign issuer, a missing
// subject. The caller reports those as Unauthenticated, because a new token
// would fix them. A token that verifies but grants nothing is not an error; it
// yields claims holding no roles, and the authorization check denies it.
func (m *mapper) claimsFrom(bearer string) (*claims, error) {
	var tc authz.Claims

	_, err := jwt.ParseWithClaims(
		bearer,
		&tc,
		func(*jwt.Token) (any, error) { return m.secret, nil },
		// The algorithm is pinned, so a token asking to be verified some other way
		// (notably "none") is rejected rather than trusted about its own signing.
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Name}),
		// Required rather than merely honoured: a token with no exp would otherwise
		// be valid forever, and this example has no revocation.
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(authz.Issuer),
	)
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	if tc.Subject == "" {
		return nil, errors.New("token carries no subject")
	}

	c := &claims{subject: tc.Subject, namespaces: make(map[string]role)}
	if p := tc.Permissions; p != nil {
		c.system = m.rolesFrom(p.System, authz.SystemScope)

		for ns, names := range p.Namespaces {
			c.namespaces[ns] = m.rolesFrom(names, ns)
		}
	}

	return c, nil
}

// rolesFrom ORs together the roles names grant, skipping any name this server
// does not recognize. scope names the scope being read, for the log line.
//
// Skipping rather than failing is what Temporal's own JWT claim mapper does with
// a permission it cannot parse, and it is the kinder behaviour: a claim written
// for a newer deployment should cost a caller the role it did not understand,
// not every role in the token.
func (m *mapper) rolesFrom(names []string, scope string) role {
	var have role

	for _, n := range names {
		got, ok := roleFor(n)
		if !ok {
			m.log.Warn(
				"Ignoring unrecognized role",
				tag.String("role", n),
				tag.String("scope", scope),
			)

			continue
		}

		have |= got
	}

	return have
}

// String renders a role as the names it is made of, so a log line reads
// "reader|writer" rather than "6".
func (r role) String() string {
	if r == roleUndefined {
		return "none"
	}

	names := make([]string, 0, len(roleNames))
	for _, rn := range roleNames {
		if r&rn.role != 0 {
			names = append(names, rn.name)
		}
	}

	return strings.Join(names, "|")
}

// roleFor returns the role name grants, and false if no role goes by that name.
func roleFor(name string) (role, bool) {
	for _, rn := range roleNames {
		if strings.EqualFold(name, rn.name) {
			return rn.role, true
		}
	}

	return roleUndefined, false
}
