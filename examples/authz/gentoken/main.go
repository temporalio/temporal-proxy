// Command gentoken mints one signed token for the authorization example and
// prints it on stdout, so a shell can capture it directly:
//
//	AUTHZ_TOKEN=$(go run ./gentoken -ns 'default=reader,writer')
//
// It signs with AUTHZ_JWT_SECRET, the same secret the extension server verifies
// against, and stands in for the identity provider that would mint these in a
// real deployment. It authenticates nobody: anyone who can run it can mint a
// token granting anything.
//
// Roles are worker, reader, writer, and admin. Nothing here checks them, on
// purpose: the extension server skips a role name it does not recognize and logs
// that it did, and watching it do so is easier if this command mints a misspelled
// role without complaint.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/temporalio/temporal-proxy/examples/authz"
)

// namespaceRoles collects repeated -ns flags, one per namespace.
type namespaceRoles map[string][]string

func main() {
	namespaces := namespaceRoles{}

	sub := flag.String("sub", "acme-worker", "token subject")
	system := flag.String("system", "", "comma-separated roles held in every namespace")
	ttl := flag.Duration("ttl", time.Hour, "how long the token stays valid")
	body := flag.Bool("claims", false, "also print the token body to stderr")
	flag.Var(namespaces, "ns", `roles held in one namespace, as "namespace=role[,role]"; repeatable`)
	flag.Parse()

	secret := os.Getenv("AUTHZ_JWT_SECRET")
	if secret == "" {
		log.Fatal("AUTHZ_JWT_SECRET is required")
	}

	now := time.Now()
	claims := &authz.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    authz.Issuer,
			Subject:   *sub,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(*ttl)),
		},
		Permissions: &authz.Permissions{
			System:     splitRoles(*system),
			Namespaces: namespaces,
		},
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		log.Fatalf("sign token: %v", err)
	}

	if *body {
		pretty, err := json.MarshalIndent(claims, "", "  ")
		if err != nil {
			log.Fatalf("render claims: %v", err)
		}

		fmt.Fprintln(os.Stderr, string(pretty))
	}

	fmt.Println(token)
}

// String reports the namespaces collected so far, for flag's own error messages.
func (r namespaceRoles) String() string {
	pairs := make([]string, 0, len(r))
	for ns, roles := range r {
		pairs = append(pairs, ns+"="+strings.Join(roles, ","))
	}

	return strings.Join(pairs, " ")
}

// Set records one namespace=role[,role] pair. A repeated namespace replaces the
// roles recorded for it rather than adding to them, since a caller writing the
// flag twice for one namespace more likely made a mistake than meant a union.
func (r namespaceRoles) Set(v string) error {
	ns, roles, ok := strings.Cut(v, "=")
	if !ok || ns == "" || roles == "" {
		return fmt.Errorf("want namespace=role[,role], got %q", v)
	}

	r[ns] = splitRoles(roles)

	return nil
}

// splitRoles splits a comma-separated role list, returning nil for an empty one
// so the claim omits the scope entirely rather than carrying an empty list.
func splitRoles(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Split(s, ",")
}
