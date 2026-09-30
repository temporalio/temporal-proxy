package codecserver_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/codecserver"
	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestNewOverrideMap(t *testing.T) {
	t.Parallel()

	policy := func(names ...string) config.Encryption {
		e := config.Encryption{Overrides: map[string]config.KeyPolicy{}}
		for _, n := range names {
			e.Overrides[n] = config.KeyPolicy{}
		}

		return e
	}

	tests := []struct {
		name    string
		cfg     *config.Config
		lookups map[string]string // what Local returns for each input
		wantErr string
	}{
		{
			name: "no overrides maps nothing",
			cfg: &config.Config{
				Upstreams: config.UpstreamList{suffixed("cloud", ".a8x72")},
			},
			lookups: map[string]string{"payments.a8x72": "payments.a8x72", "payments": "payments"},
		},
		{
			name: "suffix rule maps remote to local",
			cfg: &config.Config{
				Encryption: policy("payments"),
				Upstreams:  config.UpstreamList{suffixed("cloud", ".a8x72")},
			},
			lookups: map[string]string{
				"payments.a8x72": "payments",     // the Cloud UI's name
				"payments":       "payments",     // a local caller, passed through
				"orders.a8x72":   "orders.a8x72", // no override, passed through
			},
		},
		{
			name: "an upstream with no rules contributes nothing",
			cfg: &config.Config{
				Encryption: policy("payments"),
				Upstreams:  config.UpstreamList{{Name: "local"}},
			},
			lookups: map[string]string{"payments": "payments"},
		},
		{
			name: "two upstreams both map to the same local name",
			cfg: &config.Config{
				Encryption: policy("payments"),
				Upstreams: config.UpstreamList{
					suffixed("one", ".acct1"),
					suffixed("two", ".acct2"),
				},
			},
			lookups: map[string]string{
				"payments.acct1": "payments",
				"payments.acct2": "payments",
			},
		},
		{
			name: "an override keyed by a computed remote name is a collision",
			cfg: &config.Config{
				Encryption: policy("payments", "payments.a8x72"),
				Upstreams:  config.UpstreamList{suffixed("cloud", ".a8x72")},
			},
			wantErr: `"payments.a8x72" is both an encryption override and the remote name of "payments"`,
		},
		{
			name:    "an explicit local/remote mapping resolves the operator's remote name",
			cfg:     mustLoad(t, explicitMappingYAML),
			lookups: map[string]string{"pmts-prod": "payments"},
		},
		{
			name:    "two upstreams computing the same remote name for different locals is a collision",
			cfg:     mustLoad(t, collidingRemoteYAML),
			wantErr: `remote namespace "shared" maps to both "orders" and "payments"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := codecserver.NewOverrideMap(tc.cfg)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
			for in, want := range tc.lookups {
				require.Equal(t, want, got.Local(in), "Local(%q)", in)
			}
		})
	}
}

const (
	// explicitMappingYAML declares a NamespaceMapping override so that
	// UnmarshalYAML populates NamespaceRules' localToRemote lookup, which a
	// struct literal cannot do.
	explicitMappingYAML = `
hostPort: 127.0.0.1:7233
upstreams:
  - name: cloud
    hostPort: cloud.example.test:7233
    namespaces:
      rules:
        overrides:
          - local: payments
            remote: pmts-prod
encryption:
  overrides:
    payments:
      uri: testing://a2V5
`

	// collidingRemoteYAML has two upstreams whose explicit overrides compute the
	// same remote name ("shared") for two different local names ("orders" and
	// "payments"), both carrying an encryption policy. The first collision
	// branch in NewOverrideMap does not catch this: neither local name equals
	// the other upstream's remote name, so only the second branch, comparing
	// two remote-name computations against each other, can.
	collidingRemoteYAML = `
hostPort: 127.0.0.1:7233
upstreams:
  - name: one
    hostPort: one.example.test:7233
    namespaces:
      rules:
        overrides:
          - local: orders
            remote: shared
  - name: two
    hostPort: two.example.test:7233
    namespaces:
      rules:
        overrides:
          - local: payments
            remote: shared
encryption:
  overrides:
    orders:
      uri: testing://a2V5
    payments:
      uri: testing://a2V5
`
)

// mustLoad parses yaml into a *config.Config, failing the test on error.
func mustLoad(t *testing.T, yaml string) *config.Config {
	t.Helper()

	cfg, err := config.Load(strings.NewReader(yaml))
	require.NoError(t, err)

	return cfg
}

// suffixed returns an upstream whose namespace rules append suffix, which is the
// canonical Temporal Cloud shape.
func suffixed(name, suffix string) config.Upstream {
	up := config.Upstream{Name: name}
	up.Namespaces.Rules.Suffix = suffix

	return up
}
