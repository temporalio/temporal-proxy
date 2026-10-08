package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeGroup struct {
	on   bool
	auth *AuthConfig
}

func (f *fakeGroup) Validate() error         { return nil }
func (f *fakeGroup) enabled() bool           { return f.on }
func (f *fakeGroup) authConfig() *AuthConfig { return f.auth }

func TestValidateGroupsAppliesToAnyGroup(t *testing.T) {
	t.Parallel()

	token := &AuthConfig{StaticToken: &StaticTokenConfig{Token: "t"}}
	tests := []struct {
		name    string
		listen  ListenConfig
		groups  []namedGroup
		wantErr string
	}{
		{
			name:   "only disabled groups bind nothing",
			groups: []namedGroup{{name: "nexus", group: &fakeGroup{}}},
		},
		{
			name:    "second group without auth off loopback",
			listen:  ListenConfig{HostPort: ":8443"},
			groups:  []namedGroup{{name: "codecServer", group: &fakeGroup{}}, {name: "nexus", group: &fakeGroup{on: true}}},
			wantErr: "nexus: auth: auth is required unless hostPort is loopback",
		},
		{
			name:    "second group with auth but no tls off loopback",
			listen:  ListenConfig{HostPort: ":8443"},
			groups:  []namedGroup{{name: "nexus", group: &fakeGroup{on: true, auth: token}}},
			wantErr: "tls is required when auth is configured and hostPort is not loopback",
		},
		{
			name:   "loopback needs no auth",
			listen: ListenConfig{HostPort: "127.0.0.1:8443"},
			groups: []namedGroup{{name: "nexus", group: &fakeGroup{on: true}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateGroups(&tt.listen, tt.groups)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
