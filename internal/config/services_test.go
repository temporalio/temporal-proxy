package config_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	// Blank import guarantees a registered service that the proxy does not
	// forward, so the "registered but not forwardable" branch is exercised
	// against a real descriptor rather than one that happens to arrive through
	// a transitive dependency.
	_ "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/services"
)

func TestServices_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		svcs       config.Services
		wantTuples [][2]string
		wantMsgs   []string
	}{
		{
			name: "nil list yields no error",
			svcs: nil,
		},
		{
			name: "empty list yields no error",
			svcs: config.Services{},
		},
		{
			name: "the default set is forwardable",
			svcs: config.Services(services.Default()),
		},
		{
			name: "every known service is forwardable",
			svcs: config.Services(services.Known()),
		},
		{
			name: "the reflection alias is forwardable on its own",
			svcs: config.Services{services.ReflectionV1Alpha},
		},
		{
			name:       "a duplicate entry is rejected",
			svcs:       config.Services{services.WorkflowService, services.WorkflowService},
			wantTuples: [][2]string{{"", "allowedServices"}},
			wantMsgs:   []string{"contains duplicate value"},
		},
		{
			name:       "an unregistered name is rejected as a typo",
			svcs:       config.Services{"temporal.api.workflowservice.v1.WorkflowServ"},
			wantTuples: [][2]string{{"", "allowedServices"}},
			wantMsgs:   []string{`"temporal.api.workflowservice.v1.WorkflowServ" is not registered`},
		},
		{
			name:       "a registered name that is not a service is rejected",
			svcs:       config.Services{"temporal.api.workflowservice.v1.StartWorkflowExecutionRequest"},
			wantTuples: [][2]string{{"", "allowedServices"}},
			wantMsgs:   []string{"is not a service"},
		},
		{
			name:       "a registered service the proxy does not forward is rejected",
			svcs:       config.Services{"grpc.health.v1.Health"},
			wantTuples: [][2]string{{"", "allowedServices"}},
			wantMsgs:   []string{`"grpc.health.v1.Health" is registered but not forwardable`},
		},
		{
			name: "every bad name is reported in one pass",
			svcs: config.Services{"not.A.Thing", "grpc.health.v1.Health", services.WorkflowService},
			wantTuples: [][2]string{
				{"", "allowedServices"},
				{"", "allowedServices"},
			},
			wantMsgs: []string{"not.A.Thing", "grpc.health.v1.Health"},
		},
		{
			// Uniqueness stops at the first duplicate, but resolution does not
			// short-circuit, so a name that is both duplicated and unknown is
			// reported once for the duplication and once per occurrence.
			name: "a duplicate and an unknown name aggregate",
			svcs: config.Services{"not.A.Thing", "not.A.Thing"},
			wantTuples: [][2]string{
				{"", "allowedServices"},
				{"", "allowedServices"},
				{"", "allowedServices"},
			},
			wantMsgs: []string{"contains duplicate value", "is not registered"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svcs.Validate()
			assertTuples(t, err, tt.wantTuples)

			for _, msg := range tt.wantMsgs {
				require.ErrorContains(t, err, msg)
			}
		})
	}
}

func TestLoad_AllowedServicesDefaults(t *testing.T) {
	t.Parallel()

	// Every spelling of "I did not choose" resolves to the default set. The
	// absent-key case is the one that matters most: it is how nearly every
	// config is written, and it is the case a Services unmarshaler could not
	// have covered, since the decoder never calls one for a key that is not
	// there.
	tests := []struct {
		name string
		yaml string
		want config.Services
	}{
		{
			name: "absent key gets the default set",
			yaml: `hostPort: ":8080"`,
			want: config.Services(services.Default()),
		},
		{
			name: "explicit null gets the default set",
			yaml: "hostPort: \":8080\"\nallowedServices:",
			want: config.Services(services.Default()),
		},
		{
			name: "explicit empty list gets the default set",
			yaml: "hostPort: \":8080\"\nallowedServices: []",
			want: config.Services(services.Default()),
		},
		{
			name: "an explicit list is left alone",
			yaml: "hostPort: \":8080\"\nallowedServices:\n  - " + services.Reflection,
			want: config.Services{services.Reflection},
		},
		{
			name: "an explicit list is not merged with the default set",
			yaml: "hostPort: \":8080\"\nallowedServices:\n  - " + services.WorkflowService,
			want: config.Services{services.WorkflowService},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := config.Load(strings.NewReader(tt.yaml))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.AllowedServices)
		})
	}
}

func TestConfig_Validate_AllowedServices(t *testing.T) {
	t.Parallel()

	base := func(svcs config.Services) *config.Config {
		return &config.Config{
			Listen:          config.ListenConfig{HostPort: ":8080"},
			AllowedServices: svcs,
			Upstreams: config.UpstreamList{
				{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
			},
		}
	}

	tests := []struct {
		name       string
		svcs       config.Services
		wantTuples [][2]string
	}{
		{
			name: "an unset allowlist is optional",
			svcs: nil,
		},
		{
			name: "the default set yields no error",
			svcs: config.Services(services.Default()),
		},
		{
			// Services owns the field name, so Config nests it with an empty
			// subject and the path reads "allowedServices" rather than
			// repeating itself.
			name:       "a bad name surfaces on the bare allowedServices field",
			svcs:       config.Services{"not.A.Thing"},
			wantTuples: [][2]string{{"", "allowedServices"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertTuples(t, base(tt.svcs).Validate(), tt.wantTuples)
		})
	}
}

func TestServices_ValidateNamesTheForwardableSet(t *testing.T) {
	t.Parallel()

	// The fix for a bad name is only obvious if the error says what the legal
	// names are, so every failure carries the forwardable set.
	svcs := config.Services{"not.A.Thing"}

	err := svcs.Validate()
	require.Error(t, err)
	for _, name := range services.Known() {
		require.ErrorContains(t, err, name)
	}
}
