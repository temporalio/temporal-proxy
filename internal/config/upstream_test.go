package config_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

// namespaceConfigYAML wraps a `namespaces:` body (each line indented to sit
// under a single named upstream) so Load produces one upstream carrying those
// translation rules.
func namespaceConfigYAML(namespacesBody string) string {
	return "upstreams:\n  - name: primary\n    namespaces:\n" + namespacesBody
}

func TestNamespaceRules_Local(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		yaml     string
		remoteNS string
		want     string
	}{
		{
			name:     "no prefix, suffix, or override returns input unchanged",
			yaml:     namespaceConfigYAML("      rules: {}\n"),
			remoteNS: "payments",
			want:     "payments",
		},
		{
			name:     "strips configured prefix",
			yaml:     namespaceConfigYAML("      rules:\n        prefix: \"acme-\"\n"),
			remoteNS: "acme-payments",
			want:     "payments",
		},
		{
			name:     "strips configured suffix",
			yaml:     namespaceConfigYAML("      rules:\n        suffix: \".cloud\"\n"),
			remoteNS: "payments.cloud",
			want:     "payments",
		},
		{
			name:     "strips both prefix and suffix",
			yaml:     namespaceConfigYAML("      rules:\n        prefix: \"acme-\"\n        suffix: \".cloud\"\n"),
			remoteNS: "acme-payments.cloud",
			want:     "payments",
		},
		{
			name: "override wins over prefix/suffix",
			yaml: namespaceConfigYAML("      rules:\n" +
				"        prefix: \"acme-\"\n" +
				"        suffix: \".cloud\"\n" +
				"        overrides:\n" +
				"          - local: billing\n" +
				"            remote: legacy-billing-prod\n"),
			remoteNS: "legacy-billing-prod",
			want:     "billing",
		},
		{
			name: "no override match falls back to prefix/suffix stripping",
			yaml: namespaceConfigYAML("      rules:\n" +
				"        prefix: \"acme-\"\n" +
				"        overrides:\n" +
				"          - local: billing\n" +
				"            remote: legacy-billing\n"),
			remoteNS: "acme-payments",
			want:     "payments",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := config.Load(strings.NewReader(tc.yaml))
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Upstreams[0].Namespaces.Rules.Local(tc.remoteNS))
		})
	}
}

func TestNamespaceRules_Remote(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		yaml    string
		localNS string
		want    string
	}{
		{
			name:    "no prefix, suffix, or override returns input unchanged",
			yaml:    namespaceConfigYAML("      rules: {}\n"),
			localNS: "payments",
			want:    "payments",
		},
		{
			name:    "applies configured prefix",
			yaml:    namespaceConfigYAML("      rules:\n        prefix: \"acme-\"\n"),
			localNS: "payments",
			want:    "acme-payments",
		},
		{
			name:    "applies configured suffix",
			yaml:    namespaceConfigYAML("      rules:\n        suffix: \".cloud\"\n"),
			localNS: "payments",
			want:    "payments.cloud",
		},
		{
			name:    "applies both prefix and suffix",
			yaml:    namespaceConfigYAML("      rules:\n        prefix: \"acme-\"\n        suffix: \".cloud\"\n"),
			localNS: "payments",
			want:    "acme-payments.cloud",
		},
		{
			name: "override wins over prefix/suffix",
			yaml: namespaceConfigYAML("      rules:\n" +
				"        prefix: \"acme-\"\n" +
				"        suffix: \".cloud\"\n" +
				"        overrides:\n" +
				"          - local: billing\n" +
				"            remote: legacy-billing-prod\n"),
			localNS: "billing",
			want:    "legacy-billing-prod",
		},
		{
			name: "no override match falls back to prefix/suffix wrapping",
			yaml: namespaceConfigYAML("      rules:\n" +
				"        prefix: \"acme-\"\n" +
				"        overrides:\n" +
				"          - local: billing\n" +
				"            remote: legacy-billing\n"),
			localNS: "payments",
			want:    "acme-payments",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := config.Load(strings.NewReader(tc.yaml))
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Upstreams[0].Namespaces.Rules.Remote(tc.localNS))
		})
	}
}

func TestNamespaceMapping_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mapping  *config.NamespaceMapping
		wantErrs []validation.Error
	}{
		{
			name:    "both local and remote set",
			mapping: &config.NamespaceMapping{Local: "billing", Remote: "legacy-billing"},
		},
		{
			name:    "missing local",
			mapping: &config.NamespaceMapping{Remote: "legacy-billing"},
			wantErrs: []validation.Error{
				{Field: "local", Message: "is required"},
			},
		},
		{
			name:    "missing remote",
			mapping: &config.NamespaceMapping{Local: "billing"},
			wantErrs: []validation.Error{
				{Field: "remote", Message: "is required"},
			},
		},
		{
			name:    "both missing",
			mapping: &config.NamespaceMapping{},
			wantErrs: []validation.Error{
				{Field: "local", Message: "is required"},
				{Field: "remote", Message: "is required"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.mapping.Validate()
			if len(tt.wantErrs) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)
			require.ElementsMatch(t, tt.wantErrs, []validation.Error(errs))
		})
	}
}

func TestNamespaceRules_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rules      *config.NamespaceRules
		wantTuples [][2]string // (Subject, Field); empty slice means no error expected
	}{
		{
			name:  "no overrides yields no error",
			rules: &config.NamespaceRules{Prefix: "acme-", Suffix: ".cloud"},
		},
		{
			name: "valid overrides yield no error",
			rules: &config.NamespaceRules{
				Overrides: []config.NamespaceMapping{
					{Local: "billing", Remote: "legacy-billing"},
					{Local: "payments", Remote: "acme-payments"},
				},
			},
		},
		{
			name: "single invalid override stamped with its index",
			rules: &config.NamespaceRules{
				Overrides: []config.NamespaceMapping{
					{Remote: "legacy-billing"},
				},
			},
			wantTuples: [][2]string{
				{"overrides[0]", "local"},
			},
		},
		{
			name: "invalid overrides keep their own indices",
			rules: &config.NamespaceRules{
				Overrides: []config.NamespaceMapping{
					{Local: "billing", Remote: "legacy-billing"},
					{Local: "payments"},
					{Remote: "legacy-orders"},
				},
			},
			wantTuples: [][2]string{
				{"overrides[1]", "remote"},
				{"overrides[2]", "local"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.rules.Validate()
			if len(tt.wantTuples) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)

			got := make([][2]string, len(errs))
			for i, e := range errs {
				got[i] = [2]string{e.Subject, e.Field}
			}

			require.ElementsMatch(t, tt.wantTuples, got)
		})
	}
}

func TestNamespaceRules_Validate_Uniqueness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rules    *config.NamespaceRules
		wantErrs []validation.Error
	}{
		{
			name: "duplicate local names reported on overrides field",
			rules: &config.NamespaceRules{
				Overrides: []config.NamespaceMapping{
					{Local: "billing", Remote: "legacy-billing"},
					{Local: "billing", Remote: "legacy-payments"},
				},
			},
			wantErrs: []validation.Error{
				{Field: "overrides[local]", Message: "contains duplicate value: billing"},
			},
		},
		{
			name: "duplicate remote names reported on overrides field",
			rules: &config.NamespaceRules{
				Overrides: []config.NamespaceMapping{
					{Local: "billing", Remote: "legacy"},
					{Local: "payments", Remote: "legacy"},
				},
			},
			wantErrs: []validation.Error{
				{Field: "overrides[remote]", Message: "contains duplicate value: legacy"},
			},
		},
		{
			name: "duplicate locals and remotes both reported",
			rules: &config.NamespaceRules{
				Overrides: []config.NamespaceMapping{
					{Local: "billing", Remote: "legacy"},
					{Local: "billing", Remote: "legacy"},
				},
			},
			wantErrs: []validation.Error{
				{Field: "overrides[local]", Message: "contains duplicate value: billing"},
				{Field: "overrides[remote]", Message: "contains duplicate value: legacy"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.rules.Validate()
			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)
			require.ElementsMatch(t, tt.wantErrs, []validation.Error(errs))
		})
	}
}

func TestNamespaceConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        *config.NamespaceConfig
		wantTuples [][2]string
	}{
		{
			name: "empty config yields no error",
			cfg:  &config.NamespaceConfig{},
		},
		{
			name: "rules failure surfaces with rules path prefixed onto the subject",
			cfg: &config.NamespaceConfig{
				Rules: config.NamespaceRules{
					Overrides: []config.NamespaceMapping{
						{Local: "billing"},
					},
				},
			},
			wantTuples: [][2]string{
				{"rules.overrides[0]", "remote"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.wantTuples) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)

			got := make([][2]string, len(errs))
			for i, e := range errs {
				got[i] = [2]string{e.Subject, e.Field}
			}

			require.ElementsMatch(t, tt.wantTuples, got)
		})
	}
}

func TestUpstream_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		upstream   *config.Upstream
		wantTuples [][2]string
	}{
		{
			name: "valid name and listen, no overrides",
			upstream: &config.Upstream{
				Name:   "primary",
				Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"},
			},
		},
		{
			name: "missing name surfaces required error",
			upstream: &config.Upstream{
				Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"},
			},
			wantTuples: [][2]string{
				{"", "name"},
			},
		},
		{
			name: "templated hostPort is accepted",
			upstream: &config.Upstream{
				Name:   "primary",
				Listen: config.ListenConfig{HostPort: "{{ .RemoteNamespace }}.acme-cloud.tmprl.cloud:7233"},
			},
		},
		{
			name: "invalid static hostPort surfaces from Listen",
			upstream: &config.Upstream{
				Name:   "primary",
				Listen: config.ListenConfig{HostPort: "not-a-host-port"},
			},
			wantTuples: [][2]string{
				{"", "hostPort"},
			},
		},
		{
			name: "namespace override failure surfaces with the full namespaces path",
			upstream: &config.Upstream{
				Name:   "primary",
				Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"},
				Namespaces: config.NamespaceConfig{
					Rules: config.NamespaceRules{
						Overrides: []config.NamespaceMapping{
							{Local: "billing"},
						},
					},
				},
			},
			wantTuples: [][2]string{
				{"namespaces.rules.overrides[0]", "remote"},
			},
		},
		{
			name: "listen and namespace failures aggregate",
			upstream: &config.Upstream{
				Name:   "primary",
				Listen: config.ListenConfig{HostPort: "not-a-host-port"},
				Namespaces: config.NamespaceConfig{
					Rules: config.NamespaceRules{
						Overrides: []config.NamespaceMapping{
							{},
						},
					},
				},
			},
			wantTuples: [][2]string{
				{"", "hostPort"},
				{"namespaces.rules.overrides[0]", "local"},
				{"namespaces.rules.overrides[0]", "remote"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.upstream.Validate()
			if len(tt.wantTuples) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)

			got := make([][2]string, len(errs))
			for i, e := range errs {
				got[i] = [2]string{e.Subject, e.Field}
			}

			require.ElementsMatch(t, tt.wantTuples, got)
		})
	}
}

func TestUpstreamCredentialsRequireTLS(t *testing.T) {
	t.Parallel()

	base := func() config.Upstream {
		return config.Upstream{
			Name:        "u",
			Listen:      config.ListenConfig{HostPort: "host:7233"},
			Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "k"}},
		}
	}

	t.Run("credentials without tls is rejected", func(t *testing.T) {
		t.Parallel()
		u := base()
		require.ErrorContains(t, u.Validate(), "requires TLS")
	})

	t.Run("credentials with tls is accepted", func(t *testing.T) {
		t.Parallel()
		u := base()
		u.Listen.TLS = &config.TLSConfig{ServerName: "my-ns.acct.tmprl.cloud"}
		require.NoError(t, u.Validate())
	})
}

func TestUpstreamOutboundTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tls     func(t *testing.T) *config.TLSConfig
		wantErr string
	}{
		{
			name: "server name only is valid (cloud + api key)",
			tls: func(*testing.T) *config.TLSConfig {
				return &config.TLSConfig{ServerName: "my-ns.acct.tmprl.cloud"}
			},
		},
		{
			name: "CA only is valid (self-signed + api key)",
			tls: func(t *testing.T) *config.TLSConfig {
				caFile, _, _ := testutil.GenerateMTLSCerts(t)
				return &config.TLSConfig{CA: caFile, ServerName: "localhost"}
			},
		},
		{
			name: "CA plus client key pair is valid (mutual TLS)",
			tls: func(t *testing.T) *config.TLSConfig {
				caFile, certFile, keyFile := testutil.GenerateMTLSCerts(t)
				return &config.TLSConfig{CA: caFile, Cert: certFile, Key: keyFile, ServerName: "localhost"}
			},
		},
		{
			name: "self-signed leaf cert is accepted as a trust anchor (pinning)",
			// A peer proxy may present a plain self-signed leaf (IsCA=false).
			// Pinning it as the trust anchor is valid TLS, so it must validate.
			tls: func(t *testing.T) *config.TLSConfig {
				leafCert, _ := testutil.GenerateSelfSignedCert(t)
				return &config.TLSConfig{CA: leafCert, ServerName: "localhost"}
			},
		},
		{
			name: "cert without key is rejected",
			tls: func(t *testing.T) *config.TLSConfig {
				_, certFile, _ := testutil.GenerateMTLSCerts(t)
				return &config.TLSConfig{Cert: certFile}
			},
			wantErr: "cert and key must be set together",
		},
		{
			name: "key without cert is rejected",
			tls: func(t *testing.T) *config.TLSConfig {
				_, _, keyFile := testutil.GenerateMTLSCerts(t)
				return &config.TLSConfig{Key: keyFile}
			},
			wantErr: "cert and key must be set together",
		},
		{
			name: "client cert without CA is rejected",
			tls: func(t *testing.T) *config.TLSConfig {
				_, certFile, keyFile := testutil.GenerateMTLSCerts(t)
				return &config.TLSConfig{Cert: certFile, Key: keyFile}
			},
			wantErr: "required when a client certificate is set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			u := config.Upstream{
				Name:   "u",
				Listen: config.ListenConfig{HostPort: "host:7233", TLS: tt.tls(t)},
			}

			err := u.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestNamespaceRulesConfigured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		rules config.NamespaceRules
		want  bool
	}{
		{"empty", config.NamespaceRules{}, false},
		{"prefix set", config.NamespaceRules{Prefix: "acme-"}, true},
		{"suffix set", config.NamespaceRules{Suffix: "-prod"}, true},
		{"override set", config.NamespaceRules{Overrides: []config.NamespaceMapping{{Local: "a", Remote: "b"}}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.rules.Configured())
		})
	}
}
