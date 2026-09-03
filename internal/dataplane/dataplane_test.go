package dataplane_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// testDeps are the collaborators a [dataplane.Dataplane] is built from, held as
// values so both construction paths this package tests draw on one source: the
// options [dataplane.New] takes, and what testGraph supplies to fx.
type testDeps struct {
	ctx        context.Context
	cfg        *config.Config
	extractor  *protoutil.Extractor
	translator *protoutil.Translator
	pool       *connect.Pool
	metrics    *metrics.Factory
	allowlist  services.Allowlist
	auth       auth.Authenticator
	vault      *crypto.Vault
	logger     logger.Logger
}

func TestNew(t *testing.T) {
	t.Parallel()

	cfg := testConfig()

	dp, err := dataplane.New(t.Context(), cfg, newTestDeps(t, cfg).opts()...)
	require.NoError(t, err)
	require.NotNil(t, dp)
	require.Nil(t, dp.Addr(), "New binds nothing, so there is no address yet")
}

func TestNewRejectsMissingContextOrConfig(t *testing.T) {
	t.Parallel()

	t.Run("ctx", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig()

		// A typed nil rather than an untyped one: staticcheck rejects passing a
		// literal nil where a context is expected, which is the mistake under test.
		var ctx context.Context

		_, err := dataplane.New(ctx, cfg, newTestDeps(t, cfg).opts()...)
		require.ErrorContains(t, err, "ctx is required")
	})

	t.Run("cfg", func(t *testing.T) {
		t.Parallel()

		_, err := dataplane.New(t.Context(), nil, newTestDeps(t, testConfig()).opts()...)
		require.ErrorContains(t, err, "cfg is required")
	})
}

func TestNewRejectsMissingRequiredOption(t *testing.T) {
	t.Parallel()

	// The names New reports, which double as the required set: every option here
	// must be supplied even though the signature cannot enforce it.
	for _, name := range []string{
		"WithExtractor",
		"WithTranslator",
		"WithPool",
		"WithMetrics",
		"WithAllowlist",
		"WithAuth",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()

			_, err := dataplane.New(t.Context(), cfg, newTestDeps(t, cfg).opts(name)...)
			require.ErrorContains(t, err, name)
		})
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Routing.DefaultUpstream = "nope"

	_, err := dataplane.New(t.Context(), cfg, newTestDeps(t, cfg).opts()...)
	require.ErrorContains(t, err, "nope")
}

// TestNewRejectsConfiguredKeysWithoutVault covers both configurations that
// leave keys unusable. internal/kms builds a vault from key presence rather
// than from Enabled, so turning encryption off for new traffic still needs one
// to open payloads sealed while it was on. Enabled is the only difference
// between the cases: Config.Validate already requires a Default whenever
// Enabled, so keys are what the guard actually turns on.
func TestNewRejectsConfiguredKeysWithoutVault(t *testing.T) {
	t.Parallel()

	for name, enabled := range map[string]bool{
		"enabled":                  true,
		"disabled but still keyed": false,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			cfg.Encryption = config.Encryption{
				Enabled:   enabled,
				CacheSize: 10,
				Default: &config.KeyPolicy{
					URI:         testingKeyURL(t),
					Duration:    time.Hour,
					RenewBefore: time.Minute,
				},
			}

			_, err := dataplane.New(t.Context(), cfg, newTestDeps(t, cfg).opts()...)
			require.ErrorContains(t, err, "no vault was provided")
		})
	}
}

func TestNewRejectsATagLabelCollidingWithACollectorsOwn(t *testing.T) {
	t.Parallel()

	// A tag claiming a label a collector already declares makes the Desc
	// invalid, so registration panics. That is caught in the same place a
	// duplicate registration is, and the message has to name this cause too:
	// config cannot check it without knowing every collector's label set.
	cfg := testConfig()
	cfg.Metrics.Tags = []config.MetricTag{{Header: "x-method", Label: "method"}}

	deps := newTestDeps(t, cfg)
	_, err := dataplane.New(deps.ctx, deps.cfg, deps.opts()...)
	require.ErrorContains(t, err, "metrics tag label colliding with a collector's own")
}

func TestNewTwiceOverOneMetricsFactoryDoesNotPanic(t *testing.T) {
	t.Parallel()

	// Reporters register Prometheus collectors, and a second registration on the
	// same registry panics. Every other test builds its own registry, so this is
	// the only place that exercises a shared one.
	f := metrics.New("test", promauto.With(prometheus.NewRegistry()))

	first := newTestDeps(t, testConfig())
	first.metrics = f
	_, err := dataplane.New(first.ctx, first.cfg, first.opts()...)
	require.NoError(t, err)

	second := newTestDeps(t, testConfig())
	second.metrics = f
	_, err = dataplane.New(second.ctx, second.cfg, second.opts()...)
	require.ErrorContains(
		t,
		err,
		"registering metrics panicked",
		"a second Dataplane on one registry must fail cleanly, not panic",
	)

	var dup prometheus.AlreadyRegisteredError
	require.ErrorAs(t, err, &dup, "the Prometheus error must survive the recover, not just its text")
}

// opts returns d as the options [dataplane.New] takes, less any named in omit,
// so a caller can prove New reports one as missing. Names are the ones New
// reports.
func (d testDeps) opts(omit ...string) []dataplane.Option {
	all := map[string]dataplane.Option{
		"WithExtractor":  dataplane.WithExtractor(d.extractor),
		"WithTranslator": dataplane.WithTranslator(d.translator),
		"WithPool":       dataplane.WithPool(d.pool),
		"WithMetrics":    dataplane.WithMetrics(d.metrics),
		"WithAllowlist":  dataplane.WithAllowlist(d.allowlist),
		"WithAuth":       dataplane.WithAuth(d.auth),
		"WithVault":      dataplane.WithVault(d.vault),
		"WithLogger":     dataplane.WithLogger(d.logger),
	}
	for _, name := range omit {
		delete(all, name)
	}

	opts := make([]dataplane.Option, 0, len(all))
	for _, opt := range all {
		opts = append(opts, opt)
	}

	return opts
}

// testConfig is a minimal valid configuration: one gateway listener and one
// static upstream. Metrics is populated because Config.Validate requires it;
// nothing in these tests serves it.
func testConfig() *config.Config {
	return &config.Config{
		Listen:  config.ListenConfig{HostPort: "127.0.0.1:0"},
		Metrics: config.Metrics{HostPort: "127.0.0.1:0", Namespace: "test"},
		Routing: config.Routing{DefaultUpstream: "primary"},
		Upstreams: config.UpstreamList{{
			Name:   "primary",
			Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"},
		}},
	}
}

// newTestDeps builds every dependency a [dataplane.Dataplane] needs, each scoped
// to this test. Callers override a field before calling opts, and reach for
// append only for an option no construction path shares.
func newTestDeps(t *testing.T, cfg *config.Config) testDeps {
	t.Helper()

	pool := connect.NewPool()
	t.Cleanup(func() { _ = pool.Close() })

	return testDeps{
		ctx:        t.Context(),
		cfg:        cfg,
		extractor:  protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes),
		translator: protoutil.NewTranslator(protoregistry.GlobalFiles),
		pool:       pool,
		metrics:    metrics.New("test", promauto.With(prometheus.NewRegistry())),
		allowlist:  config.NewAllowlist(cfg),
		auth:       auth.AdmitAll(),
		logger:     logger.NewTestLogger(),
	}
}

// testingKeyURL builds a key URI the config validator accepts without reaching
// a real KMS.
func testingKeyURL(t *testing.T) url.URL {
	t.Helper()

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'a'}, 32))
	u, err := url.Parse("testing://" + key)
	require.NoError(t, err)

	return *u
}
