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
	"go.temporal.io/api/common/v1"
	"go.uber.org/fx"
	"google.golang.org/protobuf/reflect/protoregistry"

	"go.temporal.io/proxy/internal/auth"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane"
	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/protoutil"
	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/internal/transport/connect"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/pkg/codec"
	"go.temporal.io/proxy/pkg/crypto"
	"go.temporal.io/proxy/pkg/logger"
)

// testDeps are the collaborators a [dataplane.Dataplane] is built from, held as
// values so a test can replace one before testGraph supplies them to fx.
type testDeps struct {
	ctx        context.Context
	cfg        *config.Config
	extractor  *protoutil.Extractor
	translator *protoutil.Translator
	pool       *connect.Pool
	metrics    *metrics.Factory
	auth       auth.Authenticator
	vault      *crypto.Vault
	reloader   *creds.Reloader
	logger     logger.Logger
}

func TestNew(t *testing.T) {
	t.Parallel()

	cfg := testConfig()

	dp, err := newPlane(t, newTestDeps(t, cfg))
	require.NoError(t, err)
	require.NotNil(t, dp)
	require.Nil(t, dp.Addr(), "building the plane binds nothing, so there is no address yet")
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Routing.DefaultUpstream = "nope"

	_, err := newPlane(t, newTestDeps(t, cfg))
	require.ErrorContains(t, err, "nope")
}

// TestNewRejectsConfiguredKeysWithoutVault covers both configurations that
// leave keys unusable. internal/kms builds a vault from key presence rather
// than from Enabled, so turning encryption off for new traffic still needs one
// to open payloads sealed while it was on. Enabled is the only difference
// between the cases: Config.Prepare already requires a Default whenever
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
				CacheSize: new(10),
				Default: &config.KeyPolicy{
					URI:         testingKeyURL(t),
					Duration:    time.Hour,
					RenewBefore: time.Minute,
				},
				NamespaceBinding: config.NamespaceBindingStrict,
			}

			_, err := newPlane(t, newTestDeps(t, cfg))
			require.ErrorContains(t, err, "no vault was provided")
		})
	}
}

// TestNewCompressesWithConfiguredSettings shows the compression block reaches
// the chain every upstream and the codec server apply. Leaving it out of the
// codec options the plane builds fails it.
func TestNewCompressesWithConfiguredSettings(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Compression = config.Compression{
		Enabled: true,
		Default: &config.CompressionPolicy{Algorithm: config.CompressionLZ4, MinBytes: new(0)},
	}

	var codecs *proxy.Codecs
	_, err := newPlane(t, newTestDeps(t, cfg), fx.Populate(&codecs))
	require.NoError(t, err)

	payload := &common.Payload{Data: bytes.Repeat([]byte("compressible "), 100)}
	got, err := codecs.Encode(t.Context(), "orders", "orders", []*common.Payload{payload})
	require.NoError(t, err)
	require.Equal(t, "lz4", string(got[0].Metadata[codec.MetadataCompressionAlgorithm]))
}

func TestNewRejectsAMetadataLabelCollidingWithACollectorsOwn(t *testing.T) {
	t.Parallel()

	// A metadata label claiming a name a collector already declares makes the Desc
	// invalid, so registration panics. That is caught in the same place a
	// duplicate registration is, and the message has to name this cause too:
	// config cannot check it without knowing every collector's label set.
	cfg := testConfig()
	cfg.Metrics.Labels.Metadata = []config.MetricLabel{{Header: "x-method", Name: "method"}}

	deps := newTestDeps(t, cfg)
	_, err := newPlane(t, deps)
	require.ErrorContains(t, err, "metrics.labels entry colliding with a collector's own label")
}

func TestNewRejectsAFixedLabelCollidingWithACollectorsOwn(t *testing.T) {
	t.Parallel()

	// A fixed label is stamped onto the factory's registerer rather than onto a
	// collector's label names, but the collision lands in the same place: the
	// wrapped Desc is invalid, so registration panics.
	cfg := testConfig()
	cfg.Metrics.Labels.Fixed = map[string]string{"method": "gateway"}

	deps := newTestDeps(t, cfg)
	deps.metrics = metrics.New("test", promauto.With(
		metrics.WithFixedLabels(prometheus.NewRegistry(), cfg.Metrics.Labels.Fixed),
	))

	_, err := newPlane(t, deps)
	require.ErrorContains(t, err, "metrics.labels entry colliding with a collector's own label")
}

func TestNewTwiceOverOneMetricsFactoryDoesNotPanic(t *testing.T) {
	t.Parallel()

	// Reporters register Prometheus collectors, and a second registration on the
	// same registry panics. Every other test builds its own registry, so this is
	// the only place that exercises a shared one.
	//
	// Run it both with and without fixed labels, because those wrap the
	// registerer: the wrapper has to pass the Prometheus error through rather
	// than replace it with one of its own.
	tests := []struct {
		name  string
		fixed map[string]string
	}{
		{name: "a bare registerer"},
		{name: "a registerer wrapped in fixed labels", fixed: map[string]string{"region": "us-west-2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := metrics.New("test", promauto.With(
				metrics.WithFixedLabels(prometheus.NewRegistry(), tt.fixed),
			))

			first := newTestDeps(t, testConfig())
			first.metrics = f
			_, err := newPlane(t, first)
			require.NoError(t, err)

			second := newTestDeps(t, testConfig())
			second.metrics = f
			_, err = newPlane(t, second)
			require.ErrorContains(
				t,
				err,
				"registering metrics panicked",
				"a second Dataplane on one registry must fail cleanly, not panic",
			)

			var dup prometheus.AlreadyRegisteredError
			require.ErrorAs(t, err, &dup, "the Prometheus error must survive the recover, not just its text")
		})
	}
}

// TestNewWarnsWhenSkipEncodingsHasNoKeys covers a list with nothing to apply it
// to. Removing the warning fails the first case; dropping the length check fails
// the second.
func TestNewWarnsWhenSkipEncodingsHasNoKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		skip     []string
		wantWarn bool
	}{
		{name: "list without keys", skip: []string{"acme/aes-gcm"}, wantWarn: true},
		{name: "no list"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			cfg.Encryption.SkipEncodings = tc.skip

			log := logger.NewTestLogger()
			deps := newTestDeps(t, cfg)
			deps.logger = log

			_, err := newPlane(t, deps)
			require.NoError(t, err)
			require.Equal(t, tc.wantWarn, log.Contains(skipEncodingsUnusedWarning))
		})
	}
}

// testConfig is a minimal valid configuration: one gateway listener and one
// static upstream. Metrics is set explicitly rather than left to the defaults
// Config.Prepare fills; nothing in these tests serves it.
func testConfig() *config.Config {
	return &config.Config{
		Listen:  config.ListenConfig{HostPort: "127.0.0.1:0"},
		Metrics: config.Metrics{HostPort: "127.0.0.1:0", Namespace: "test"},
		Routing: config.Routing{DefaultUpstream: "primary"},
		Upstreams: config.UpstreamList{{
			Name:   "primary",
			Listen: config.ListenConfig{HostPort: "127.0.0.1:7233", Insecure: true},
		}},
	}
}

// newTestDeps builds every dependency a [dataplane.Dataplane] needs, each scoped
// to this test. Callers override a field before building the plane. Reloader is
// nil, so no credential reloads unless a test supplies one and starts it.
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
		auth:       auth.AdmitAll(),
		logger:     logger.NewTestLogger(),
	}
}

// testGraph supplies every dependency dataplane.Module needs from d. The config
// goes through config.Supply, as it does in the application, so it is prepared
// and the allowlist is derived from it.
func testGraph(d testDeps) fx.Option {
	return fx.Options(
		fx.Supply(fx.Annotate(d.ctx, fx.As(new(context.Context)))),
		config.Supply(d.cfg),
		fx.Supply(
			d.extractor,
			d.translator,
			fx.Annotate(protoregistry.GlobalTypes, fx.As(new(protoutil.Types))),
			d.pool,
			d.metrics,
			fx.Annotate(d.auth, fx.As(new(auth.Authenticator))),
			fx.Annotate(d.logger, fx.As(new(logger.Logger))),
			d.vault,
			d.reloader,
		),
	)
}

// newPlane builds a Dataplane from d through dataplane.Module, with extra
// applied last for fx.Decorate and fx.Replace. The application is never started,
// so the caller drives Start and Stop on the plane itself.
func newPlane(t *testing.T, d testDeps, extra ...fx.Option) (*dataplane.Dataplane, error) {
	t.Helper()

	var dp *dataplane.Dataplane

	app := fx.New(
		testGraph(d),
		dataplane.Module,
		fx.Populate(&dp),
		fx.NopLogger,
		fx.Options(extra...),
	)

	return dp, app.Err()
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

// cloudAPIUnusedWarning is the message the plane logs for a Cloud API override no
// upstream can use. TestLogger matches an entry's message in full, so it is
// spelled once here rather than approximated at each assertion.
const cloudAPIUnusedWarning = "apiTranslations is configured but no upstream is Temporal Cloud, so no method " +
	"will be translated"

// skipEncodingsUnusedWarning is the message the plane logs for a skip list with no
// encryption codec to act on.
const skipEncodingsUnusedWarning = "encryption.skipEncodings is set but no encryption keys are configured, so it " +
	"has no effect"

// stagingCloudAPI is an override that says something, which is what an inert
// block has to be to be worth warning about: one that says nothing is
// indistinguishable from no block at all, and describes the defaults anyway.
func stagingCloudAPI() config.APITranslations {
	return config.APITranslations{
		CloudAPI: config.CloudAPI{
			Listen: config.ListenConfig{HostPort: "saas-api.staging.tmprl.cloud:443"},
		},
	}
}

// TestNewWarnsWhenCloudAPIHasNoCloudUpstream covers a block that changes nothing.
// The Cloud API is reached only by a translation, and only a Cloud upstream
// installs one, so configuring the override without one leaves an operator
// waiting for behaviour that cannot arrive.
func TestNewWarnsWhenCloudAPIHasNoCloudUpstream(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.APITranslations = stagingCloudAPI()

	log := logger.NewTestLogger()
	deps := newTestDeps(t, cfg)
	deps.logger = log

	_, err := newPlane(t, deps)
	require.NoError(t, err)

	require.True(t, log.Contains(cloudAPIUnusedWarning),
		"a block that changes nothing must not pass unremarked")
}

// TestNewIsQuietWhenTheOverrideSaysNothing is the other side of the trigger. An
// apiTranslations block with nothing in it describes the defaults, so there is
// no override going unused and nothing to report.
func TestNewIsQuietWhenTheOverrideSaysNothing(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.APITranslations = config.APITranslations{}

	log := logger.NewTestLogger()
	deps := newTestDeps(t, cfg)
	deps.logger = log

	_, err := newPlane(t, deps)
	require.NoError(t, err)

	require.False(t, log.Contains(cloudAPIUnusedWarning))
}

// TestNewIsQuietWhenTheCloudUpstreamServesNoNamespacelessRequests covers the
// hybrid: Temporal Cloud is configured, but namespace-less requests are routed to
// a Temporal Service that serves them itself. The Cloud upstream still installs
// translation for the namespaced methods routed to it, so the override is in use.
func TestNewIsQuietWhenTheCloudUpstreamServesNoNamespacelessRequests(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.APITranslations = stagingCloudAPI()
	cfg.Upstreams = append(cfg.Upstreams, config.Upstream{
		Name:   "cloud",
		Cloud:  true,
		Listen: config.ListenConfig{HostPort: "ns.acct.tmprl.cloud:7233"},
	})
	cfg.Routing.Rules = []config.RoutingRule{{
		Upstream: "cloud",
		Match:    config.RoutingMatch{Namespace: "*.acct"},
	}}

	log := logger.NewTestLogger()
	deps := newTestDeps(t, cfg)
	deps.logger = log

	_, err := newPlane(t, deps)
	require.NoError(t, err)

	require.False(t, log.Contains(cloudAPIUnusedWarning),
		"a Cloud upstream that serves only namespaced requests still installs translation")
}

// TestNewIsQuietWhenCloudAPIHasACloudUpstream is the control: the same block with
// an upstream that can use it must not warn.
func TestNewIsQuietWhenCloudAPIHasACloudUpstream(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.APITranslations = stagingCloudAPI()
	cfg.Upstreams[0].Cloud = true

	log := logger.NewTestLogger()
	deps := newTestDeps(t, cfg)
	deps.logger = log

	_, err := newPlane(t, deps)
	require.NoError(t, err)

	require.False(t, log.Contains(cloudAPIUnusedWarning))
}
