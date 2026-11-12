package app_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.temporal.io/proxy/internal/app"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/pkg/logger"
)

const lifecycleTimeout = 10 * time.Second

func TestNewStartsAndStops(t *testing.T) {
	t.Parallel()

	a := app.New(testInputs(t, testConfig(t)))
	require.NoError(t, a.Err())

	ctx, cancel := context.WithTimeout(t.Context(), lifecycleTimeout)
	defer cancel()

	require.NoError(t, a.Start(ctx))
	require.NoError(t, a.Stop(ctx))
}

func TestNewRequireEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      bool
		required bool
		wantErr  bool
	}{
		{name: "no key, not required"},
		{name: "no key, required", required: true, wantErr: true},
		{name: "key, required", key: true, required: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			if tt.key {
				cfg.Encryption = config.Encryption{
					Enabled:          true,
					NamespaceBinding: config.NamespaceBindingStrict,
					Default:          &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
				}
			}

			in := testInputs(t, cfg)
			in.RequireEncryption = tt.required

			err := app.New(in).Err()
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestNewWarmsTranslatorServices(t *testing.T) {
	t.Parallel()

	a := app.New(testInputs(t, testConfig(t)), fx.Decorate(func([]protoreflect.FullName) []protoreflect.FullName {
		return []protoreflect.FullName{"no.such.Service"}
	}))

	require.ErrorContains(t, a.Err(), "failed to set up translations for no.such.Service")
}

// TestNewServesWhatItRegisters proves the Registerer collectors use is the
// registry /metrics serves, for a private registry and for the default one. It
// is not parallel: the default row swaps the package-level default registry for
// a fresh one, so repeated runs never collide on a duplicate registration.
func TestNewServesWhatItRegisters(t *testing.T) {
	tests := []struct {
		name     string
		registry func(t *testing.T) *prometheus.Registry
	}{
		{name: "private", registry: func(*testing.T) *prometheus.Registry { return prometheus.NewRegistry() }},
		{name: "default", registry: func(t *testing.T) *prometheus.Registry {
			fresh := prometheus.NewRegistry()
			prevReg, prevGath := prometheus.DefaultRegisterer, prometheus.DefaultGatherer
			prometheus.DefaultRegisterer, prometheus.DefaultGatherer = fresh, fresh
			t.Cleanup(func() { prometheus.DefaultRegisterer, prometheus.DefaultGatherer = prevReg, prevGath })

			return nil
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := testInputs(t, testConfig(t))
			in.Registry = tt.registry(t)

			var reg prometheus.Registerer
			var svr *metrics.Server
			a := app.New(in, fx.Populate(&reg, &svr))
			require.NoError(t, a.Err())

			ctx, cancel := context.WithTimeout(t.Context(), lifecycleTimeout)
			defer cancel()
			require.NoError(t, a.Start(ctx))
			t.Cleanup(func() { require.NoError(t, a.Stop(context.WithoutCancel(t.Context()))) })

			c := prometheus.NewCounter(prometheus.CounterOpts{Name: "app_test_pairing_total", Help: "pairing probe"})
			require.NoError(t, reg.Register(c))
			c.Inc()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+svr.Addr().String()+"/metrics", nil)
			require.NoError(t, err)

			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer func() { _ = res.Body.Close() }()

			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), "app_test_pairing_total 1")
		})
	}
}

func TestNewLogsLifecycleFailures(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	in := testInputs(t, testConfig(t))
	in.Logger = log

	a := app.New(in, fx.Invoke(func(lc fx.Lifecycle) {
		lc.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("boom") }})
	}))
	require.NoError(t, a.Err())

	ctx, cancel := context.WithTimeout(t.Context(), lifecycleTimeout)
	defer cancel()

	require.Error(t, a.Start(ctx))
	require.True(t, log.Contains("Failed to start"))
}

func TestNewRejectsMissingInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		clear func(*app.Inputs)
		want  []string
	}{
		{
			name:  "context",
			clear: func(in *app.Inputs) { in.Context = nil },
			want:  []string{"app.Inputs: Context: is required"},
		},
		{
			name:  "logger",
			clear: func(in *app.Inputs) { in.Logger = nil },
			want:  []string{"app.Inputs: Logger: is required"},
		},
		{
			name:  "config",
			clear: func(in *app.Inputs) { in.Config = nil },
			want:  []string{"app.Inputs: Config: is required"},
		},
		{
			name:  "all",
			clear: func(in *app.Inputs) { *in = app.Inputs{} },
			want: []string{
				"app.Inputs: Context: is required",
				"app.Inputs: Logger: is required",
				"app.Inputs: Config: is required",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := testInputs(t, testConfig(t))
			tt.clear(&in)

			err := app.New(in).Err()
			for _, msg := range tt.want {
				require.ErrorContains(t, err, msg)
			}
		})
	}
}

// testConfig is a minimal valid config whose listeners are all ephemeral.
func testConfig(t *testing.T) *config.Config {
	t.Helper()

	cfg := dataplanetest.Config(dataplanetest.NewUpstream(t))
	cfg.Metrics.HostPort = "127.0.0.1:0"

	return cfg
}

// testInputs returns Inputs for a private graph that is safe to build in
// parallel with others.
func testInputs(t *testing.T, cfg *config.Config) app.Inputs {
	t.Helper()

	return app.Inputs{
		Context:  t.Context(),
		Logger:   logger.NewNoopLogger(),
		Config:   config.Supply(cfg),
		Registry: prometheus.NewRegistry(),
	}
}

// testingKeyURI is a local testing:// key URI, which the kms module serves
// without any cloud KMS.
func testingKeyURI(t *testing.T) url.URL {
	t.Helper()

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x2a}, 32))
	u, err := url.Parse("testing://" + key)
	require.NoError(t, err)

	return *u
}
