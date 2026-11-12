package app

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"go.temporal.io/proxy/internal/api"
	"go.temporal.io/proxy/internal/auth"
	"go.temporal.io/proxy/internal/certreload"
	"go.temporal.io/proxy/internal/codecserver"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane"
	"go.temporal.io/proxy/internal/httpserver"
	"go.temporal.io/proxy/internal/kms"
	"go.temporal.io/proxy/internal/metrics"
	"go.temporal.io/proxy/internal/pprofserver"
	"go.temporal.io/proxy/internal/protoutil"
	"go.temporal.io/proxy/internal/transport/connect"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
	"go.temporal.io/proxy/pkg/validation"
)

// shutdownTimeout bounds the whole stop sequence: the serving tiers, which drain
// concurrently within their own budgets, and every lifecycle hook queued behind
// them.
const shutdownTimeout = 30 * time.Second

// warmServices are the services whose request and response types have their
// namespace translation plans warmed at startup.
var warmServices = []protoreflect.FullName{"temporal.api.workflowservice.v1.WorkflowService"}

type (
	// Inputs are what the graph cannot decide for itself. Context, Logger and
	// Config are required.
	Inputs struct {
		// Context is the process context; cancelling it stops background work
		// such as health checks.
		Context context.Context

		// Logger is the application logger, and also where startup and shutdown
		// failures are reported.
		Logger logger.Logger

		// Config is config.File(path) or config.Supply(cfg).
		Config fx.Option

		// RequireEncryption fails the graph unless the config has an encryption
		// key.
		RequireEncryption bool

		// Registry is where every collector registers and what /metrics serves.
		// Nil means the process-wide Prometheus default registry.
		Registry *prometheus.Registry
	}

	// eventLogger swallows fx's event stream except for Started/Stopped
	// failures, which it forwards to the app logger. Configuration errors are
	// surfaced separately via fx.App.Err.
	eventLogger struct {
		log logger.Logger
	}
)

// New assembles the full proxy graph from in. Overrides apply last and are for
// fx.Replace, fx.Decorate and fx.Populate; a second provider of a type the
// graph already provides fails the graph.
func New(in Inputs, overrides ...fx.Option) *fx.App {
	if err := in.validate(); err != nil {
		return fx.New(fx.Error(err), fx.NopLogger)
	}

	return fx.New(
		fx.Supply(fx.Annotate(in.Context, fx.As(new(context.Context)))),
		in.Config,
		config.RequireKey(in.RequireEncryption),
		fx.Supply(
			fx.Annotate(protoregistry.GlobalFiles, fx.As(new(protoutil.Files))),
			fx.Annotate(protoregistry.GlobalTypes, fx.As(new(protoutil.Types))),
			warmServices,
		),
		fx.Provide(func() logger.Logger { return in.Logger }),
		registry(in.Registry),
		api.Module,
		auth.Module,
		certreload.Module,
		codecserver.Module,
		connect.Module,
		dataplane.Module,
		httpserver.Module,
		kms.Module,
		metrics.Module,
		pprofserver.Module,
		protoutil.Module,
		fx.WithLogger(func(l logger.Logger) fxevent.Logger { return &eventLogger{log: l} }),

		// Stated rather than inherited, because the relationship is what matters
		// and it is otherwise invisible: each serving tier drains within its own
		// budget, this bounds all of them plus the hooks behind them, and
		// whatever supervises the process has to allow more than this before it
		// resorts to SIGKILL. fx abandons the hooks still queued when this
		// expires.
		fx.StopTimeout(shutdownTimeout),

		fx.Options(overrides...),
	)
}

// validate reports every required input that is missing.
func (in Inputs) validate() error {
	return validation.Validate(
		"app.Inputs",
		validation.Field("Context", in.Context, validation.Required[context.Context]()),
		validation.Field("Logger", in.Logger, validation.Required[logger.Logger]()),
		validation.Field("Config", in.Config, validation.Required[fx.Option]()),
	)
}

// LogEvent forwards lifecycle failures to the app logger and drops the rest.
func (l *eventLogger) LogEvent(e fxevent.Event) {
	switch e := e.(type) {
	case *fxevent.Started:
		if e.Err != nil {
			l.log.Error("Failed to start", tag.Error(e.Err))
		}
	case *fxevent.Stopped:
		if e.Err != nil {
			l.log.Error("Failed to stop cleanly", tag.Error(e.Err))
		}
	}
}

// registry provides the Registerer collectors register with and the Gatherer
// /metrics serves, always from the same registry.
func registry(r *prometheus.Registry) fx.Option {
	if r == nil {
		return fx.Provide(
			func() prometheus.Registerer { return prometheus.DefaultRegisterer },
			func() prometheus.Gatherer { return prometheus.DefaultGatherer },
		)
	}

	return fx.Provide(
		func() prometheus.Registerer { return r },
		func() prometheus.Gatherer { return r },
	)
}
