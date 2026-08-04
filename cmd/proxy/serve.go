package main

import (
	"context"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v3"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/kms"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// appLogger reports the fx lifecycle failures worth an operator's attention
// through the proxy's own logger.
//
// fx surfaces a failed start as an event rather than as [fx.App.Run]'s return
// value, so an app running under [fx.NopLogger] exits non-zero having said
// nothing: an unreachable upstream or extension server used to look like the
// proxy simply quitting. Everything else fx emits is dropped, which keeps
// startup quiet without hiding the one thing worth saying.
type appLogger struct {
	log logger.Logger
}

func serve() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "Start the proxy server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:      "config",
				Aliases:   []string{"c"},
				Usage:     "Path to the config file",
				TakesFile: true,
				Sources:   cli.EnvVars("PROXY_CONFIG"),
				Required:  true,
			},
			&cli.StringFlag{
				Name:    "level",
				Usage:   "Set the log level (debug, info, warn, error)",
				Value:   "info",
				Sources: cli.EnvVars("LOG_LEVEL"),
			},
			&cli.StringFlag{
				Name:    "metrics-addr",
				Usage:   "The host:port on which to serve /metrics",
				Value:   ":9090",
				Sources: cli.EnvVars("METRICS_ADDR"),
			},
			&cli.StringFlag{
				Name:    "metrics-namespace",
				Usage:   "The prometheus namespace for metrics",
				Value:   "tmprl_proxy",
				Sources: cli.EnvVars("METRICS_NAMESPACE"),
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			log := logger.NewZeroLogger(os.Stderr, logger.ParseLevel(cmd.String("level")))

			fxApp := fx.New(
				fx.Supply(
					fx.Annotate(ctx, fx.As(new(context.Context))),
					fx.Annotate(cmd.String("config"), config.ConfigFileTag),
					fx.Annotate(cmd.String("metrics-addr"), metrics.AddrTag),
					fx.Annotate(cmd.String("metrics-namespace"), metrics.NamespaceTag),
					// Services whose request and response types have their namespace
					// translation plans warmed at startup.
					[]protoreflect.FullName{"temporal.api.workflowservice.v1.WorkflowService"},
				),
				fx.Provide(
					func() logger.Logger { return log },
					func() prometheus.Gatherer { return prometheus.DefaultGatherer },
					func() prometheus.Registerer { return prometheus.DefaultRegisterer },
				),
				api.Module,
				auth.Module,
				config.Module,
				connect.Module,
				kms.Module,
				metrics.Module,
				protoutil.Module,
				proxy.Module,
				router.Module,
				server.Module,
				fx.WithLogger(func(l logger.Logger) fxevent.Logger { return &appLogger{log: l} }),
			)

			if err := fxApp.Err(); err != nil {
				log.Error("Misconfigured fx app", tag.Error(err))
				return err
			}

			// Run exits the process itself on failure, with the code fx.Shutdowner
			// was given, so a server that stops serving still exits non-zero for
			// whatever supervises this. It returns only on a clean shutdown.
			fxApp.Run()
			return nil
		},
	}
}

// LogEvent implements [fxevent.Logger]. Both cases carry the outcome of a whole
// lifecycle phase, so one line is logged per failed start or stop rather than one
// per hook.
func (l *appLogger) LogEvent(e fxevent.Event) {
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
