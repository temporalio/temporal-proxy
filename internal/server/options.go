package server

import (
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

type (
	Option interface {
		apply(*options)
	}

	options struct {
		creds       Credentials
		healthCheck HealthCheck
		logger      log.Logger
		reflect     bool
	}

	optFunc func(*options)
)

func WithCredentials(creds Credentials) Option {
	return optFunc(func(o *options) { o.creds = creds })
}

func WithHealthCheck(hc HealthCheck) Option {
	return optFunc(func(o *options) { o.healthCheck = hc })
}

func WithLogger(log log.Logger) Option {
	return optFunc(func(o *options) { o.logger = log })
}

func WithReflection(r bool) Option {
	return optFunc(func(o *options) { o.reflect = r })
}

func (o *options) serverOptions() ([]grpc.ServerOption, error) {
	creds, err := o.creds.ServerOption()
	if err != nil {
		return nil, err
	}

	return []grpc.ServerOption{creds}, nil
}

func (f optFunc) apply(o *options) {
	f(o)
}
