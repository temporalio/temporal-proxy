package config

import (
	"cmp"
	"time"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

const (
	// defaultMaxResponseSize matches the Temporal Go SDK's receive limit, so a
	// response a worker accepts on a direct connection is not refused by the proxy.
	defaultMaxResponseSize ByteSize = 128 << 20 // 134217728 (128MiB)

	// defaultKeepAliveTime and defaultKeepAliveTimeout match the Temporal Go
	// SDK's keepalive, which a Temporal Service already permits.
	defaultKeepAliveTime    = 30 * time.Second
	defaultKeepAliveTimeout = 15 * time.Second

	// defaultMaxConnections spreads calls to a static or SRV upstream across
	// enough connections that long polls, which each hold a stream for up to a
	// minute, do not fill a connection's stream limit and stall other calls.
	defaultMaxConnections = 32

	// defaultTemplatedMaxConnections is the default for a templated upstream.
	// maxConnections applies per target, and a templated upstream resolves a
	// separate target for every namespace, so its total is this times the number
	// of namespaces it serves.
	defaultTemplatedMaxConnections = 4

	// maxResponseSizeLimit is one past the largest size gRPC can represent, since
	// it takes the limit as an int32-sized int.
	maxResponseSizeLimit ByteSize = 2 << 30 // 2147483648 (2GiB)

	// maxConnectionsLimit is one past the largest pool allowed. Static upstreams
	// open every connection on start, so a typo should not open thousands.
	maxConnectionsLimit = 65

	// minKeepAliveTime is gRPC's floor on the ping interval, which a Temporal
	// Service also enforces. gRPC raises a lower value to it with only a log
	// line, so a lower value is rejected rather than silently changed.
	minKeepAliveTime = 10 * time.Second
)

type (
	// ConnectionConfig tunes the gRPC connection to an upstream. Every field is
	// optional; see the accessors for what an absent one means.
	ConnectionConfig struct {
		MaxResponseSize ByteSize        `yaml:"maxResponseSize"`
		MaxConnections  int             `yaml:"maxConnections"`
		KeepAlive       KeepAliveConfig `yaml:"keepAlive"`
	}

	// KeepAliveConfig sets how often an idle connection is pinged (Time) and how
	// long to wait for the ack before the connection is closed (Timeout).
	KeepAliveConfig struct {
		Time    time.Duration `yaml:"time"`
		Timeout time.Duration `yaml:"timeout"`
	}
)

// ResponseLimit is the largest response accepted from the upstream: the
// configured size when there is one, and [defaultMaxResponseSize] when the field
// is absent.
func (c *ConnectionConfig) ResponseLimit() ByteSize {
	return cmp.Or(c.MaxResponseSize, defaultMaxResponseSize)
}

// PoolSize is how many connections calls to each upstream target are spread
// across: the configured count when there is one. When the field is absent it is
// [defaultTemplatedMaxConnections] for a templated upstream (see
// [Upstream.IsTemplated]), which has a target per namespace, and
// [defaultMaxConnections] otherwise.
func (c *ConnectionConfig) PoolSize(templated bool) int {
	if templated {
		return cmp.Or(c.MaxConnections, defaultTemplatedMaxConnections)
	}

	return cmp.Or(c.MaxConnections, defaultMaxConnections)
}

// Validate requires a positive response size that gRPC can represent, a pool of
// 1 to 64 connections, and valid keepalive settings. Fields are checked through
// their accessors, so an absent field is the default (which passes; both pool
// defaults are in range, so the static one stands in for either).
func (c *ConnectionConfig) Validate() error {
	return validation.Validate(
		"",
		validation.Field(
			"maxResponseSize",
			c.ResponseLimit(),
			validation.GT[ByteSize](0),
			validation.LT(maxResponseSizeLimit),
		),
		validation.Field(
			"maxConnections",
			c.PoolSize(false),
			validation.GT(0),
			validation.LT(maxConnectionsLimit),
		),
		validation.Nested("keepAlive", &c.KeepAlive),
	)
}

// PingTime is how long a connection may sit idle before it is pinged: the
// configured time when there is one, and [defaultKeepAliveTime] when the field
// is absent.
func (k *KeepAliveConfig) PingTime() time.Duration {
	return cmp.Or(k.Time, defaultKeepAliveTime)
}

// PingTimeout is how long to wait for a ping ack before closing the connection:
// the configured timeout when there is one, and [defaultKeepAliveTimeout] when
// the field is absent.
func (k *KeepAliveConfig) PingTimeout() time.Duration {
	return cmp.Or(k.Timeout, defaultKeepAliveTimeout)
}

// Validate requires a ping time at or above gRPC's minimum and a positive
// timeout.
func (k *KeepAliveConfig) Validate() error {
	return validation.Validate(
		"",
		validation.Field("time", k.PingTime(), validation.GTE(minKeepAliveTime)),
		validation.Field("timeout", k.PingTimeout(), validation.GT[time.Duration](0)),
	)
}
