// Package certreload wires TLS certificate hot reload into the proxy. It
// provides the Prometheus implementation of [creds.Reporter] and an fx [Module]
// that supplies the [creds.Reloader] and ties its polling loop to the
// application lifecycle. It lives apart from the creds package because the
// metrics package depends on config, which depends on creds.
//
// [creds.Reloader]: go.temporal.io/proxy/internal/transport/creds.Reloader
// [creds.Reporter]: go.temporal.io/proxy/internal/transport/creds.Reporter
package certreload
