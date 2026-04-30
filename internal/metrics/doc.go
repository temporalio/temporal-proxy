// Package metrics provides Prometheus metric collection via typed handles.
// Use [NewProvider] to create a provider (or inject *Provider via metrics.Module),
// then call Register* methods to get handles for recording observations.
package metrics
