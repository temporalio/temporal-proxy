// Package template renders the templated fields used by the proxy against
// per-request values.
//
// Templates are ordinary text/template source. They are bound to the context
// they render against:
//
//   - [ParseRouting] yields a template over a [RoutingContext] (local namespace
//     and metadata), used while routing rules choose an upstream. The remote
//     namespace is not available here because translation is defined per
//     upstream.
//   - [ParseUpstream] yields a template over an [UpstreamContext] (local and
//     remote namespace, and metadata), used to render the chosen upstream's
//     hostPort and serverName.
//
// A reference to a field absent from the bound context (such as RemoteNamespace
// in a routing template) fails at parse time. A reference to an absent metadata
// key renders as the empty string.
package template
