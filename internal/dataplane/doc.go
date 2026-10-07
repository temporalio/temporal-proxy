// Package dataplane assembles the proxy's request path: one inbound gateway
// that routes by namespace, and one forwarder per upstream that translates
// namespaces, attaches outbound credentials, and optionally encrypts payloads
// before forwarding to a Temporal Service.
package dataplane
