// Package dataplane assembles the proxy's request path: one inbound gateway
// that routes by namespace, and one proxy per upstream that translates
// namespaces, attaches outbound credentials, and optionally encrypts payloads
// before forwarding to a Temporal Service.
package dataplane
