// Package codecserver serves the Temporal codec server HTTP endpoints, so a
// client that reaches a Temporal Service without passing through the gateway
// can still read payloads the proxy sealed.
//
// # Why It Exists
//
// A worker or client connecting through the gateway never sees ciphertext: the
// per-upstream proxy opens inbound payloads whether or not sealing is enabled.
// Two callers do not have that path. The Temporal Cloud UI reaches Cloud
// directly and calls a codec endpoint from the operator's browser, and the
// Temporal CLI run with --codec-endpoint reaches a Temporal Service directly.
// Those two are the audience for this package.
//
// # Routes
//
// POST /encode, POST /decode and POST /download, each also served under a
// leading namespace path segment, because the CLI can substitute the namespace
// into the endpoint URL while the Web UI sends it in an X-Namespace header. A
// known path used with any method but POST answers 405 with an Allow header;
// an unknown path answers 404.
//
// Requests and responses are protojson-encoded temporal.api.common.v1.Payloads
// with Content-Type application/json. A response carries exactly as many
// payloads as its request, in the same order, which the SDK's remote codec
// client requires. Unknown query parameters are ignored, since the Web UI
// appends preserveStorageRefs to every call.
//
// /download resolves external storage references. This proxy configures no
// external storage and so produces none, and the route reports that rather
// than appearing to retrieve something.
//
// # Codec Identity
//
// The handler applies a [Codecs], in practice
// [github.com/temporalio/temporal-proxy/internal/proxy.Codecs], which is the
// same value the per-upstream proxies install as a gRPC client interceptor. A
// payload therefore transforms identically whichever path it travelled,
// because the chain has one construction site rather than two free to drift
// apart.
//
// # Namespace Resolution
//
// The callers above name namespaces the way the upstream does, while the seal
// path keys its key policy on the pre-translation local name. [OverrideMap]
// closes that gap, mapping remote names forward through each upstream's own
// translation rules. A name it does not hold passes through unchanged, because
// the vault already falls back to the default key policy for a namespace it
// holds no key for.
//
// Resolution is a heuristic rather than a proof. The codec server protocol
// carries a bare namespace string with no indication of which name space it
// belongs to, so any implementation must either guess or be told. It can be
// wrong only where a local namespace name collides with a different
// namespace's translated remote name, it affects /encode alone, and the cost
// is the wrong key policy rather than unreadable data.
//
// # Security
//
// This surface holds KMS unwrap permission. A reachable /decode without
// authentication is a decryption oracle, and /encode is a sealing oracle,
// which is why configuration refuses an enabled codec server bound beyond
// loopback with no auth block, and refuses auth without TLS.
//
// The built-in authenticators validate the caller's credential and ignore the
// target namespace, so a token that passes authorizes every namespace's
// payloads rather than one. Per-namespace decisions require an
// extension-server authorizer.
//
// Every failure answers 4xx. A browser retries a 5xx three times and never
// retries a 4xx, so reporting a misconfiguration as a server error would
// triple its own load. The consequence worth knowing is that the Web UI
// renders a 4xx on /decode as the original sealed payload rather than an
// error, so a rejected token looks like undecoded ciphertext to an operator.
//
// # Thread Safety
//
// [Handler] and the http.Handler it returns are safe for concurrent use, as
// are [Reporter] and [OverrideMap]. A [Server] is single-use and is not
// restartable after [Server.Stop].
package codecserver
