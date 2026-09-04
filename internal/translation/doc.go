// Package translation rewrites one gRPC method call into another on the hop to
// the upstream.
//
// A [Translation] pairs an inbound method with the upstream method that stands
// in for it, plus the conversions between their request and response types. A
// [Registry] holds the set of them, and [UnaryClientInterceptor] applies it:
// a call whose method is registered is converted, sent under the upstream
// method, and converted back before the caller sees a reply. Everything else is
// passed straight through.
//
// This is distinct from the namespace translation in internal/proxy, which
// rewrites names inside a message but leaves the method alone. The two compose:
// install method translation as the innermost interceptor so namespace
// translation, payload codecs, and the reflective forwarder all keep seeing the
// method and message types the caller actually asked for.
//
// The package is the mechanism only; it ships no translations of its own. A
// [Translation] names the two methods and carries the conversions between their
// message types, so what is translated lives with the domain that needs it.
package translation
