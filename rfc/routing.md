# Upstream Routing

How to handle routing to multiple upstream clusters based on namespace.

## Background

The proxy currently supports a single static upstream defined in config. However, we want a single proxy (tenant) to
serve many namespaces, each potentially terminating at a different address/upstream.

This can be for a number of reasons, but notably, during migrations, some teams will want to move over
namespace-by-namespace rather than cluster-by-cluster. They’d point all namespaces to the original upstream (typically
the OSS frontend) and switch to another one as needed based on some routing rules.

> [!NOTE]
>
> Cloud offers three relevant flavours of endpoint: per-namespace endpoints (`<namespace>.<account>.tmprl.cloud:7233`),
> regional endpoints (`<region>.aws.tmprl.cloud:7233`), and private-link endpoints (per-VPC hostnames). Operators also
> need to mix Cloud and self-hosted upstreams in a single proxy.
>
> Ideally, we’d use the namespace endpoints since they’re already set up to handle multi-region traffic. However, there
> are times when that isn’t possible, so we should support all cases.

## Proposal

We need to support multiple named upstream configs in a single proxy and select among them (per request) using a set of
routing rules. These rules will have access to the incoming namespace (before and after translation) and to gRPC
metadata headers.

### Upstreams

As part of this, we’ll need to exchange the single upstream we have today for multiple upstream definitions. Each
upstream will have its `hostPort` and `tls.serverName` fields templated, allowing for the following replacements:

- `{{ .LocalNamespace }}` \- The namespace before translation
- `{{ .RemoteNamespace }}` \- The namespace after translation
- `{{ .Metadata.DC }}` \- The `DC` metadata value (arbitrary)
- `{{ index .Metadata “x-cluster” }}` \- The `x-cluster` metadata value (arbitrary)

To be clear, these are simply available, but not required. Static `hostPort/serverName` values are not a problem (see
the example config [below](#configuration-schema)).

To minimize runtime issues, all static `hostPort` upstreams will establish eager connections at startup. Templated
values are not predetermined and therefore cannot benefit from this. Those connections will be created lazily on first
use.

All connections will be maintained in a shared container (TBD) since connection establishment is expensive. Each
connection will be a gRPC `ClientConn,` ensuring automatic redials and shared underlying channels via connection
pooling.

### Routing

To match incoming requests to the appropriate upstream, we’ll need to define routing rules. In proxy config, we can
define a set of rules that grant access to request information, which can be used to select an upstream connection.

These rules will be evaluated in the order in which they are defined. The first matching rule will be used to route the
request, with one notable exception: requests without a namespace (system requests). These will be sent to a particular
upstream defined as the `system` upstream.

The other special case is handling the absence of a match. It seems perfectly reasonable to reject the request with an
error, but some operators may prefer to fall back to a specific upstream (e.g., receiving Temporal’s `NamespaceNotFound`
rather than a proxy-specific error).

#### Rules

To route incoming requests to the appropriate upstream, we’ll need to define a mapping in the proxy config. The mapping
can be done using the incoming namespace and/or gRPC metadata attached to the request. A rule is defined with a matcher
that specifies the namespace and the metadata values to match, and an upstream to route to.

Rules use `AND` logic to match. This means that defining a rule for `namespace=test` that contains a metadata
`key=value` pair requires both conditions to be true to match. While more complex logic could be added, it also
introduces complexity and the risk of logic errors in the config.

It is an error to define a rule without a namespace or metadata values. This will fail validation on startup.

##### Namespace Rules

Since namespace translation is defined per upstream and we don’t yet know which upstream we’re using, the local
namespace (before translation) is used to perform the match. When a namespace is not defined on the rule, no
namespace-based matching will be performed.

Namespace matches can be defined as string literals or simple globs. This supports exact-match, starts-with, ends-with,
and contains semantics. Here are some valid examples:

- `my-namespace`
- `prod-*`
- `*-test`
- `*-test-*`

##### Metadata Rules

Key/value pairs can be added to the match to validate incoming header metadata. When multiple conditions are defined,
they are `AND`ed (i.e. all conditions must match).

A header condition contains a map of keys and values. Keys are not case-sensitive (normalized to lowercase) but do not
support wildcards. The intent here is to allow operators to make routing decisions based on known metadata (e.g.
`X-Data-Centre`). Values follow the same convention as namespaces, with support for exact-match, starts-with, ends-with,
and contains semantics.

#### System Upstream

Not all RPCs have a namespace. For example, `GetClusterInfo` or `GetSystemInfo`. When these are called, we need to
ensure they get sent to the appropriate upstream. To handle this, the routing config has a `system` field which can be
set to a known upstream.  
When not defined, requests without a namespace fallback to the default upstream (see below).

#### Default Upstream

When a request cannot be matched, that is to say, no routing rules apply, the proxy will return an error (e.g.
`RouteNotFound`). This isn’t always what operators desire. Sometimes they’d like a fallback that sends the request to a
default upstream.

For these cases, a `default` upstream can be defined in the routing rules. This will ensure all requests are proxied
regardless of namespace or metadata.

### Configuration Schema

Based on the information above, here’s an example of what the config could look like.

```yaml
hostPort: 0.0.0.0:7233
tls: { ... }

# Now plural
upstreams:
  - name: cloud-namespace
    hostPort: "{{ .RemoteNamespace }}.acme-cloud.tmprl.cloud:7233"
    tls:
      cert: /etc/proxy/client.crt
      key: /etc/proxy/client.key
      # serverName defaults to resolved host

  - name: cloud-regional
    hostPort: "us-west-2.aws.tmprl.cloud:7233"
    tls:
      ca: /etc/proxy/ca.crt
      cert: /etc/proxy/client.crt
      key: /etc/proxy/client.key
      serverName: "{{ .RemoteNamespace }}.aws.tmprl.cloud"

  - name: local
    hostPort: "localhost:7233"

routing:
  default: local
  system: cloud-regional

  rules:
    - match:
        namespace: "prod-*"
        headers:
          x-tier: "gold"
      upstream: cloud-namespace

    - match:
        namespace: "regional-*"
      upstream: cloud-regional

    - match:
        namespace: "dev-*"
      upstream: local
```
