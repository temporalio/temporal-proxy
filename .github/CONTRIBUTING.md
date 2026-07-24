# Contributing

## Dependencies

This project uses [mise] to pin every tool it depends on. Install mise, then run `mise install` to fetch everything
declared in the `[tools]` section of `mise.toml` (Go, buf, golangci-lint, goreleaser, markdownlint-cli2, mkcert, and
svu, among others).

## Useful Commands

`mise` also serves as the task runner. Run `mise tasks` to list all available commands.

| Command           | Description                  |
| ----------------- | ---------------------------- |
| `mise deps`       | Update dependencies          |
| `mise run lint`   | Lint Go and Markdown files   |
| `mise run format` | Format Go and Markdown files |
| `mise run test`   | Run tests                    |

[mise]: https://mise.jdx.dev/

## Testing the proxy locally

`mise run server` starts everything you need: three Temporal dev servers and the proxy. Each dev server backs one
upstream defined in `dev/config.yaml`:

| Dev server       | Namespaces                 | Upstream    |
| ---------------- | -------------------------- | ----------- |
| `localhost:7233` | `ns1.remote`, `ns2.remote` | `cluster-1` |
| `localhost:7234` | `test`                     | `cluster-2` |
| `localhost:7235` | `test2`                    | `cluster-3` |

The proxy itself listens on `localhost:8444` with TLS terminated using the dev certs in `dev/certs/`. The inbound server
requires a client certificate (mTLS), so requests must present `dev/certs/client.crt`.

In a second shell, use the `grpc` task to send requests. It wraps `buf curl` with the dev certs and gRPC-over-HTTP2
flags:

```sh
mise run grpc <Method> [json-body] [-H 'key: value' ...]
```

A bare method name is assumed to be on `WorkflowService`. Anything containing a slash is used as a full `service/method`
path. Pass `-H`/`--header` (repeatable) to attach gRPC metadata, for example `-H 'x-cluster: 3'`.

### Confirming requests are forwarded upstream

`GetSystemInfo` takes no namespace and is the cleanest proof that a request is relayed to the real Temporal frontend:

```sh
mise run grpc GetSystemInfo
```

A response with the upstream's capabilities means the call traversed the proxy and reached the frontend.

To exercise a namespace-scoped call, use `DescribeNamespace`. Send the local alias; the proxy rewrites it to the
upstream name before forwarding, per the matched upstream's `upstream.namespaces.rules` in `dev/config.yaml`:

```sh
mise run grpc DescribeNamespace '{"namespace":"ns1"}'
```

> [!NOTE]
>
> The default upstream applies `suffix: .remote`, so the local alias `ns1` reaches the frontend as `ns1.remote`, and the
> name in the response is translated back to `ns1`. The `ns3 -> ns2.remote` override maps the local alias `ns3` to
> `ns2.remote` upstream. `cluster-3` configures no rules, so calls routed there are forwarded verbatim.

### Confirming per-request routing

The proxy picks an upstream per request from the `routing` rules in `dev/config.yaml`, evaluated in order (first match
wins):

1. namespace `test` -> `cluster-2`
2. metadata `x-cluster: 3` -> `cluster-3`

Everything else, including requests with no namespace (such as `GetSystemInfo`), falls through to the `default` upstream
(`cluster-1`). A namespace-scoped call to `test` lands on the second dev server:

```sh
mise run grpc DescribeNamespace '{"namespace":"test"}'
```

#### Routing on metadata

The second rule routes on request metadata rather than namespace, so any request carrying `x-cluster: 3` goes to
`cluster-3` regardless of namespace. `cluster-3` (`localhost:7235`) is the only dev server with the `test2` namespace,
which makes the routing observable:

```sh
# Routed to cluster-3 (which has test2) -> succeeds
mise run grpc DescribeNamespace '{"namespace":"test2"}' -H 'x-cluster: 3'

# No header -> falls through to cluster-1 (which does not have test2) -> NotFound
mise run grpc DescribeNamespace '{"namespace":"test2"}'
```

The differing results confirm the metadata rule selected the upstream. Metadata keys are matched case-insensitively
(gRPC lowercases them).

### Checking the gateway (not proxied)

The health service is answered locally by the inbound server, so it verifies the gateway is up but does not prove
forwarding. It uses a different schema, so call `buf curl` directly:

```sh
buf curl --protocol grpc --http2-prior-knowledge \
  --cacert dev/certs/ca.crt \
  --cert dev/certs/client.crt \
  --key dev/certs/client.key \
  --schema buf.build/grpc/grpc \
  https://localhost:8444/grpc.health.v1.Health/Check
```

## Releases

Releases are cut from the [release workflow](workflows/release.yaml) via manual dispatch; you'll need write access to
the repository. The workflow computes the version, creates and pushes the tag, then runs goreleaser to publish the
binaries, container image, and GitHub release.

To cut a release, open **Actions -> release -> Run workflow** in the GitHub UI, select `main` as the branch, and choose
either:

- a **bump** (`patch`, `minor`, or `major`); the next version is computed from the latest tag with `svu`, or
- an explicit **version** (with or without a leading `v`, e.g. `1.4.0` or `v0.2.0-alpha.20260722`), which overrides the
  bump.

The run fails fast, without creating a tag, if it is dispatched from a branch other than `main`, if an explicit version
is not valid semver, or if the resolved tag already exists.
