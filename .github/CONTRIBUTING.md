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

The proxy itself listens on `localhost:8444` with TLS terminated using the dev certs in `dev/certs/`. The gateway
requires a client certificate (mTLS), so requests must present `dev/certs/client.crt`.

In a second shell, use the `grpc` task to send requests. It wraps `buf curl` with the dev certs and gRPC-over-HTTP2
flags:

```sh
mise run grpc <Method> [json-body] [-H 'key: value' ...]
```

A bare method name is assumed to be on `WorkflowService`. Anything containing a slash is used as a full `service/method`
path. Pass `-H`/`--header` (repeatable) to attach gRPC metadata, for example `-H 'x-cluster: 3'`.

### Confirming requests are forwarded upstream

`GetSystemInfo` takes no Namespace and is the cleanest proof that a request is relayed to the real Temporal Service:

```sh
mise run grpc GetSystemInfo
```

A response with the upstream's capabilities means the call traversed the proxy and reached the Temporal Service.

To exercise a Namespace-scoped call, use `DescribeNamespace`. Send the local alias; the proxy rewrites it to the
upstream name before forwarding, per the matched upstream's `upstream.namespaces.rules` in `dev/config.yaml`:

```sh
mise run grpc DescribeNamespace '{"namespace":"ns1"}'
```

> [!NOTE]
>
> The default upstream applies `suffix: .remote`, so the local alias `ns1` reaches the Temporal Service as `ns1.remote`,
> and the name in the response is translated back to `ns1`. The `ns3 -> ns2.remote` override maps the local alias `ns3`
> to `ns2.remote` upstream. `cluster-3` configures no rules, so calls routed there are forwarded verbatim.

### Confirming per-request routing

The proxy picks an upstream per request from the `routing` rules in `dev/config.yaml`, evaluated in order (first match
wins):

1. Namespace `test` -> `cluster-2`
2. metadata `x-cluster: 3` -> `cluster-3`

Everything else, including requests with no Namespace (such as `GetSystemInfo`), falls through to the `default` upstream
(`cluster-1`). A Namespace-scoped call to `test` lands on the second dev server:

```sh
mise run grpc DescribeNamespace '{"namespace":"test"}'
```

#### Routing on metadata

The second rule routes on request metadata rather than Namespace, so any request carrying `x-cluster: 3` goes to
`cluster-3` regardless of Namespace. `cluster-3` (`localhost:7235`) is the only dev server with the `test2` Namespace,
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

The gateway answers the health service locally, so a response proves it is accepting but says nothing about
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

We support the current minor release and the two before it (for example, `v1.0`, `v0.9`, and `v0.8`). Each minor
version has a `releases/vX.Y.x` branch, and patches for that line are cut from it. New minor and major releases are cut
from `main`.

Releases are cut from the [release workflow](workflows/release.yaml) via manual dispatch; you'll need write access to
the repository. The workflow computes the version, creates and pushes the tag, then runs goreleaser to publish the
binaries, container image, and GitHub release.

To cut a release, open **Actions -> release -> Run workflow** in the GitHub UI, pick the branch, and choose either:

- a **bump**; the next version is computed from the latest tag with `svu`. Use `minor` or `major` from `main`, and
  `patch` from a release branch (scoped to that line's tags), or
- an explicit **version** (with or without a leading `v`, e.g. `1.4.0` or `v0.2.0-alpha.20260722`), which overrides the
  bump. On a release branch it must belong to that line.

Releasing `vX.Y.0` from `main` also creates the `releases/vX.Y.x` branch from the new tag.

The release is marked latest on GitHub, and the container image is tagged `latest`, only when it is the highest stable
version. A patch to an older line (say `v0.9.1` after `v1.0.1`) is published without touching `latest`.

The run fails fast, without creating a tag, if it is dispatched from a branch other than `main` or `releases/vX.Y.x`,
if the bump doesn't match the branch, if an explicit version is not valid semver or doesn't belong to the release
branch, or if the resolved tag already exists. Only one release runs at a time.

### Backporting fixes

Fixes usually land on `main` first. To ship one on a supported line, open a PR against its `releases/vX.Y.x` branch:

- If the fix applies cleanly, cherry-pick it with `git cherry-pick -x <sha>`.
- If it doesn't (for example, the code it touches doesn't exist on that line), adapt it by hand.

Either way, link the corresponding `main` commit in the PR description. A fix that only applies to an older line is
fine too; say why there's no `main` counterpart. Once merged, run the release workflow from the release branch with a
`patch` bump.

Release branches for lines that fall out of the support window can be deleted by hand.
