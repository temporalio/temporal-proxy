# Contributing

## Dependencies

This project uses [mise] to pin every tool it depends on. Install mise, then run `mise install`
to fetch everything declared in the `[tools]` section of `mise.toml` (currently Go, buf, and
mkcert). After that, `mise deps` syncs the pinned dev tools (golangci-lint, mockgen).

## Useful Commands

`mise` also serves as the task runner. Run `mise tasks` to list all available commands.

| Command           | Description         |
| ----------------- | ------------------- |
| `mise deps`       | Update dependencies |
| `mise run lint`   | Lint Go files       |
| `mise run format` | Format Go files     |
| `mise run test`   | Run tests           |

[mise]: https://mise.jdx.dev/

## Testing the proxy locally

`mise run server` starts everything you need: a Temporal dev server (the upstream, on
`localhost:7233` with namespaces `ns1.remote` and `ns2.remote`) and the proxy itself, listening on
`localhost:8444` with TLS terminated using the dev certs in `dev/certs/`. The inbound server
requires a client certificate (mTLS), so requests must present `dev/certs/client.crt`.

In a second shell, use the `grpc` task to send requests. It wraps `buf curl` with the dev certs and
gRPC-over-HTTP2 flags:

```sh
mise run grpc <Method> [json-body]
```

A bare method name is assumed to be on `WorkflowService`. Anything containing a slash is used as a
full `service/method` path.

### Confirming requests are forwarded upstream

`GetSystemInfo` takes no namespace and is the cleanest proof that a request is relayed to the real
Temporal frontend:

```sh
mise run grpc GetSystemInfo
```

A response with the upstream's capabilities means the call traversed the proxy and reached the
frontend.

To exercise a namespace-scoped call, use `DescribeNamespace`:

```sh
mise run grpc DescribeNamespace '{"namespace":"ns1.remote"}'
```

> **Note:** namespace names are currently forwarded verbatim - the `upstream.namespaces.rules` in
> `dev/config.yaml` are parsed and validated but not yet applied. Until translation is wired up, you
> must send the real upstream name (`ns1.remote`), not the local alias (`ns1`). Sending `ns1`
> returns `NotFound`.

### Checking the front door (not proxied)

The health service is answered locally by the inbound server, so it verifies the front door is up
but does not prove forwarding. It uses a different schema, so call `buf curl` directly:

```sh
buf curl --protocol grpc --http2-prior-knowledge \
  --cacert dev/certs/ca.crt \
  --cert dev/certs/client.crt \
  --key dev/certs/client.key \
  --schema buf.build/grpc/grpc \
  https://localhost:8444/grpc.health.v1.Health/Check
```
