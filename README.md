# Temporal Proxy ([Pre-release])

[![ci](https://github.com/temporalio/temporal-proxy/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/temporalio/temporal-proxy/actions/workflows/ci.yaml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A gRPC proxy that sits between Temporal SDK clients, workers, and the Temporal UI on one side and one or more upstream
Temporal clusters on the other. It handles namespace translation and TLS termination so applications can target a
single local endpoint while the proxy fans requests out to the right upstream (a local dev cluster, a self-hosted
deployment, Temporal Cloud, or some mix).

> [!NOTE]
> **Pre-release:** This project is under active development and evolving quickly. It is not ready for production use.
> Open a GitHub issue if you have questions or want to follow along.

## Development

See [`.github/CONTRIBUTING.md`](.github/CONTRIBUTING.md) for the dev loop. The common entry points are `mise run test`,
`mise run lint`, and `mise run format`.

## Security

See [`SECURITY.md`](SECURITY.md) for how to report vulnerabilities.

## License

MIT, see [`LICENSE`](LICENSE).

[Pre-release]: https://docs.temporal.io/evaluate/development-production-features/release-stages
