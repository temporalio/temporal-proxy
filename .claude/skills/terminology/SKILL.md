---
name: terminology
description: This project's canonical vocabulary. Use when writing or reviewing prose, doc comments, README or example docs, or commit messages, and when asked to check terminology or add a domain term.
allowed-tools: Read, Grep, Glob, Bash
---

# Terminology

Use these terms. The rejected column is not a style preference; each one was a
real drift that got fixed, and the checker will fail the build on it.

## Canonical terms

| Term                   | Means                                                                       | Never say                           |
| ---------------------- | --------------------------------------------------------------------------- | ----------------------------------- |
| **Temporal Service**   | A Temporal deployment the proxy connects to: local dev, self-hosted, Cloud. | cluster, Temporal server, frontend  |
| **gateway**            | The single inbound gRPC endpoint everything connects to.                    | front door, inbound server, ingress |
| **upstream**           | A configured destination the proxy forwards to, identified by name.         | backend, target, destination        |
| **per-upstream proxy** | The component bound to one upstream that translates, credentials, encrypts. | egress, forwarder                   |
| **system upstream**    | Handles Namespace-less requests such as `GetSystemInfo`.                    | control upstream                    |
| **default upstream**   | Where a request lands when no routing rule matched.                         | fallback upstream, catch-all        |
| **extension server**   | A gRPC service the operator runs that the proxy delegates a decision to.    | external server, plugin, provider   |
| **seal** / **open**    | Encrypt and decrypt a payload.                                              | encrypt/decrypt (at payload level)  |
| **DEK** / **KEK**      | Per-payload data key; the key that wraps it.                                | data key, master key                |
| **metric prefix**      | The string stamped on every metric name (`metrics.namespace` in config).    | metric namespace                    |

## Casing

Temporal's core nouns are proper nouns **in markdown prose**: Namespace, Worker,
Workflow, Activity, Client. Lowercase stays correct inside code fences, inline
code, config keys, CLI commands, log output, and link targets.

Go doc comments are the exception. They use lowercase "namespace" throughout and
are internally consistent, so leave them alone rather than starting a rewrite.

## Standing exceptions

- `rfc/` is a historical record. Report divergence, never edit it to match.
- Test fakes may say "frontend": standing in for the Temporal frontend is the point.
- `.github/` says "workflow" for GitHub Actions, which is not a Temporal Workflow.
- `dev/config.yaml` keeps its `cluster-1/2/3` upstream names; that is dev-harness
  naming, deliberately left alone.
- `examples/authz` says "cluster-scoped" because that mirrors Temporal OSS's own
  authorizer scope, not our upstream vocabulary.

## Qualify the overloaded words

`RoutingRule`, `NamespaceRules`, and `validation.Rule` are unrelated, as are
`server.Server` (the gateway), `proxy.Server` (a per-upstream proxy), and the
extension server. Never write "the rule" or "the server" unqualified in prose.

## Checking

`mise run lint:terms` runs the mechanical half: rejected phrases anywhere, plus
prose casing in markdown. It cannot judge whether a given "client" means a
Temporal Client or a gRPC client, so that part is on you.

Adding or changing a term means editing both this file and
`.claude/skills/terminology/terms.yaml`, which holds the enforced rules and the
reason behind every exception.
