# Compression of Workflow Data

This RFC outlines a proposal for how the Temporal Proxy will optionally compress customer data before forwarding it
upstream. It expands on the **Payload compression** capability introduced in the
[Temporal Proxy overview RFC](./01-overview.md). Where the two differ, this RFC supersedes the overview's summary: it
adds Snappy alongside LZ4 and ZSTD, and allows per-namespace overrides rather than a single proxy-wide setting.

Questions and feedback can be directed to your Temporal account rep, the proxy team, or the community Slack channel.

- **Slack:** [#proxy](https://temporalio.slack.com/archives/C0BD5MZF6UD)
- **Email:** <proxy@temporal.io>

## Background

Workflow payloads cost bandwidth on the way to the upstream cluster and storage once they land in Workflow history,
visibility, and archival. Large, text-heavy payloads (JSON, XML, logs, serialized documents) are often highly
compressible, so compressing them before they leave the Worker pool can meaningfully cut both costs.

Today, a customer who wants this must build compression into every Worker's Data Converter or stand up their own codec
server, then keep that consistent across many teams. By centralizing compression at the proxy, operators turn it on in
one place, transparently to workflow developers, and pair it cleanly with the proxy's encryption if enabled.

Compression is designed as an **opt-in** feature. It is useful on its own and composes with encryption when both are
enabled.

> [!TIP]
>
> **Staying under Temporal's [size limits](https://docs.temporal.io/cloud/limits).** Temporal Cloud caps each payload at
> 2 MB, and every gRPC request at 4 MB across all payloads plus command metadata, so a Workflow can exceed the request
> limit even when each individual payload is under 2 MB. Because the proxy compresses before forwarding upstream, the
> cluster sees the compressed bytes, so compression can bring a payload, or a request carrying many of them, back under
> these limits.

## Goals and non-goals

Goals for the initial release:

- **Transparent:** no special Worker or client configuration; nothing changes for workflow developers.
- **Effective:** meaningful size reduction on compressible payloads, with a choice of algorithms to trade speed against
  ratio.
- **Composable:** works cleanly alongside encryption, always compressing before encrypting.
- **Reliable under change:** the proxy must never produce something it cannot later decompress. Changing the configured
  algorithm, or turning compression off, must never strand data already on the wire.
- **Observable:** operators can see whether compression is actually helping for their traffic.

Non-goals for the initial release:

- **Automatic content-type detection or auto-skip** of already-compressed or incompressible data. Operators use the
  ratio metric and per-namespace configuration instead.
- **Per-payload algorithm selection.** The configured algorithm applies to all eligible payloads.

## At a glance

- **Scheme:** per-payload. The proxy compresses the whole payload and replaces it with one whose metadata marks it
  compressed and names the algorithm used, so it can be decompressed correctly on the return trip.
- **Algorithms:** LZ4 (throughput), ZSTD (ratio, tunable level), and Snappy (fast, simple). All ship in every build.
- **Ordering:** compression runs before encryption, because encrypted bytes generally do not compress.
- **Granularity:** a `default` block, plus optional per-namespace overrides that inherit from it field by field.
- **Default algorithm:** none. When compression is enabled, an algorithm must be chosen explicitly.
- **Minimum size:** payloads below a configurable threshold are forwarded uncompressed, since the framing overhead
  outweighs any savings.
- **Decompression:** always attempted on any payload whose metadata marks it compressed, regardless of whether
  compression is currently enabled.

> [!NOTE]
>
> Decoding is driven entirely by the payload, not the current configuration. Because every build understands all
> supported algorithms, changing the configured algorithm or disabling compression never prevents an older payload from
> being decompressed.

## How it works

Compression is implemented the same way encryption is: a payload-visiting interceptor installed on each upstream
connection, which walks every request and response and acts on the [Payload] values it finds. When enabled, the proxy
compresses eligible payloads before forwarding them upstream and decompresses them before returning results downstream.
All of this is transparent; Workers and clients need no special configuration.

The proxy compresses the payload whole, not just its `Data`, and replaces it with a payload whose `Data` is the
compressed bytes and whose metadata marks what happened:

- `encoding` is set to `binary/compressed`. This is how the return path recognizes the proxy's own output.
- `compression-algorithm` records the algorithm used, so a payload decodes without consulting configuration.

Because the original payload is compressed whole, its own metadata (including its original `encoding`) travels inside
the compressed bytes and is restored exactly on the way back. This mirrors how encrypted payloads are marked and
restored, and it means a reader that encounters one of these payloads sees an `encoding` that honestly describes the
bytes in front of it rather than the value the payload carried before the proxy touched it.

```mermaid
flowchart LR
    PT[[Payload]] --> M["marshal (metadata included)"]
    M --> COMP["compress (LZ4 / ZSTD / Snappy)"]
    COMP --> P[["Payload<br/>encoding: binary/compressed<br/>compression-algorithm: zstd"]]
```

> [!NOTE]
>
> [Memo] and [Header] field values are already Payload objects, so they are compressed and decompressed in the same way
> without any special handling. In practice many of these are small enough to fall below the minimum-size threshold and
> are forwarded uncompressed.

Search attributes are the exception: they are skipped in both directions, so they stay indexed and queryable upstream.
Compressing them would leave the cluster indexing opaque bytes, which is the same reason encryption skips them.

[Memo]:
  https://github.com/temporalio/api/blob/8e0453c3a17693d7abb78853c3ccccf0c632e782/temporal/api/common/v1/message.proto#L53
[Header]:
  https://github.com/temporalio/api/blob/8e0453c3a17693d7abb78853c3ccccf0c632e782/temporal/api/common/v1/message.proto#L59
[Payload]:
  https://github.com/temporalio/api/blob/8e0453c3a17693d7abb78853c3ccccf0c632e782/temporal/api/common/v1/message.proto#L32

### Why compress before encrypt?

Encryption turns data into high-entropy, effectively random bytes, which do not compress. Compressing first is the only
ordering that yields any savings; reversing it would spend CPU on both steps for no benefit. When both features are
enabled the pipeline is therefore `compress -> encrypt` on the way upstream, and `decrypt -> decompress` on the way
back. The two interceptors are independent: either can be enabled without the other.

Mechanically this falls out of interceptor ordering. Encryption is the innermost interceptor on each upstream
connection, so compression sits outside it: outbound, compression runs first and encryption last; inbound, the chain
unwinds in reverse and decryption runs before decompression. Encryption seals the whole marshaled payload, so the
compression markers travel inside the ciphertext and are restored intact before the decompression step ever looks at
them.

### Choosing an algorithm

The proxy proposes three algorithms. We deliberately keep the list short; adding more later is straightforward.

| Algorithm  | Speed                        | Compression ratio                        | Notes                                                                    |
| ---------- | ---------------------------- | ---------------------------------------- | ------------------------------------------------------------------------ |
| **LZ4**    | Fastest                      | Lowest of the three                      | Symmetric; very fast to decompress as well as compress.                  |
| **Snappy** | Comparable to LZ4            | Similar to LZ4, usually a touch behind   | No tunable level; chosen for simplicity and codec-server parity.         |
| **ZSTD**   | Slower than LZ4, but tunable | Highest of the three, rises with `level` | At low levels approaches LZ4 speed; higher levels trade speed for ratio. |

There is **no implicit default**. When an operator enables compression they must name an algorithm, so payload format is
never changed based on a default we happened to pick. As a rough guide, reach for ZSTD when storage and bandwidth
savings matter most (level 3 is a sensible starting point) and LZ4 or Snappy when proxy CPU or added latency is the
primary concern. Exact throughput and ratio depend heavily on your payloads, so treat the table as a relative guide; the
[compression-ratio metric](#observability) tells you whether the choice is paying off for your traffic, and the
[zstd benchmarks](https://github.com/facebook/zstd#benchmarks) give published comparisons on a standard corpus.

> [!NOTE]
>
> Only the algorithm is recorded in payload metadata, not the level. Decompression is self-describing given the
> algorithm; the level affects encoding effort only, so old payloads decode correctly even after the configured level
> changes.

`level` is coarser than it looks. The ZSTD encoder maps the numeric level onto a handful of internal effort tiers, so
adjacent values (say 6 and 8) can produce byte-identical output. Treat it as choosing a tier, not turning a fine dial.

### Algorithms considered but excluded

The supported list is intentionally small; because each payload records its own algorithm, more can be added later
without a format change.

- **gzip / DEFLATE.** ZSTD compresses better and faster across DEFLATE's useful range, so it strictly improves on gzip
  for this use case. gzip's main draw, in the author's opinion, is universal interoperability, which does not apply here
  since the proxy compresses and decompresses its own payloads. There is no external consumer of the compressed bytes to
  remain compatible with.
- **Brotli.** Strong ratios on text, which fits this workload, but slower at the high quality levels where it wins, and
  ZSTD already covers the high-ratio end. Straightforward to add later if text ratio becomes a priority.
- **bzip2, xz / LZMA.** Archival-grade ratios at speeds far too slow for an inline, per-request transform on the hot
  path.
- **Operator-supplied codecs.** The proxy can already delegate key management to an extension server, and the same shape
  would work for compression, but a codec runs on every payload rather than once per key: an RPC per payload would cost
  far more than the compression saves. Algorithms stay built in.

## Default settings and per-namespace overrides

Compression is configured with a `default` block that sets the proxy-wide behavior, plus optional per-namespace
`overrides`. A namespace without its own entry, including one created after the proxy started, uses `default`.

This mirrors the encryption configuration, with one deliberate difference. An encryption override replaces the default
policy wholesale, because a key URI and its rotation schedule only mean anything together. A compression override is
instead merged field by field over `default`, because the common case is changing one thing (`algorithm: lz4`) and
inheriting the rest. A field an override leaves unset is inherited, not reset to zero.

Overrides are keyed by the **pre-translation (local) namespace name**: the name the Worker sends, not the upstream name
it may be translated to. This matches how encryption keys its overrides and how the interceptor reads the namespace off
the outgoing request.

Per-namespace overrides are not strictly necessary, but there are a few real reasons to reach for them:

- **Safe rollout.** Try a setting on a low-risk namespace (for example, staging or test) before applying it everywhere.
- **Mixed payload profiles.** One namespace may carry large compressible documents while another carries small or
  already-compressed blobs where compression only costs CPU. Per-namespace settings let you compress the former and skip
  the latter.
- **Cost and latency tuning.** A latency-sensitive namespace can use LZ4 while a storage-heavy one uses a higher ZSTD
  level.
- **Opt-out.** Set `enabled: false` for a single namespace whose payloads do not benefit (already compressed or
  incompressible) while compression stays on everywhere else. Unlike encryption, compression has no fail-closed
  constraint, so disabling it for one namespace is safe.

When compression is turned off (`enabled: false`), the decode path still runs: the proxy keeps decompressing any payload
whose metadata marks it compressed. Disabling compression is therefore safe and never strands data already on the wire.
Likewise, switching the configured algorithm only affects new payloads; existing ones carry their own algorithm marker
and continue to decode.

## Compression

Compression happens automatically based on the proxy configuration. For each payload being forwarded upstream, the proxy
first resolves the applicable settings (a namespace override merged over `default`, otherwise `default` alone), then
compresses only when compression is enabled for that namespace and the payload is at least `minBytes`.

```mermaid
flowchart TD
    PT["payload (forwarding upstream)"] --> RES["resolve settings (override merged over default)"]
    RES --> EN{"enabled for this namespace?"}
    EN -->|"no"| PASS["forward unchanged"]
    EN -->|"yes"| SZ{"Data size >= minBytes?"}
    SZ -->|"no"| PASS2["forward unchanged"]
    SZ -->|"yes"| COMP["compress with configured algorithm"]
    COMP --> MARK["record compression/algorithm in metadata"]
    MARK --> UP["forward upstream"]
```

## Decompression

Decompression is likewise transparent and requires no Worker or client configuration. The proxy only acts on payloads
whose `encoding` is `binary/compressed`, so plaintext payloads, and payloads compressed by some other system, pass
through untouched. Crucially, the decode path ignores the `enabled` flag entirely; it is driven only by what the payload
records.

```mermaid
flowchart TD
    IN["payload (returning downstream)"] --> MARKED{"encoding is binary/compressed?"}
    MARKED -->|"no"| PASS["pass through unchanged"]
    MARKED -->|"yes"| DEC["decompress with the recorded algorithm"]
    DEC --> UNM["unmarshal the original payload"]
    UNM --> OUT["original payload -> return to caller"]
```

## Configuration reference

Compression is configured under the top-level `compression` block. A fully annotated example:

```yaml
compression:
  # Whether to compress NEW payloads. Optional; defaults to false.
  # Decompression is attempted on any compressed payload regardless of this
  # flag, so turning it off does NOT strand data already on the wire.
  enabled: true

  # Settings for every namespace without an override.
  # REQUIRED when `enabled: true`.
  default:
    # Algorithm used to compress new payloads. REQUIRED.
    # One of: lz4 | zstd | snappy. There is no default.
    algorithm: zstd

    # Compression effort, 1-22. ZSTD only; ignored by lz4 and snappy.
    # Optional. Higher is smaller and slower, in coarse tiers rather than
    # 22 distinct settings.
    level: 3

    # Payloads whose Data is smaller than this (in bytes) are forwarded
    # uncompressed, since framing overhead would outweigh any savings.
    # Optional; defaults to a small value.
    minBytes: 256

  # Per-namespace overrides, keyed by the PRE-TRANSLATION (local) namespace
  # name. Each value may set any of `enabled`, `algorithm`, `level`, and
  # `minBytes`; whatever it leaves unset is inherited from `default`, and an
  # `enabled` here supersedes the top-level flag for that namespace alone. A
  # namespace not listed uses `default` as-is. Optional.
  overrides:
    staging:
      algorithm: lz4 # inherits level and minBytes from default
    archive:
      algorithm: zstd
      level: 9
    precompressed:
      enabled: false # opt this namespace out while compression stays on elsewhere
```

### Field reference

| Field               | Type     | Required                  | Description                                                                                                  |
| ------------------- | -------- | ------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `enabled`           | `bool`   | No (default `false`)      | Whether to compress new payloads. Decompression is attempted regardless.                                     |
| `default`           | `block`  | Yes, when `enabled: true` | Settings for any namespace without an override.                                                              |
| `default.algorithm` | `string` | Yes, when `enabled: true` | One of `lz4`, `zstd`, `snappy`. An unrecognized value is rejected at startup. No default.                    |
| `default.level`     | `int`    | No                        | ZSTD compression effort, 1-22; ignored by `lz4` and `snappy`. Validated at startup, applied in coarse tiers. |
| `default.minBytes`  | `int`    | No (small default)        | Payloads with `Data` smaller than this are forwarded uncompressed. Must be `>= 0`.                           |
| `overrides`         | `map`    | No                        | Per-namespace settings (`enabled`, `algorithm`, `level`, `minBytes`), merged field by field over `default`.  |

## Observability

### Metrics

Metrics are declared with the short names below and exported under the proxy's configured Prometheus namespace plus a
`compression` subsystem, matching how the encryption and KMS metrics are named. With a Prometheus namespace of `proxy`,
`ops_total` is exported as `proxy_compression_ops_total`, which is the form the examples below use.

| Metric              | Type      | Labels                                                        |
| ------------------- | --------- | ------------------------------------------------------------- |
| `ops_total`         | Counter   | operation (compress/decompress), algorithm, namespace, result |
| `ops_duration_secs` | Histogram | operation, algorithm, namespace                               |
| `payload_bytes`     | Histogram | stage (pre_compress/post_compress), algorithm, namespace      |

Size is measured on either side of compression only. Post-encryption size is deliberately absent: encryption reports its
own telemetry, and the two features are enabled independently, so a compression metric that assumed encryption was on
would be misleading whenever it was not.

### Examples of using these metrics

#### Compression ratio

Tracks how effective compression is, expressed as original size over compressed size (so 2.0 means the data halved;
higher is better). A ratio at or near 1.0 for a namespace means its payloads barely compress (already compressed,
encrypted, or otherwise incompressible), so compression is spending CPU for little or no gain; that namespace is a
candidate for a faster algorithm or for disabling compression via an override.

**PromQL:**
`avg(rate(proxy_compression_payload_bytes_sum{stage="pre_compress"}[5m])) / avg(rate(proxy_compression_payload_bytes_sum{stage="post_compress"}[5m]))`
by namespace.

#### Compression error rate

Compression or decompression errors cascade into request failures. Splitting by operation distinguishes an encode
problem from a decode problem (for example, a payload compressed by an algorithm an older build did not understand).

**PromQL:** `rate(proxy_compression_ops_total{result="error"}[5m]) / rate(proxy_compression_ops_total[5m])` by
operation.

#### CPU spent on compression

Compression is the one feature here that trades proxy CPU for savings elsewhere, so it is worth watching directly rather
than inferring from host CPU. The sum of operation durations per second reads as "cores occupied by compression", which
is the number to compare before and after changing algorithm or level.

**PromQL:** `sum(rate(proxy_compression_ops_duration_secs_sum[5m])) by (operation, algorithm)`.

## Operational notes

- **Configuration is validated at startup.** The proxy refuses to start on problems it can detect up front: a missing
  `default` block or `default.algorithm` while `enabled: true`, an unrecognized algorithm, a `level` outside the
  supported range, or a negative `minBytes`. The goal is to surface foreseeable mistakes as a clear startup failure
  rather than a runtime error mid-request.
- **Compressed payloads are marked, and opaque outside the proxy.** A compressed payload advertises
  `encoding: binary/compressed`, so nothing mistakes it for the data it used to be, but anything reading Workflow data
  straight from the cluster (the Cloud UI, a CLI pointed at the upstream, an archival consumer) sees compressed bytes.
  Only traffic returning through the proxy is decompressed.
- **Search attributes are never compressed.** They are skipped in both directions so they remain indexed and queryable
  upstream.
- **Decoding is never gated by `enabled`.** Turning compression off stops new payloads from being compressed but does
  not stop decompression, so disabling it never loses access to data already on the wire.
- **Restart on config change.** The proxy reads compression configuration at startup; changing the algorithm, level,
  threshold, or overrides requires a restart.
- **Compression spends proxy CPU.** For typical payload volumes this is negligible, especially with LZ4, Snappy, or low
  ZSTD levels, but it is real. Already-compressed or pre-encrypted data wastes CPU; the compression-ratio metric makes
  that visible.
- **Compression runs before encryption.** When both are enabled the upstream pipeline is `compress -> encrypt` and the
  downstream pipeline is `decrypt -> decompress`.
