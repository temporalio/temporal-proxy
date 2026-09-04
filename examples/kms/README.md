# KMS extension server example

This example encrypts every payload the proxy moves to a Temporal Service, the same envelope encryption the Cloud
example uses, but the key that wraps each data encryption key (DEK) lives in a small gRPC service you run yourself
instead of a cloud KMS. The proxy talks to that service, an extension server, only to wrap and unwrap DEKs; it never
sends payload plaintext there.

```mermaid
flowchart LR
    client["worker / starter<br/>plaintext gRPC<br/>cleartext payloads"]
    proxy["proxy (local)<br/>gateway :7234"]
    dev["temporal server start-dev<br/>:7233"]
    kms["extension server<br/>go run ./server<br/>:9443"]

    client -->|"cleartext payloads"| proxy
    proxy -->|"sealed payloads"| dev
    proxy -.->|"wrap/unwrap DEK over TLS + Bearer<br/>key material only, never payloads"| kms
```

## Prerequisites

- Go and a checkout of this repository; the proxy and the example both run from source.
- The `temporal` CLI, for the dev server and for inspecting Workflow history.
- No cloud account and no credentials: everything in this example runs on localhost.

Everything under `examples` is one Go module, and its `go.mod` builds the proxy from this working tree with a `replace`
directive; if you copy this example into another tree, drop that `replace` line and require a released proxy version
instead.

## Generate certificates

```bash
cd examples/kms
go run ./gencerts
```

```text
2026/07/29 15:31:46 wrote certs/ca.pem
2026/07/29 15:31:46 wrote certs/server.pem
2026/07/29 15:31:46 wrote certs/server-key.pem
```

This writes a throwaway certificate authority and a server certificate for the extension server into `certs/`
(gitignored). The CA's private key is discarded right after it signs the server certificate, so nothing else can ever be
signed by it, and the CA is never installed in any system trust store; the proxy is pointed at the CA file directly
through `config.yaml`.

## Environment

```bash
export KMS_API_KEY=example-token
export KMS_MASTER_SECRET=example-master-secret
```

The proxy only needs `KMS_API_KEY`, the bearer token it presents to the extension server. The extension server needs
both: the same API key to authenticate callers, and the master secret every Namespace's wrapping key is derived from.

## Run

Open four terminals.

Terminal 1, in `examples/kms`, starts the dev server:

```bash
temporal server start-dev
```

Terminal 2, in `examples/kms`, starts the extension server:

```bash
KMS_API_KEY=example-token KMS_MASTER_SECRET=example-master-secret go run ./server
```

```text
2026/07/29 15:26:00 extension server listening on 127.0.0.1:9443 over TLS
```

Terminal 3, from the repository root, starts the proxy:

```bash
KMS_API_KEY=example-token go run ./cmd/proxy serve -c examples/kms/config.yaml
```

```text
{"level":"info","namespace":"default","uri":"extension://kms/payloads","time":"2026-07-29T15:26:15-04:00","message":"Registering crypto key"}
{"level":"info","component":"metrics","addr":":9090","time":"2026-07-29T15:26:15-04:00","message":"Starting metrics server"}
{"level":"warn","addr":"/var/folders/6m/x_q_q9nx6ns3ygnf48xzqrn80000gn/T/127-0-0-1-7233-6689a1b6.sock","time":"2026-07-29T15:26:15-04:00","message":"Running with insecure credentials. Configure TLS for production use."}
{"level":"info","addr":"/var/folders/6m/x_q_q9nx6ns3ygnf48xzqrn80000gn/T/127-0-0-1-7233-6689a1b6.sock","time":"2026-07-29T15:26:15-04:00","message":"Starting the server"}
{"level":"warn","addr":"127.0.0.1:7234","time":"2026-07-29T15:26:15-04:00","message":"Running with insecure credentials. Configure TLS for production use."}
{"level":"info","addr":"127.0.0.1:7234","time":"2026-07-29T15:26:15-04:00","message":"Starting the server"}
```

The two `Running with insecure credentials` warnings are expected, not a sign anything is broken: they describe the
local gateway and an internal socket, the plaintext hops between the Worker/starter and the proxy on this machine. They
say nothing about whether payloads get sealed or about the TLS connection to the extension server; both of those are
unaffected.

Terminal 4, in `examples/kms`, starts the Worker:

```bash
go run ./worker
```

```text
2026/07/29 15:26:27 worker listening on task queue "kms-example" (namespace "default")
```

With all four running, start the Workflow from `examples/kms`:

```bash
go run ./starter
```

```text
2026/07/29 15:26:37 started workflow id=kms-example-greeting runID=019faf57-d74f-7119-b9ff-70e7740fcfab
Hello, Temporal!
```

Your run ID and timestamps will differ.

## What just happened

The Worker and starter never saw an encryption key: they dialed the proxy at `127.0.0.1:7234` in plaintext and exchanged
the plain string `"Temporal"` and the plain string `"Hello, Temporal!"`. For the Workflow's first payload, the proxy:

1. generated a random 32-byte DEK;
2. called the extension server's `Encrypt` RPC over TLS, with a bearer token, asking it to wrap that DEK under the
   Namespace `default` - this is the only thing that crossed the wire to `127.0.0.1:9443`, no payload plaintext ever
   went there;
3. sealed the payload locally with the DEK and recorded the wrapped DEK, and the key's URI, in the payload's metadata;
   and
4. sent the sealed payload on to the dev server at `127.0.0.1:7233`.

Every later payload in the same run, the Activity's input and result and the Workflow's own result, reused the cached
DEK instead of asking the extension server to wrap a new one; see "The cache is working" below.

## See the ciphertext

> [!NOTE]
>
> If `TEMPORAL_API_KEY` is set in your shell, perhaps from running the Cloud example, the commands below fail with
> `tls: first record does not look like a TLS handshake`. The CLI turns on TLS whenever an API key is present, and
> everything here is plaintext on localhost. Unset it for this shell, or prefix each command with
> `env -u TEMPORAL_API_KEY`.

Straight to the dev server, bypassing the proxy, the payload is still sealed:

```bash
temporal workflow show --workflow-id kms-example-greeting --namespace default --address 127.0.0.1:7233
```

```text
Progress:
  ID           Time                     Type
    1  2026-07-29T19:26:37Z  WorkflowExecutionStarted
    2  2026-07-29T19:26:37Z  WorkflowTaskScheduled
    3  2026-07-29T19:26:37Z  WorkflowTaskStarted
    4  2026-07-29T19:26:37Z  WorkflowTaskCompleted
    5  2026-07-29T19:26:37Z  ActivityTaskScheduled
    6  2026-07-29T19:26:37Z  ActivityTaskStarted
    7  2026-07-29T19:26:37Z  ActivityTaskCompleted
    8  2026-07-29T19:26:37Z  WorkflowTaskScheduled
    9  2026-07-29T19:26:37Z  WorkflowTaskStarted
   10  2026-07-29T19:26:37Z  WorkflowTaskCompleted
   11  2026-07-29T19:26:37Z  WorkflowExecutionCompleted

Results:
  Status          COMPLETED
  Result          {"metadata":{"encoding":"YmluYXJ5L2VuY3J5cHRlZA==","encryption-dek":"QVFBSFpHVm1ZWFZzZElKVTRMRXZMSTIvVVVjeXJ2Z281M1RKbUxZK2tYRWJRRkVuVEluN1ovU3RLZUdQd0NodmxEc2lINjZZRjkxYVNRWFJPOTh1Qkx6OEl1T3BVQT09","encryption-key-id":"ZXh0ZW5zaW9uOi8va21zL3BheWxvYWRz"},"data":"BKaq27qe7704D5tnl23NI5szspr6NE7FeLk4r/Ur3zPq+XRDBXIuOswCinmTp34+9C5PrFcWrw+t13zfnRDobEy14CKSwT28"}
  ResultEncoding  binary/encrypted
```

Through the proxy, the same execution comes back in the clear (only the `Results` block is shown below):

```bash
temporal workflow show --workflow-id kms-example-greeting --namespace default --address 127.0.0.1:7234
```

```text
Results:
  Status          COMPLETED
  Result          "Hello, Temporal!"
  ResultEncoding  json/plain
```

The Web UI at <http://localhost:8233> shows the same completed execution directly against the dev server, so it is
another way to browse the sealed history without running the CLI twice.

## The cache is working

Across the whole run above, the extension server's log shows a single wrap, and a handful of unwraps:

```text
wrapped a DEK: namespace=default plaintext=32B ciphertext=70B
unwrapped a DEK: ciphertext=70B plaintext=32B
unwrapped a DEK: ciphertext=70B plaintext=32B
unwrapped a DEK: ciphertext=70B plaintext=32B
unwrapped a DEK: ciphertext=70B plaintext=32B
unwrapped a DEK: ciphertext=70B plaintext=32B
```

The one wrap is the number to watch, and it is the same on every run. The unwrap count is not: expect a few, varying run
to run. The read-side cache is filled after the first miss and has no single-flight, so payloads opened concurrently can
all miss it together and each ask the extension server for the same DEK.

Meanwhile the Workflow's history carries four payloads sealed under that one DEK: the Workflow input, the Activity's
input and result, and the Workflow's result each carry an `encryption-dek` entry in their metadata. Two separate
settings in `config.yaml` are behind that, and they govern different sides of the exchange. `duration: 1h` is how long
the proxy reuses one sealed-side DEK before wrapping a fresh one, which is why sealing four payloads in this run only
cost one wrap call. `cacheSize: 200` is unrelated to sealing: it bounds a separate LRU of unwrapped DEKs the proxy keeps
for reading history back, keyed by the encrypted DEK it already opened once. `renewBefore: 15m` is how far ahead of
`duration`'s expiry the proxy starts rotating in a replacement sealed-side DEK.

## How the provider works

The whole provider is the `server` command you ran in terminal 2, and almost all of it is one file. The crypto and the
ciphertext framing live in `server/keyring.go`; the gRPC surface, the bearer token check, TLS, and graceful shutdown all
come from `pkg/ext`. `keyring`'s `Wrap` and `Unwrap` are what satisfy `ext.KMS`, and `server/main.go` hands them to
`ext.Serve` alongside the token check and the certificate. Start with `keyring.go` if you are writing one of these
against a real key service: it is the part you have to replace.

Every ciphertext the provider produces is a self-contained frame:

```text
[ version: 1 byte ][ namespace length: 2 bytes ][ namespace ][ nonce: 12 bytes ][ ciphertext + GCM tag ]
```

`Decrypt` receives nothing but that ciphertext, no Namespace and no other context, so `Unwrap` has to read the Namespace
back out of the frame before it can derive the matching key. The Namespace also doubles as the GCM additional data, so a
ciphertext relabelled with a different Namespace fails to open rather than silently decrypting under the wrong key. It
is also why the `default` Namespace's ciphertext above is 70 bytes while the `payments` Namespace's, in the optional
section below, is 71: the only difference is one extra byte of Namespace name carried inside the frame.

Three things worth being honest about. First, a Namespace is not secret, but a real provider that would rather not carry
a plaintext tenant name in its ciphertexts could use an opaque key identifier instead and resolve it internally. Second,
the `payloads` segment in `config.yaml`'s key URI (`extension://kms/payloads`) never reaches the extension server; the
provider selects a key by Namespace alone, nothing else. That segment exists only on the proxy's side, as the identifier
it uses to pick the same key policy again when opening a payload later, but it does have to be globally unique across
`default` and every entry in `overrides`: two policies sharing a URI fail proxy startup with a `duplicate key id` error
(see the optional section below). Third, the Namespace the provider receives is always the local, pre-translation
Namespace, never a translated remote name; this example configures no translation so it never comes up here, but a
provider paired with Namespace translation has to key on that same local name, or its per-Namespace keys end up
misaligned with the Namespace a caller actually asked for.

## When it breaks

**Wrong credentials.** Restarting the extension server with a different `KMS_API_KEY` while the proxy still holds the
old one does not fail right away: the proxy caches DEKs, so a run within the cache window keeps succeeding without
another call to the extension server. Left alone, that window closes on its own after roughly 45 minutes (the `1h`
`duration` minus the `15m` `renewBefore` head start), when the proxy wraps a fresh DEK and finally calls an extension
server that no longer accepts the old credential; a broken credential can stay invisible for most of an hour. To see the
failure immediately instead of waiting, restart the proxy, which clears the cache and forces a fresh `Encrypt` call,
which the extension server now rejects:

```text
start workflow: rpc error: code = Unauthenticated desc = failed to encrypt payload: failed to encrypt message in
namespace: default, failed to encrypt DEK, id: extension://kms/payloads, err: rpc error: code = Unauthenticated
desc = invalid credentials
```

The Workflow never starts; no execution shows up in `temporal workflow list`. The error surfaces only on the starter's
side. The proxy's own log stays silent about it.

**Wrong certificate name.** Removing `serverName: localhost` from `config.yaml`'s extension server block does not stop
the proxy from starting: nothing dials the extension server until the first payload needs a key, so a mismatched server
name is invisible at boot. The first Workflow start after that just hangs and times out, rather than failing with a
clear error:

```text
start workflow: context deadline exceeded
```

Again the proxy's log has nothing to say, and no execution is created either. The real problem only surfaces if you turn
on gRPC's own logging (`GRPC_GO_LOG_SEVERITY_LEVEL=info GRPC_GO_LOG_VERBOSITY_LEVEL=2`), where the proxy retries every
couple of seconds with:

```text
x509: cannot validate certificate for 127.0.0.1 because it doesn't contain any IP SANs
```

which is exactly the mismatch the comment above `serverName` in `config.yaml` describes: the certificate carries
`DNS:localhost` and no IP address, so verifying the dialed `127.0.0.1` fails without that setting. Put the line back and
restart the proxy to recover.

## What this is not

This provider is a teaching aid, not a key manager:

- the master secret lives in a plain environment variable (`KMS_MASTER_SECRET`);
- that secret also has to be actual secret material: HKDF extracts entropy from it rather than adding any, so it needs
  to be at least 32 bytes of cryptographic randomness, for example from `openssl rand -base64 32`, not a human-chosen
  passphrase. A passphrase here is directly brute-forceable, and recovering it yields every per-Namespace key at once.
  This example's own value, `example-master-secret`, is a passphrase, and that is fine only because this is a localhost
  demo, not something to copy into a real deployment;
- nothing on the provider side rotates: the same secret derives the same per-Namespace key forever; and
- losing that secret loses every payload ever sealed under it, with no recovery path.

Also worth flagging: the extension server here authenticates only with a bearer token over TLS, which is the right
choice for this example and matches what the proxy sends. `config.yaml`'s `tls` block for an extension server also
accepts `cert` and `key`, so a real extension server would likely add mutual TLS, requiring a client certificate
alongside the token.

For a real deployment, point the proxy's `encryption` block at one of the built-in schemes instead: `awskms://`,
`azurekeyvault://`, or `gcpkms://`.

## Running it again

The starter always uses the fixed Workflow ID `kms-example-greeting`. Run it again after the previous run has completed
and nothing special happens: the server's default Workflow ID reuse policy allows a new run once the old one has closed,
so you get a fresh run ID and `Hello, Temporal!` again with no cleanup required.

The case worth knowing about is a still-open execution, for example one started while the Worker was stopped. Here
neither the SDK nor the CLI report an already-started error the way you might expect: `ExecuteWorkflow` and
`temporal workflow start` both attach to the open run instead of rejecting the request, so the starter just blocks
waiting for a result that will not arrive until a Worker picks the run up. If that happens, either bring the Worker back
so the open run can finish, clear it directly with:

```bash
temporal workflow terminate --workflow-id kms-example-greeting --namespace default --address 127.0.0.1:7234
```

or restart the dev server, whose state lives entirely in memory.

## Optional: per-Namespace keys

Restarting the dev server below discards the `kms-example-greeting` execution from the sections above, since its state
lives entirely in memory; that is fine for this section, which stands on its own, but do it only once you are done
inspecting that execution.

Start the dev server with a second Namespace:

```bash
temporal server start-dev -n payments
```

This pre-creates both `default` and `payments` on the same server. Add an override to `config.yaml` so the proxy uses a
distinct key for `payments`:

```yaml
encryption:
  enabled: true
  cacheSize: 200
  default:
    uri: extension://kms/payloads
    duration: 1h
    renewBefore: 15m
  overrides:
    payments:
      uri: extension://kms/payments-payloads
      duration: 1h
      renewBefore: 15m
```

The override's URI has to differ from the default key's URI even though both point at the same extension server: as
noted above, reusing `extension://kms/payloads` for both fails proxy startup with
`duplicate key id: extension://kms/payloads`. Restart the proxy, then start a Workflow against the new Namespace; no
Worker is needed for this check, since the key exchange happens on the way in:

```bash
temporal workflow start --address 127.0.0.1:7234 --namespace payments --task-queue kms-example \
  --type GreetingWorkflow --workflow-id kms-payments-demo --input '"Payments"'
```

The extension server's log now shows a second, independent key coming into play, appended below the `default` line left
over from the earlier run in "The cache is working" above:

```text
wrapped a DEK: namespace=default plaintext=32B ciphertext=70B
wrapped a DEK: namespace=payments plaintext=32B ciphertext=71B
```

Same master secret, same extension server connection, but a different derived key per Namespace. Clean up with:

```bash
temporal workflow terminate --workflow-id kms-payments-demo --namespace payments --address 127.0.0.1:7234
```
