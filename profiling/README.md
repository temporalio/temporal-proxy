# Profiling

A Docker Compose stack for profiling temporal-proxy under real gRPC load.
One `docker compose up` brings up everything an evidence pass needs on any
Docker host, including a GCP VM: the proxy built from this source tree, a
dev server it forwards to, two independent continuous-profiling backends,
Grafana, and a k6 load generator.

Containers are deliberate here: Pyroscope, Parca, Grafana, Alloy, k6,
postgres, and the Temporal dev server all publish official images, so the
stack needs nothing but Docker and reproduces the same topology every
time. The proxy is the one image built in this directory, because the root
Dockerfile only packages a goreleaser artifact instead of building from
source.

## What runs

| Service   | Port | Purpose                                         |
| --------- | ---- | ----------------------------------------------- |
| proxy     | 8233 | gRPC gateway (the process being profiled)       |
| proxy     | 9090 | Prometheus metrics (`/metrics`)                 |
| Grafana   | 3000 | Flamecharts through the Pyroscope datasource    |
| Parca     | 7070 | Independent flamecharts, native scraper         |
| Pyroscope | -    | Profiling database (in-network only)            |
| Temporal  | 7233 | Dev server the proxy forwards to                |
| postgres  | -    | Storage for the dev server                      |
| Alloy     | -    | Pull collector: scrapes pprof, writes Pyroscope |
| k6        | -    | gRPC load generator (on demand)                 |

The two backends collect profiles differently because their projects
document it that way:

- **Pyroscope only accepts profiles; it does not pull them.** Grafana's
  pull-mode setup has a collector scrape `/debug/pprof/*` and write to
  Pyroscope, and that collector is **Alloy** (`pyroscope.scrape` plus
  `pyroscope.write`).
- **Parca scrapes on its own.** Its configuration file carries built-in
  `scrape_configs`, so no collector is involved.

Alloy and Parca both reach the pprof routes on `127.0.0.1:8445` by
sharing the proxy container's network stack (compose
`network_mode: "service:proxy"`). That is what makes the setup valid: the
proxy only allows unauthenticated pprof on a loopback bind
(`internal/config/http.go`), and sidecars on that loopback are exactly
the intended consumers.

Every published port binds `127.0.0.1` only, so nothing is exposed off
the machine; reach the UIs through an SSH tunnel.

## Quick start

```bash
# Start the stack (detached, returns once healthchecks pass)
mise run profile:up

# Send gRPC load for 60 s with 10 virtual users. The script waits for the
# dev server's `default` Namespace to exist before the load begins.
mise run profile:k6

# Tear down
mise run profile:down
```

## k6 flags

```bash
mise run profile:k6 -v 50 -d 5m        # 50 VUs for 5 minutes
mise run profile:k6 -s custom.js        # custom script from profiling/k6/
```

The script discovers the API through gRPC server reflection (allowed in
`proxy-config.yaml`), so it needs no proto files.

## Collecting evidence

Scrape intervals are 60 s (Parca, 10 s CPU windows) and 30 s (Alloy, 8 s
CPU windows). The staggered short windows matter: a Go process answers
only one CPU profile at a time, and both collectors pull the same pprof
endpoint, so keep the load running for at least a minute before
screenshotting. On a VM, tunnel the UIs instead of opening firewall
ports:

```bash
gcloud compute ssh INSTANCE -- -N \
  -L 3000:localhost:3000 -L 7070:localhost:7070
```

1. `mise run profile:up`, then `mise run profile:k6 -d 3m`.
2. Grafana at <http://localhost:3000> (no login): Explore, Pyroscope
   datasource, service `temporal-proxy`, profile type CPU, view Flame
   Graph, time range covering the load window. Screenshot.
3. Parca at <http://localhost:7070>: pick the `temporal-proxy` process,
   view Flame Graph (or Table). Screenshot.
4. Host both PNGs on a persistent fork branch and link them from the PR
   body so the evidence URLs survive.
