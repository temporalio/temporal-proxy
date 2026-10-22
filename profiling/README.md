# Profiling

A self-contained Docker Compose stack for profiling temporal-proxy under
load.  It starts a Temporal dev server, builds the proxy from source (with
debug symbols), and wires continuous profiling through two independent
backends so you can compare their flamechart UIs side by side.

## What runs

| Service     | Port  | Purpose                                          |
| ----------- | ----- | ------------------------------------------------ |
| proxy       | 8233  | gRPC gateway (the thing being profiled)          |
| Pyroscope   | 4040  | Continuous profiling backend (Grafana ecosystem) |
| Grafana     | 3000  | Flamecharts via the Pyroscope datasource         |
| Parca       | 7070  | Continuous profiling backend (standalone UI)     |
| Temporal    | 7233  | Dev server the proxy forwards to                 |
| Alloy       | —     | Scrapes pprof from the proxy, pushes to Pyroscope|
| k6          | —     | gRPC load generator (on-demand)                  |

Alloy and Parca share the proxy container's network stack (compose
`network_mode: "service:proxy"`) so they can scrape `/debug/pprof` on
loopback without auth or TLS.

## Quick start

```bash
# Start the stack (detached by default)
mise run profile:up

# Send gRPC load through the proxy for 60 s with 10 virtual users
mise run profile:k6

# Open flamecharts
#   Grafana (Pyroscope): http://localhost:3000
#   Parca:               http://localhost:7070

# Tear down
mise run profile:down
```

## k6 flags

```bash
mise run profile:k6 -v 50 -d 5m        # 50 VUs for 5 minutes
mise run profile:k6 -s custom.js        # custom script from profiling/k6/
```

## Collecting evidence

After a load run, open Grafana or Parca, navigate to the flamechart for
`temporal-proxy`, set the time range to the load window, and screenshot.
Host screenshots on a persistent fork branch so PR evidence URLs survive.
