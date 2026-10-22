// k6 gRPC load test for temporal-proxy.  Sends StartWorkflowExecution
// calls through the proxy using server reflection.
//
// Run via:  mise run profile:k6
//   or:     docker compose --profile load run --rm k6 run /scripts/grpc-load.js
//
// Environment:
//   K6_VUS       virtual users   (default 10)
//   K6_DURATION  test duration   (default 60s)
//   PROXY_ADDR   proxy host:port (default proxy:8233)

import grpc from "k6/net/grpc";
import { check, sleep } from "k6";

const client = new grpc.Client();

export const options = {
  scenarios: {
    steady: {
      executor: "constant-vus",
      vus: parseInt(__ENV.K6_VUS || "10"),
      duration: __ENV.K6_DURATION || "60s",
    },
  },
  thresholds: {
    checks: ["rate>0.95"],
  },
};

const addr = __ENV.PROXY_ADDR || "proxy:8233";

export default function () {
  if (__ITER === 0) {
    client.connect(addr, { plaintext: true, reflect: true });
  }

  const resp = client.invoke(
    "temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
    {
      namespace: "default",
      workflow_id: `k6-${__VU}-${__ITER}-${Date.now()}`,
      workflow_type: { name: "k6.LoadTest" },
      task_queue: { name: "k6-load-tq" },
      identity: `k6-vu-${__VU}`,
    },
  );

  check(resp, {
    "rpc ok": (r) => r && r.status === grpc.StatusOK,
  });

  sleep(0.01);
}
