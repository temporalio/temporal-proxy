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
  setupTimeout: "120s",
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

// The dev server registers the default Namespace while its frontend is
// still coming up, so poll DescribeNamespace for that exact name until the
// frontend answers instead of trusting port readiness. The first failing
// poll also logs which Namespaces the cluster does list, for diagnosis.
export function setup() {
  client.connect(addr, { plaintext: true, reflect: true });

  const deadline = Date.now() + 90000;
  let first = true;
  for (;;) {
    const resp = client.invoke(
      "temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
      { namespace: "default" },
    );
    if (resp.status === grpc.StatusOK) {
      client.close();
      return;
    }
    if (first) {
      first = false;
      const list = client.invoke(
        "temporal.api.workflowservice.v1.WorkflowService/ListNamespaces",
        {},
      );
      const msg = list.message;
      console.log(
        "DescribeNamespace status=" +
          resp.status +
          " error=" +
          JSON.stringify(resp.error) +
          " | ListNamespaces status=" +
          list.status +
          " token=" +
          JSON.stringify(msg && msg.nextPageToken) +
          " namespaces=" +
          JSON.stringify(
            (msg && msg.namespaces ? msg.namespaces : []).map((n) => {
              const i = n.namespaceInfo || n.namespace_info || {};
              return i.namespace || n.namespace || "?";
            }),
          ),
      );
    }
    if (Date.now() > deadline) {
      throw new Error(
        "the default Namespace never became ready: DescribeNamespace status=" +
          resp.status +
          " error=" +
          JSON.stringify(resp.error),
      );
    }
    sleep(2);
  }
}

export default function () {
  if (__ITER === 0) {
    client.connect(addr, { plaintext: true, reflect: true });
  }

  const workflowId = `k6-${__VU}-${__ITER}-${Date.now()}`;
  const resp = client.invoke(
    "temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
    {
      namespace: "default",
      workflowId,
      requestId: workflowId,
      workflowType: { name: "k6.LoadTest" },
      taskQueue: { name: "k6-load-tq" },
      identity: `k6-vu-${__VU}`,
    },
  );

  check(resp, {
    "rpc ok": (r) => r && r.status === grpc.StatusOK,
  });

  sleep(0.01);
}
