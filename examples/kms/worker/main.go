// Command worker runs a Temporal worker that connects to the proxy and serves
// the kms-example task queue. It exchanges cleartext payloads and knows nothing
// about encryption; the proxy seals them on the way to the Temporal Service.
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/temporalio/temporal-proxy/examples/kms"
)

func main() {
	c, err := client.Dial(client.Options{
		HostPort:  kms.ProxyHostPort,
		Namespace: kms.Namespace,
	})
	if err != nil {
		log.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	w := worker.New(c, kms.TaskQueue, worker.Options{})
	w.RegisterWorkflow(kms.GreetingWorkflow)
	w.RegisterActivity(kms.ComposeGreeting)

	log.Printf("worker listening on task queue %q (namespace %q)", kms.TaskQueue, kms.Namespace)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("run worker: %v", err)
	}
}
