// Command worker runs a Temporal worker that connects to the proxy at
// localhost:7233 and serves the cloud-example task queue. It carries no Cloud
// TLS or credentials; the proxy adds those on the way to Temporal Cloud.
package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/temporalio/temporal-proxy/examples/cloud"
)

func main() {
	c, err := client.Dial(client.Options{
		HostPort:  "localhost:7233",
		Namespace: os.Getenv("TEMPORAL_NAMESPACE"),
	})
	if err != nil {
		log.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	w := worker.New(c, cloud.TaskQueue, worker.Options{})
	w.RegisterWorkflow(cloud.GreetingWorkflow)
	w.RegisterActivity(cloud.ComposeGreeting)

	log.Printf("worker listening on task queue %q (namespace %q)", cloud.TaskQueue, os.Getenv("TEMPORAL_NAMESPACE"))
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("run worker: %v", err)
	}
}
