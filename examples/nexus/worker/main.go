// Command worker runs a Temporal worker that connects to the proxy at
// localhost:7234 and serves the nexus-example task queue: the caller workflow,
// the handler workflow, and the Nexus Service.
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	hello "go.temporal.io/proxy/examples/nexus"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "localhost:7234"})
	if err != nil {
		log.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	svc, err := hello.NewService()
	if err != nil {
		log.Fatalf("build nexus service: %v", err)
	}

	w := worker.New(c, hello.TaskQueue, worker.Options{})
	w.RegisterWorkflow(hello.CallerWorkflow)
	w.RegisterWorkflow(hello.HelloHandlerWorkflow)
	w.RegisterNexusService(svc)

	log.Printf("worker listening on task queue %q", hello.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("run worker: %v", err)
	}
}
