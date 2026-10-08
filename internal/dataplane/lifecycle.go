package dataplane

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// Start opens every static upstream connection so an unreachable one fails
// startup, then binds and serves the gateway. It returns once the gateway is
// accepting. ctx bounds startup only and should carry a deadline, since it is
// what limits the wait for an upstream to answer; the gateway's serving
// goroutine gets the Context passed to New instead. A failure part-way through
// stops whatever already started.
func (d *Dataplane) Start(ctx context.Context) error {
	// A static upstream's connection is created during New, but gRPC does not
	// open a socket until it is used, so open them here: an unreachable upstream
	// fails startup instead of surfacing as request errors once the gateway is
	// already serving. Templated upstreams resolve per request and have nothing
	// to open yet.
	if err := connect.WaitReady(ctx, d.ready...); err != nil {
		return d.rollback(ctx, fmt.Errorf("upstream connection not ready: %w", err))
	}

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", d.hostPort)
	if err != nil {
		return d.rollback(ctx, fmt.Errorf("failed to create listener: %w", err))
	}

	d.mu.Lock()
	d.lis = lis
	d.addr = lis.Addr()
	d.mu.Unlock()

	d.serve(func() error { return d.gateway.Start(d.ctx, lis) })

	return nil
}

// Stop drains the gateway within its shutdown budget and closes the listener
// Start bound.
func (d *Dataplane) Stop(ctx context.Context) error {
	d.mu.Lock()
	d.stopping = true
	lis := d.lis
	d.lis = nil
	d.mu.Unlock()

	var errs []error
	if err := d.gateway.Stop(ctx); err != nil {
		errs = append(errs, err)
	}

	// A graceful stop closes the listener its server was serving on, but one
	// bound by a Start that failed before its goroutine reached Serve is not.
	// Closing here is what keeps that case from leaking a socket.
	if lis != nil {
		if err := lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// rollback undoes a partial Start and returns cause, so a failed Start leaves
// nothing serving. The shutdown deliberately does not inherit ctx: the usual
// reason to be here is that the startup deadline expired, and the rollback
// still has to finish.
func (d *Dataplane) rollback(ctx context.Context, cause error) error {
	if err := d.Stop(context.WithoutCancel(ctx)); err != nil {
		return errors.Join(cause, err)
	}

	return cause
}

// serve runs fn in the background and reports an unexpected exit through Abort
// exactly once. An error after Stop has begun is an ordinary shutdown race and
// is not reported.
func (d *Dataplane) serve(fn func() error) {
	go func() {
		err := fn()

		d.mu.Lock()
		stopping := d.stopping
		d.mu.Unlock()

		if err == nil || stopping {
			return
		}

		d.logger.Error("Dataplane stopped serving", tag.Error(err))
		d.abortOnce.Do(func() {
			if d.abort == nil {
				return
			}

			d.abort(fmt.Errorf("gateway stopped serving: %w", err))
		})
	}()
}
