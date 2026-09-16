package dataplane

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// Start binds and serves every upstream socket, opens every static upstream
// connection so an unreachable one fails startup, then binds and serves the
// gateway, in that order. It returns once the gateway is accepting. ctx bounds
// startup only and should carry a deadline, since it is what limits the wait for
// an upstream to answer; the serving goroutines get the Context passed to New
// instead. A failure part-way through stops whatever already started.
func (d *Dataplane) Start(ctx context.Context) error {
	for _, up := range d.upstreams {
		// Bind synchronously so the socket is listening before the gateway routes
		// anything to it, then serve in the background.
		lis, err := up.svr.Listen(ctx)
		if err != nil {
			return d.rollback(ctx, fmt.Errorf("failed to start proxy for upstream %q: %w", up.name, err))
		}

		d.track(lis)
		d.serve(fmt.Sprintf("upstream %q", up.name), func() error {
			return up.svr.Start(d.ctx, lis)
		})
	}

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

	d.track(lis)

	d.mu.Lock()
	d.addr = lis.Addr()
	d.mu.Unlock()

	d.serve("gateway", func() error { return d.gateway.Start(d.ctx, lis) })

	return nil
}

// Stop drains the gateway first, so no request is admitted for a tier that is
// going away, then every upstream proxy. Each tier's drain is bounded, so the
// upstreams go concurrently: they are independent, and serially their budgets
// would sum, which is how a shutdown overruns the lifecycle deadline and strands
// the hooks queued behind this one.
func (d *Dataplane) Stop(ctx context.Context) error {
	d.mu.Lock()
	d.stopping = true
	listeners := d.listeners
	d.listeners = nil
	d.mu.Unlock()

	var errs []error
	if err := d.gateway.Stop(ctx); err != nil {
		errs = append(errs, err)
	}

	// Indexed rather than appended, so the slot is written without coordinating
	// and the report stays in upstream order.
	upstreamErrs := make([]error, len(d.upstreams))

	var wg sync.WaitGroup
	for i, up := range d.upstreams {
		wg.Go(func() {
			if err := up.svr.Stop(ctx); err != nil {
				upstreamErrs[i] = fmt.Errorf("upstream %q: %w", up.name, err)
			}
		})
	}

	wg.Wait()
	errs = append(errs, upstreamErrs...)

	// A graceful stop closes the listeners its server was serving on, but one
	// bound by a Start that failed before its goroutine reached Serve is not
	// among them. Closing here is what keeps that case from leaking a socket.
	for _, lis := range listeners {
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
func (d *Dataplane) serve(name string, fn func() error) {
	go func() {
		err := fn()

		d.mu.Lock()
		stopping := d.stopping
		d.mu.Unlock()

		if err == nil || stopping {
			return
		}

		d.logger.Error("Dataplane stopped serving", tag.String("tier", name), tag.Error(err))
		d.abortOnce.Do(func() {
			if d.abort == nil {
				return
			}

			d.abort(fmt.Errorf("%s stopped serving: %w", name, err))
		})
	}()
}

// track records a listener so Stop can close one whose server never reached
// Serve, which a graceful stop would otherwise leave open.
func (d *Dataplane) track(lis net.Listener) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.listeners = append(d.listeners, lis)
}
