package resolve_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	"github.com/temporalio/temporal-proxy/internal/transport/resolve"
)

const (
	record  = "_grpc._tcp.frontend.example"
	refresh = 30 * time.Second
	minGap  = 5 * time.Second
)

type (
	// fakeLookup serves a fixed answer and counts calls.
	fakeLookup struct {
		mu      sync.Mutex
		calls   int
		records []*net.SRV
		err     error
	}

	// fakeCC records what a resolver reports.
	fakeCC struct {
		mu     sync.Mutex
		states []resolver.State
		errs   []error
		scJSON string
		scErr  error
	}
)

func TestBuildRejectsBadInput(t *testing.T) {
	t.Parallel()

	t.Run("malformed target", func(t *testing.T) {
		t.Parallel()

		_, err := newBuilder(&fakeLookup{}).Build(mustTarget(t, "srv://frontend.example"), &fakeCC{}, resolver.BuildOptions{})
		require.ErrorIs(t, err, resolve.ErrInvalidTarget)
	})

	t.Run("service config rejected", func(t *testing.T) {
		t.Parallel()

		cc := &fakeCC{scErr: errors.New("no round_robin")}
		_, err := newBuilder(&fakeLookup{}).Build(mustTarget(t, "srv:///"+record), cc, resolver.BuildOptions{})
		require.ErrorContains(t, err, "no round_robin")
	})
}

func TestResolverEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		records []*net.SRV
		want    []string
	}{
		{
			name:    "sorted by address",
			records: []*net.SRV{{Target: "b.example.", Port: 7233}, {Target: "a.example.", Port: 7233}},
			want:    []string{"a.example:7233", "b.example:7233"},
		},
		{
			name:    "duplicates collapse",
			records: []*net.SRV{{Target: "a.example.", Port: 7233}, {Target: "a.example", Port: 7233}},
			want:    []string{"a.example:7233"},
		},
		{
			name:    "same host on two ports stays two backends",
			records: []*net.SRV{{Target: "a.example.", Port: 7234}, {Target: "a.example.", Port: 7233}},
			want:    []string{"a.example:7233", "a.example:7234"},
		},
		{
			name:    "IPv6 literal is bracketed",
			records: []*net.SRV{{Target: "::1", Port: 7233}},
			want:    []string{"[::1]:7233"},
		},
		{
			name:    "dot target dropped",
			records: []*net.SRV{{Target: ".", Port: 0}, {Target: "a.example.", Port: 7233}},
			want:    []string{"a.example:7233"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				cc, _ := build(t, &fakeLookup{records: tt.records})
				synctest.Wait()

				states, errs := cc.snapshot()
				require.Empty(t, errs)
				require.Len(t, states, 1)
				require.Equal(t, tt.want, addrs(states[0]))
				require.NotNil(t, states[0].ServiceConfig, "the resolver supplies its own LB policy")
				require.JSONEq(t, `{"loadBalancingConfig":[{"round_robin":{}}]}`, cc.serviceConfig())
			})
		})
	}
}

func TestResolverReportsLookupFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		records []*net.SRV
		err     error
		want    string
	}{
		{name: "lookup error", err: errors.New("SERVFAIL"), want: "SERVFAIL"},
		{name: "no records", want: "no usable SRV records"},
		{name: "only dot records", records: []*net.SRV{{Target: "."}}, want: "no usable SRV records"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				cc, _ := build(t, &fakeLookup{records: tt.records, err: tt.err})
				synctest.Wait()

				states, errs := cc.snapshot()
				require.Empty(t, states, "a failed lookup must not push an empty backend list")
				require.Len(t, errs, 1)
				require.ErrorContains(t, errs[0], record, "the error names the record")
				require.ErrorContains(t, errs[0], tt.want)
			})
		})
	}
}

func TestResolverKeepsLastGoodState(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		lk := &fakeLookup{records: []*net.SRV{{Target: "a.example.", Port: 7233}}}
		cc, _ := build(t, lk)
		synctest.Wait()

		lk.set(nil, errors.New("SERVFAIL"))
		time.Sleep(refresh)
		synctest.Wait()

		states, errs := cc.snapshot()
		require.Len(t, states, 1, "the failed poll must not replace the good state")
		require.Len(t, errs, 1)
	})
}

func TestResolverRetriesFailedLookupAtGap(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		lk := &fakeLookup{} // no records yet, e.g. pods not ready
		cc, _ := build(t, lk)
		synctest.Wait()

		lk.set([]*net.SRV{{Target: "a.example.", Port: 7233}}, nil)

		time.Sleep(minGap - time.Second)
		synctest.Wait()
		require.Equal(t, 1, lk.count())

		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 2, lk.count(), "a failed lookup is retried at the gap, not the poll")

		states, _ := cc.snapshot()
		require.Len(t, states, 1)
	})
}

func TestResolverPollsOnInterval(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		lk := &fakeLookup{records: []*net.SRV{{Target: "a.example.", Port: 7233}}}
		cc, _ := build(t, lk)
		synctest.Wait()
		require.Equal(t, 1, lk.count(), "the first lookup runs right away")

		lk.set([]*net.SRV{{Target: "a.example.", Port: 7233}, {Target: "b.example.", Port: 7233}}, nil)

		time.Sleep(refresh - time.Second)
		synctest.Wait()
		require.Equal(t, 1, lk.count())

		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 2, lk.count())

		states, _ := cc.snapshot()
		require.Equal(t, []string{"a.example:7233", "b.example:7233"}, addrs(states[1]), "a scale-up is picked up by the poll")
	})
}

func TestResolverResolveNow(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		lk := &fakeLookup{records: []*net.SRV{{Target: "a.example.", Port: 7233}}}
		_, r := build(t, lk)
		synctest.Wait() // t=0: first lookup

		for range 3 {
			r.ResolveNow(resolver.ResolveNowOptions{})
		}
		synctest.Wait()
		require.Equal(t, 1, lk.count(), "a burst inside the gap waits for it")

		time.Sleep(minGap - time.Second)
		synctest.Wait()
		require.Equal(t, 1, lk.count())

		time.Sleep(time.Second) // t=5
		synctest.Wait()
		require.Equal(t, 2, lk.count(), "the burst collapses into one lookup at the gap")

		time.Sleep(refresh - time.Second) // t=34
		synctest.Wait()
		require.Equal(t, 2, lk.count(), "the poll re-arms from the last lookup")

		time.Sleep(time.Second) // t=35
		synctest.Wait()
		require.Equal(t, 3, lk.count())

		time.Sleep(10 * time.Second) // t=45, gap long past
		r.ResolveNow(resolver.ResolveNowOptions{})
		synctest.Wait()
		require.Equal(t, 4, lk.count(), "past the gap, ResolveNow looks up immediately")
	})
}

func TestResolverCloseStopsPolling(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		lk := &fakeLookup{records: []*net.SRV{{Target: "a.example.", Port: 7233}}}
		_, r := build(t, lk)
		synctest.Wait()

		r.Close()
		time.Sleep(time.Hour)
		synctest.Wait()
		require.Equal(t, 1, lk.count())
	})
}

func TestResolverCloseDuringLookup(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		cc := &fakeCC{}
		blocking := func(ctx context.Context, _ string) ([]*net.SRV, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}

		r, err := resolve.NewTestBuilder(blocking, refresh, minGap).
			Build(mustTarget(t, "srv:///"+record), cc, resolver.BuildOptions{})
		require.NoError(t, err)
		synctest.Wait() // lookup in flight

		r.Close() // returns only once the goroutine has exited

		states, errs := cc.snapshot()
		require.Empty(t, states)
		require.Empty(t, errs, "nothing is reported to a closed ClientConn")
	})
}

func TestResolverLookupTimesOut(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		cc := &fakeCC{}
		blocking := func(ctx context.Context, _ string) ([]*net.SRV, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}

		r, err := resolve.NewTestBuilder(blocking, refresh, minGap).
			Build(mustTarget(t, "srv:///"+record), cc, resolver.BuildOptions{})
		require.NoError(t, err)
		t.Cleanup(r.Close)

		time.Sleep(10 * time.Second)
		synctest.Wait()

		_, errs := cc.snapshot()
		require.Len(t, errs, 1)
		require.ErrorIs(t, errs[0], context.DeadlineExceeded)
	})
}

// UpdateState records s.
func (c *fakeCC) UpdateState(s resolver.State) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.states = append(c.states, s)
	return nil
}

// ReportError records err.
func (c *fakeCC) ReportError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.errs = append(c.errs, err)
}

// NewAddress is deprecated in gRPC and unused by the resolver.
func (c *fakeCC) NewAddress([]resolver.Address) {}

// ParseServiceConfig records the JSON it was given and fails with scErr when set.
func (c *fakeCC) ParseServiceConfig(js string) *serviceconfig.ParseResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.scJSON = js
	return &serviceconfig.ParseResult{Err: c.scErr}
}

// serviceConfig returns the JSON passed to ParseServiceConfig.
func (c *fakeCC) serviceConfig() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.scJSON
}

// snapshot returns copies of the recorded states and errors.
func (c *fakeCC) snapshot() ([]resolver.State, []error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]resolver.State(nil), c.states...), append([]error(nil), c.errs...)
}

// count returns how many lookups ran.
func (f *fakeLookup) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

// lookup answers with the configured records and error.
func (f *fakeLookup) lookup(context.Context, string) ([]*net.SRV, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	return f.records, f.err
}

// set replaces the answer for later lookups.
func (f *fakeLookup) set(records []*net.SRV, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.records, f.err = records, err
}

// addrs flattens s to one address per endpoint.
func addrs(s resolver.State) []string {
	out := make([]string, 0, len(s.Endpoints))
	for _, ep := range s.Endpoints {
		parts := make([]string, 0, len(ep.Addresses))
		for _, a := range ep.Addresses {
			parts = append(parts, a.Addr)
		}

		out = append(out, strings.Join(parts, ","))
	}

	return out
}

// build starts a resolver for record over lk with the production intervals and
// closes it when the test ends.
func build(t *testing.T, lk *fakeLookup) (*fakeCC, resolver.Resolver) {
	t.Helper()

	cc := &fakeCC{}
	r, err := newBuilder(lk).Build(mustTarget(t, "srv:///"+record), cc, resolver.BuildOptions{})
	require.NoError(t, err)
	t.Cleanup(r.Close)

	return cc, r
}

// newBuilder returns a Builder over lk with the production intervals.
func newBuilder(lk *fakeLookup) *resolve.Builder {
	return resolve.NewTestBuilder(lk.lookup, refresh, minGap)
}
