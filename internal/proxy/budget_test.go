package proxy_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.temporal.io/proxy/internal/proxy"
)

// TestDecodeLimiterResponseLimit shows a response is refused a charge that
// takes it past its own limit, with ResourceExhausted, even with room left in
// the pool, and that a refused charge holds nothing. Comparing with >= instead
// of > fails the exact fit; keeping a refused charge fails the last one.
func TestDecodeLimiterResponseLimit(t *testing.T) {
	t.Parallel()

	ctx, release := proxy.NewDecodeLimiter(100).Open(t.Context(), 10)
	t.Cleanup(release)

	b := proxy.DecodeBudgetFrom(ctx)
	require.NoError(t, b.Charge(6))

	err := b.Charge(5)
	require.ErrorIs(t, err, proxy.ErrResponseTooLarge)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	require.NoError(t, b.Charge(4))
}

// TestDecodeLimiterPool shows responses share the pool: one is refused, with
// ResourceExhausted, what another holds, gets it once that one is released, and
// an inner Open on a context that already has a budget releases nothing early.
// Releasing on the inner Open fails the busy check after it; never releasing
// fails the last charge.
func TestDecodeLimiterPool(t *testing.T) {
	t.Parallel()

	l := proxy.NewDecodeLimiter(10)

	first, releaseFirst := l.Open(t.Context(), 10)
	inner, releaseInner := l.Open(first, 10)
	require.NoError(t, proxy.DecodeBudgetFrom(inner).Charge(8))
	releaseInner()

	second, releaseSecond := l.Open(t.Context(), 10)
	t.Cleanup(releaseSecond)

	err := proxy.DecodeBudgetFrom(second).Charge(4)
	require.ErrorIs(t, err, proxy.ErrDecodeBusy)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	releaseFirst()
	require.NoError(t, proxy.DecodeBudgetFrom(second).Charge(10))
}

// TestDecodeLimiterNoSpuriousRefusal shows a charge that can never fit doesn't
// get a concurrent one that does refused. Adding before checking and rolling
// back after lets the charge that can't fit briefly hold the pool, and the one
// that fits fails while it does.
func TestDecodeLimiterNoSpuriousRefusal(t *testing.T) {
	t.Parallel()

	l := proxy.NewDecodeLimiter(10)

	held, releaseHeld := l.Open(t.Context(), 10)
	t.Cleanup(releaseHeld)
	require.NoError(t, proxy.DecodeBudgetFrom(held).Charge(5))

	var (
		done = make(chan struct{})
		wg   sync.WaitGroup
	)

	wg.Go(func() {
		big, release := l.Open(t.Context(), 10)
		defer release()

		for {
			select {
			case <-done:
				return
			default:
				_ = proxy.DecodeBudgetFrom(big).Charge(6)
			}
		}
	})

	for range 100_000 {
		ctx, release := l.Open(t.Context(), 10)
		err := proxy.DecodeBudgetFrom(ctx).Charge(5)
		release()

		if err != nil {
			close(done)
			wg.Wait()
			require.NoError(t, err)
		}
	}

	close(done)
	wg.Wait()
}

// testResponseLimit is the response limit tests that aren't about decode
// limits use, the default upstream maxResponseSize.
const testResponseLimit = 128 << 20

// testLimiter returns a pool too large for any test that isn't about decode
// limits to reach.
func testLimiter() *proxy.DecodeLimiter {
	return proxy.NewDecodeLimiter(1 << 40)
}
