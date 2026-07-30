package kms

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// These tests stay in package kms because they are the only ones that cannot be
// driven through Module. runRotation needs a refresher that fails on demand and
// an interval short enough to advance, and neither is reachable from outside: the
// module always pairs the loop with a real vault and a fixed interval. Everything
// else about the module is exercised externally in fx_test.go.
type refresherFunc func() error

func TestRunRotation_RefreshesImmediatelyThenEachInterval(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		r := refresherFunc(func() error {
			calls.Add(1)
			return nil
		})

		ctx, cancel := context.WithCancel(t.Context())
		go runRotation(ctx, r, time.Second, logger.NewNoopLogger())

		// The first refresh runs immediately; the goroutine then blocks on the
		// interval timer.
		synctest.Wait()
		require.Equal(t, int64(1), calls.Load())

		// Advance past three ticks (t=1s, 2s, 3s); the fourth at t=4s has not fired.
		time.Sleep(3500 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, int64(4), calls.Load())

		// Cancellation stops the loop and no further refreshes occur.
		cancel()
		synctest.Wait()
		require.Equal(t, int64(4), calls.Load())
	})
}

func TestRunRotation_ContinuesAfterRefreshError(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		r := refresherFunc(func() error {
			calls.Add(1)
			return errors.New("kms unavailable")
		})

		ctx, cancel := context.WithCancel(t.Context())
		go runRotation(ctx, r, time.Second, logger.NewNoopLogger())

		synctest.Wait()
		require.Equal(t, int64(1), calls.Load())

		// A failing Refresh is logged and the loop keeps ticking.
		time.Sleep(2500 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, int64(3), calls.Load())

		cancel()
		synctest.Wait()
	})
}

func (f refresherFunc) Refresh() error { return f() }
