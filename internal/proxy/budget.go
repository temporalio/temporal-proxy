package proxy

import (
	"context"
	"errors"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// errResponseTooLarge refuses a response that decodes past its own limit. It
	// is the code gRPC answers a response with that arrives past it.
	errResponseTooLarge = &limitError{msg: "proxy: response decodes past the response size limit"}

	// errDecodeBusy refuses a response when every decoded byte the proxy allows
	// at once is already held by others. It is retryable once they finish.
	errDecodeBusy = &limitError{msg: "proxy: too many decoded bytes in flight"}

	// errNoBudget refuses a charge with no budget open, which is a wiring
	// mistake; decoding without one would be unbounded.
	errNoBudget = errors.New("proxy: no decode budget is open")
)

type (
	// DecodeLimiter bounds the bytes decoded payloads hold across every response
	// in flight, so concurrent responses that each stay under their own limit
	// still can't exhaust memory together. A charge that doesn't fit is refused
	// at once rather than waited on: a response holding part of the pool while
	// it waits for more could stall the rest until their deadlines.
	DecodeLimiter struct {
		max  int64
		used atomic.Int64
	}

	// responseBudget is one response's share of a [DecodeLimiter], bounded by
	// the response's own limit. used is what it holds from the pool.
	responseBudget struct {
		pool  *DecodeLimiter
		limit int64
		used  atomic.Int64
	}

	// limitError is a refused charge, answered with ResourceExhausted.
	limitError struct {
		msg string
	}

	// budgetKey carries a responseBudget on a context.
	budgetKey struct{}
)

// NewDecodeLimiter returns a [DecodeLimiter] that lets responses hold at most
// max decoded bytes at once.
func NewDecodeLimiter(max int64) *DecodeLimiter {
	return &DecodeLimiter{max: max}
}

// Open returns ctx carrying a budget that lets one response decode at most
// limit bytes from l, and the func that returns them to l once the response
// is done with them. A ctx that already carries a budget is returned as is,
// with a release that does nothing, so whoever opened it first decides how
// long the bytes are held.
func (l *DecodeLimiter) Open(ctx context.Context, limit int64) (context.Context, func()) {
	if budgetFrom(ctx) != nil {
		return ctx, func() {}
	}

	b := &responseBudget{pool: l, limit: limit}

	return context.WithValue(ctx, budgetKey{}, b), b.release
}

// take holds n more bytes, unless that would pass max. It checks before it
// adds, so a charge that doesn't fit never holds bytes a concurrent one needs.
func (l *DecodeLimiter) take(n int64) bool {
	for {
		cur := l.used.Load()
		if cur+n > l.max {
			return false
		}

		if l.used.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

// Charge holds n more decoded bytes for the response, refusing them past the
// response's limit or when the pool is full. A refused charge holds nothing. A
// nil budget, what a context with none yields, refuses everything. Under
// contention a charge the pool refuses can briefly count against the response,
// so a concurrent one may be refused as too large rather than busy.
func (b *responseBudget) Charge(n int) error {
	if b == nil {
		return errNoBudget
	}

	m := int64(n)
	if b.used.Add(m) > b.limit {
		b.used.Add(-m)
		return errResponseTooLarge
	}

	if !b.pool.take(m) {
		b.used.Add(-m)
		return errDecodeBusy
	}

	return nil
}

// release returns everything the response holds to the pool. It must not run
// while a Charge can: one between its two steps would return bytes the pool
// never granted. Callers release only after decoding returns, and the SDK's
// payload visitor waits for every goroutine before it does.
func (b *responseBudget) release() {
	b.pool.used.Add(-b.used.Swap(0))
}

func (e *limitError) Error() string { return e.msg }

// GRPCStatus makes a refused charge answer ResourceExhausted, however it was
// wrapped on the way out.
func (e *limitError) GRPCStatus() *status.Status {
	return status.New(codes.ResourceExhausted, e.msg)
}

// budgetFrom returns the budget [DecodeLimiter.Open] put on ctx, or nil.
func budgetFrom(ctx context.Context) *responseBudget {
	b, _ := ctx.Value(budgetKey{}).(*responseBudget)
	return b
}
