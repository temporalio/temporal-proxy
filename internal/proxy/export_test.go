package proxy

import (
	"context"

	"go.temporal.io/proxy/pkg/codec"
)

var (
	// CrossingOf exposes crossingOf to the guard that pins it against the API.
	CrossingOf = crossingOf

	// ErrResponseTooLarge and ErrDecodeBusy expose the limiter's errors, so a
	// test can tell which limit refused a charge.
	ErrResponseTooLarge = errResponseTooLarge
	ErrDecodeBusy       = errDecodeBusy
)

// DecodeBudgetFrom exposes the budget Open put on ctx, which is nil when it
// has none.
func DecodeBudgetFrom(ctx context.Context) codec.DecodeBudget {
	if b := budgetFrom(ctx); b != nil {
		return b
	}

	return nil
}
