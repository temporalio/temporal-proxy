package resolve

import (
	"context"
	"net"
	"time"
)

// NewTestBuilder returns a Builder with its lookup and intervals replaced.
func NewTestBuilder(
	lookup func(context.Context, string) ([]*net.SRV, error),
	refresh, minGap time.Duration,
) *Builder {
	return &Builder{lookup: lookup, refresh: refresh, minGap: minGap}
}
