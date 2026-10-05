package resolve

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const (
	// Scheme is the target scheme the SRV resolver handles.
	Scheme = "srv"

	// defaultRefresh is how often the record set is re-read. LookupSRV does not
	// expose TTLs, so this is a fixed poll, and it is what notices a scale-up:
	// healthy backends never ask for a re-resolve.
	defaultRefresh = 30 * time.Second

	// defaultMinGap is the shortest time between a lookup and one requested
	// through ResolveNow, which gRPC calls when a backend's connection fails.
	defaultMinGap = 5 * time.Second

	// lookupTimeout bounds a single SRV lookup.
	lookupTimeout = 10 * time.Second

	// roundRobin spreads calls across every backend instead of pinning to one.
	roundRobin = `{"loadBalancingConfig":[{"round_robin":{}}]}`
)

var (
	// ErrInvalidTarget is wrapped by every error for a malformed srv:/// target.
	ErrInvalidTarget = errors.New("invalid srv target")

	errNoRecords = errors.New("no usable SRV records")
)

type (
	// Builder is a gRPC resolver.Builder for srv:/// targets. Each connection
	// gets a resolver that reads the named SRV record, balances round-robin
	// across the backends it lists, and re-reads it on a fixed interval.
	// Construct one with NewSRVBuilder.
	Builder struct {
		lookup  lookupFunc
		refresh time.Duration
		minGap  time.Duration
	}

	// lookupFunc returns the records for an SRV name.
	lookupFunc func(context.Context, string) ([]*net.SRV, error)

	// srvResolver polls one SRV record for one ClientConn. A single goroutine
	// owns every lookup, so lookups never overlap.
	srvResolver struct {
		cc         resolver.ClientConn
		name       string
		lookup     lookupFunc
		refresh    time.Duration
		minGap     time.Duration
		sc         *serviceconfig.ParseResult
		cancel     context.CancelFunc
		resolveNow chan struct{}
		done       chan struct{}
	}
)

// NewSRVBuilder returns a Builder that looks records up through the system
// resolver, re-reads them every 30s, and honors ResolveNow at most once per 5s.
func NewSRVBuilder() *Builder {
	return &Builder{
		lookup:  lookupSRV,
		refresh: defaultRefresh,
		minGap:  defaultMinGap,
	}
}

// IsTarget reports whether target uses the srv scheme. It does not check the
// rest of the target; ParseTarget does.
func IsTarget(target string) bool {
	return strings.HasPrefix(target, Scheme+":")
}

// ParseTarget returns the SRV record name in an srv:///<name> target, or an
// error wrapping ErrInvalidTarget when target is not one.
func ParseTarget(target string) (string, error) {
	u, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("%w %q: %v", ErrInvalidTarget, target, err)
	}

	if u.Scheme != Scheme {
		return "", fmt.Errorf("%w %q: scheme must be %q", ErrInvalidTarget, target, Scheme)
	}

	return recordName(resolver.Target{URL: *u})
}

// Build starts a resolver for target. The first lookup runs in the background,
// so Build does not block on DNS; gRPC calls it when the connection first leaves
// idle, not in grpc.NewClient.
func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	name, err := recordName(target)
	if err != nil {
		return nil, err
	}

	sc := cc.ParseServiceConfig(roundRobin)
	if sc.Err != nil {
		return nil, fmt.Errorf("srv: parse service config: %w", sc.Err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &srvResolver{
		cc:         cc,
		name:       name,
		lookup:     b.lookup,
		refresh:    b.refresh,
		minGap:     b.minGap,
		sc:         sc,
		cancel:     cancel,
		resolveNow: make(chan struct{}, 1),
		done:       make(chan struct{}),
	}

	go r.run(ctx)
	return r, nil
}

// OverrideAuthority names the connection after the record's domain rather than
// the record itself: it drops a trailing dot and every leading label that starts
// with an underscore, so _grpc._tcp.frontend.example becomes frontend.example,
// the name a certificate would carry. A name that is all such labels is returned
// unchanged. An explicit TLS server name still takes precedence.
func (b *Builder) OverrideAuthority(target resolver.Target) string {
	name := target.Endpoint()

	rest := strings.TrimSuffix(name, ".")
	for strings.HasPrefix(rest, "_") {
		_, after, ok := strings.Cut(rest, ".")
		if !ok {
			return name
		}

		rest = after
	}

	if rest == "" {
		return name
	}

	return rest
}

// Scheme returns the scheme this builder resolves.
func (b *Builder) Scheme() string {
	return Scheme
}

// Close stops polling and waits for an in-flight lookup to unwind. It is safe to
// call more than once.
func (r *srvResolver) Close() {
	r.cancel()
	<-r.done
}

// ResolveNow asks for an early lookup, no sooner than the minimum gap after the
// last one. It never blocks; requests made while one is pending collapse into it.
func (r *srvResolver) ResolveNow(resolver.ResolveNowOptions) {
	select {
	case r.resolveNow <- struct{}{}:
	default:
	}
}

// run owns the lookup schedule. One timer holds the next lookup: it starts at
// zero, is re-armed for the poll interval after each lookup, and ResolveNow only
// ever pulls it earlier, to the minimum gap.
func (r *srvResolver) run(ctx context.Context) {
	defer close(r.done)

	timer := time.NewTimer(0)
	defer timer.Stop()

	var last time.Time
	due := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.resolveNow:
			if soon := last.Add(r.minGap); soon.Before(due) {
				due = soon
				timer.Reset(time.Until(due))
			}

			continue
		case <-timer.C:
		}

		// A failure is retried at the gap: with no backends there is no failing
		// connection to prompt a ResolveNow, so waiting for the poll would leave a
		// cold start without backends for the whole interval.
		wait := r.refresh
		if !r.resolve(ctx) {
			wait = r.minGap
		}

		last = time.Now()
		due = last.Add(wait)
		timer.Reset(wait)
	}
}

// resolve runs one lookup, reports it, and returns whether it succeeded. A failed
// lookup, or one with no usable records, is reported as an error without touching
// the current state, so a DNS blip leaves gRPC on the last good backends instead
// of none.
func (r *srvResolver) resolve(ctx context.Context) bool {
	lctx, cancel := context.WithTimeout(ctx, lookupTimeout)
	defer cancel()

	records, err := r.lookup(lctx, r.name)
	if ctx.Err() != nil {
		return false // closed mid-lookup; the ClientConn is gone
	}

	eps := endpoints(records)
	if err == nil && len(eps) == 0 {
		err = errNoRecords
	}

	if err != nil {
		r.cc.ReportError(fmt.Errorf("srv: lookup %q: %w", r.name, err))
		return false
	}

	// An error here means the balancer rejected the state; the next poll sends a
	// fresh one, so there is nothing more to do with it.
	_ = r.cc.UpdateState(resolver.State{Endpoints: eps, ServiceConfig: r.sc})
	return true
}

// endpoints turns records into one sorted, de-duplicated endpoint per host:port.
// Priority and weight are ignored, since round_robin treats backends equally. A
// target of "." means the service is not offered there (RFC 2782) and is dropped.
func endpoints(records []*net.SRV) []resolver.Endpoint {
	addrs := make([]string, 0, len(records))
	for _, rec := range records {
		host := strings.TrimSuffix(rec.Target, ".")
		if host == "" {
			continue
		}

		addrs = append(addrs, net.JoinHostPort(host, strconv.Itoa(int(rec.Port))))
	}

	slices.Sort(addrs)
	addrs = slices.Compact(addrs)

	eps := make([]resolver.Endpoint, len(addrs))
	for i, addr := range addrs {
		eps[i] = resolver.Endpoint{Addresses: []resolver.Address{{Addr: addr}}}
	}

	return eps
}

// lookupSRV queries name as written, with no _service._proto prefix added.
func lookupSRV(ctx context.Context, name string) ([]*net.SRV, error) {
	_, records, err := net.DefaultResolver.LookupSRV(ctx, "", "", name)
	return records, err
}

// recordName extracts the SRV record name from t. The name must sit in the path
// (srv:///name): a URL host means the target was written srv://name, an opaque
// one means srv:name (or a host:port whose host is "srv"), and a port or further
// path has no place since ports come from the records.
func recordName(t resolver.Target) (string, error) {
	raw := t.URL.String()
	if t.URL.Host != "" || t.URL.Opaque != "" {
		return "", fmt.Errorf("%w %q: use %s:///<name>; a DNS server authority is not supported", ErrInvalidTarget, raw, Scheme)
	}

	name := t.Endpoint()
	switch {
	case name == "":
		return "", fmt.Errorf("%w %q: missing SRV record name", ErrInvalidTarget, raw)
	case strings.ContainsAny(name, ":/"):
		return "", fmt.Errorf("%w %q: want a bare SRV record name; ports come from the records", ErrInvalidTarget, raw)
	}

	return name, nil
}
