package router

import (
	"github.com/temporalio/temporal-proxy/internal/config"
)

// MuxFor builds the Mux for prepared routing configuration. Panics if the
// routing was never prepared by [config.Config.Prepare].
func MuxFor(r config.Routing) *Mux {
	rules := make([]Rule, 0, len(r.Rules))
	for i := range r.Rules {
		ns, meta := r.Rules[i].Matchers()
		rules = append(rules, Rule{upstream: r.Rules[i].Upstream, ns: ns, meta: meta})
	}

	return New(r.DefaultUpstream, r.SystemUpstream, rules...)
}
