package protoutil

import (
	"go.uber.org/fx"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Module provides an [*Extractor]. Files and Types default to the global
// protobuf registries when the application supplies neither, so most consumers
// wire this module with no extra dependencies; supplying either (for example in
// tests) overrides the corresponding default.
var Module = fx.Options(fx.Provide(
	func(p ExtractorParams) *Extractor {
		if p.Files == nil {
			p.Files = protoregistry.GlobalFiles
		}

		if p.Types == nil {
			p.Types = protoregistry.GlobalTypes
		}

		return NewExtractor(p.Files, p.Types)
	},
))

// ExtractorParams collects the dependencies used to build the [*Extractor].
// Both are optional: when absent, Module falls back to the global protobuf
// registries.
type ExtractorParams struct {
	fx.In

	Files Files `optional:"true"`
	Types Types `optional:"true"`
}
