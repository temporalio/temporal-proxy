package protoutil

import (
	"fmt"

	"go.uber.org/fx"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Module provides an [*Extractor] and a [*Translator], and warms the
// translator's plan cache for every service in TranslatorServices at startup.
// Files and Types default to the global protobuf registries when the
// application supplies neither. TranslatorServices is optional: when empty, no
// plans are pre-populated and the translator builds them lazily on first use.
var Module = fx.Options(
	fx.Provide(
		func(p ExtractorParams) *Translator {
			return NewTranslator(filesOrGlobal(p.Files))
		},
		func(p ExtractorParams) *Extractor {
			return NewExtractor(filesOrGlobal(p.Files), typesOrGlobal(p.Types))
		},
	),
	fx.Invoke(func(t *Translator, p ExtractorParams) error {
		for _, svc := range p.TranslatorServices {
			if err := t.WarmService(svc); err != nil {
				return fmt.Errorf("failed to set up translations for %s: %w", svc, err)
			}
		}

		return nil
	}),
)

// ExtractorParams collects the dependencies used to build the [*Extractor] and
// the [Translator]. Files and Types are optional: when absent, Module falls back
// to the global protobuf registries. TranslatorServices is optional and lists
// the gRPC services whose request and response types are pre-warmed at startup.
type ExtractorParams struct {
	fx.In

	Files              Files                   `optional:"true"`
	Types              Types                   `optional:"true"`
	TranslatorServices []protoreflect.FullName `optional:"true"`
}

// filesOrGlobal returns files, or the global descriptor registry when files is
// nil.
func filesOrGlobal(files Files) Files {
	if files == nil {
		return protoregistry.GlobalFiles
	}

	return files
}

// typesOrGlobal returns types, or the global type registry when types is nil.
func typesOrGlobal(types Types) Types {
	if types == nil {
		return protoregistry.GlobalTypes
	}

	return types
}
