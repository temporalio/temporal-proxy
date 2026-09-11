package translation

import "sync"

// Default returns the registry of translations the proxy ships with, built once
// and shared. It fails only if a compiled-in mapping is malformed, which is a
// build-time mistake rather than anything configuration can cause; callers still
// propagate it so the process refuses to start rather than silently forwarding
// a method it was meant to translate.
func Default() (*Registry, error) {
	return defaultRegistry()
}

// defaultRegistry builds the shipped registry on first use. Every translation
// the proxy performs is listed here, and nowhere else.
var defaultRegistry = sync.OnceValues(func() (*Registry, error) {
	return NewRegistry(
		listNamespaces(),
	)
})
