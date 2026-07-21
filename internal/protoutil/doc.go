// Package protoutil provides helpers for working with Temporal's protobuf
// request types at the gRPC boundary, without the caller needing to know the
// concrete message type for a given method.
//
// [Extractor] reads the namespace from an encoded request by resolving its
// message type from a descriptor registry and a type registry.
//
// [Translator] rewrites namespace names inside decoded messages, driven by a
// direction function, using a per-message-type plan cache that WarmService
// pre-populates. The package is free of any gRPC dependency; callers apply a
// Translator on a connection themselves.
//
// [Module] wires both an Extractor and a Translator into an fx application,
// defaulting the descriptor and type registries to
// [google.golang.org/protobuf/reflect/protoregistry.GlobalFiles] and
// [google.golang.org/protobuf/reflect/protoregistry.GlobalTypes] when they are
// not supplied, and warming the translator for the services the application
// lists in ExtractorParams.TranslatorServices.
package protoutil
