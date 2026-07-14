// Package protoutil provides helpers for working with Temporal's protobuf
// request types at the gRPC boundary, without the caller needing to know the
// concrete message type for a given method.
//
// [Extractor] reads the namespace from an encoded request by resolving its
// message type from a descriptor registry and a type registry. [Module] wires
// an Extractor into an fx application, defaulting those registries to
// [google.golang.org/protobuf/reflect/protoregistry.GlobalFiles] and
// [google.golang.org/protobuf/reflect/protoregistry.GlobalTypes] when they are
// not supplied.
package protoutil
